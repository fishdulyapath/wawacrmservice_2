const express = require('express')
const router  = express.Router()
const multer  = require('multer')
const sharp   = require('sharp')
const path    = require('path')
const fs      = require('fs')
const { crmDB }          = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { logAudit }       = require('../middleware/audit')
const { notify, notifyMany } = require('../services/notifyService')

router.use(authMiddleware)

// ── Permission helpers ───────────────────────────────────────
const isSA   = u => u.code?.toUpperCase() === 'SUPERADMIN'
const canPin = u => isSA(u) || ['admin', 'manager'].includes(u.role)
const canMod = (obj, u) => isSA(u) || obj.created_by === u.id

// ── File upload helpers ──────────────────────────────────────
const UPLOAD_ROOT = path.join(__dirname, '../uploads')
const MAX_SIZE    = 20 * 1024 * 1024
const MAX_FILES   = 10
const IMAGE_MIME  = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic', 'image/heif']

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_SIZE, files: MAX_FILES },
  fileFilter: (req, file, cb) => cb(null, true)
})

function safeSegment(value) {
  return String(value || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80) || 'unknown'
}

async function saveWbFiles(type, parentId, files, userId) {
  // type = 'threads' | 'comments'
  if (!files?.length) return []
  const dir = path.join(UPLOAD_ROOT, 'webboard', type, String(parentId))
  fs.mkdirSync(dir, { recursive: true })
  const table = type === 'threads' ? 'crm_webboard_thread_attachments' : 'crm_webboard_comment_attachments'
  const fkCol = type === 'threads' ? 'thread_id' : 'comment_id'
  const saved = []
  for (const file of files) {
    const isImage  = IMAGE_MIME.includes(file.mimetype)
    const ext      = path.extname(file.originalname).toLowerCase() || (isImage ? '.jpg' : '')
    const baseName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    try {
      if (isImage) {
        const outName = `${baseName}.webp`
        const info = await sharp(file.buffer).rotate()
          .resize(1920, 1920, { fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 80 }).toFile(path.join(dir, outName))
        await sharp(file.buffer).rotate()
          .resize(400, 400, { fit: 'cover' })
          .webp({ quality: 70 }).toFile(path.join(dir, `thumb_${baseName}.webp`))
        const relOut   = `webboard/${type}/${parentId}/${outName}`
        const relThumb = `webboard/${type}/${parentId}/thumb_${baseName}.webp`
        const row = await crmDB.query(
          `INSERT INTO ${table} (${fkCol},user_id,filename,original_name,mime_type,file_size,file_path,thumb_path)
           VALUES ($1,$2,$3,$4,'image/webp',$5,$6,$7) RETURNING *`,
          [parentId, userId, outName, file.originalname, info.size, relOut, relThumb]
        )
        saved.push(row.rows[0])
      } else {
        const fileName = `${baseName}${ext}`
        fs.writeFileSync(path.join(dir, fileName), file.buffer)
        const relPath = `webboard/${type}/${parentId}/${fileName}`
        const row = await crmDB.query(
          `INSERT INTO ${table} (${fkCol},user_id,filename,original_name,mime_type,file_size,file_path,thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,NULL) RETURNING *`,
          [parentId, userId, fileName, file.originalname, file.mimetype, file.size, relPath]
        )
        saved.push(row.rows[0])
      }
    } catch (err) {
      console.error('[WB Upload]', file.originalname, err.message)
    }
  }
  return saved
}

function deleteFiles(rows) {
  rows.forEach(att => {
    ;[att.file_path, att.thumb_path].filter(Boolean).forEach(p => {
      try { fs.unlinkSync(path.join(UPLOAD_ROOT, p)) } catch {}
    })
  })
}

// ─────────────────────────────────────────────────────────────
// GET /api/webboard/categories
// ─────────────────────────────────────────────────────────────
router.get('/categories', async (req, res) => {
  try {
    const all = req.query.all === 'true'
    const where = all ? '' : 'WHERE c.is_active = TRUE'
    const r = await crmDB.query(
      `SELECT c.*, u.name AS created_by_name
       FROM crm_webboard_categories c
       LEFT JOIN crm_users u ON u.id = c.created_by
       ${where} ORDER BY c.sort_order ASC`
    )
    res.json(r.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── POST /api/webboard/categories ─────────────────────────────
router.post('/categories', async (req, res) => {
  const { name, icon = '📁', color = '#6366f1' } = req.body
  if (!name?.trim()) return res.status(400).json({ error: 'กรุณาระบุชื่อหมวดหมู่' })
  try {
    const r = await crmDB.query(
      `INSERT INTO crm_webboard_categories (name, icon, color, sort_order, is_active, created_by)
       VALUES ($1,$2,$3,(SELECT COALESCE(MAX(sort_order),0)+1 FROM crm_webboard_categories),TRUE,$4)
       RETURNING *`,
      [name.trim(), icon, color, req.user.id]
    )
    res.status(201).json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── PATCH /api/webboard/categories/:id ────────────────────────
router.patch('/categories/:id', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_webboard_categories WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบหมวดหมู่' })
    const cat = existing.rows[0]
    if (!canMod(cat, req.user)) return res.status(403).json({ error: 'ไม่มีสิทธิ์แก้ไข' })
    const { name, icon, color, is_active } = req.body
    const r = await crmDB.query(
      `UPDATE crm_webboard_categories SET
        name=$1, icon=$2, color=$3,
        is_active=COALESCE($4::boolean, is_active)
       WHERE id=$5 RETURNING *`,
      [name?.trim() || cat.name, icon ?? cat.icon, color ?? cat.color, is_active ?? null, req.params.id]
    )
    res.json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── DELETE /api/webboard/categories/:id ───────────────────────
router.delete('/categories/:id', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_webboard_categories WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบหมวดหมู่' })
    const cat = existing.rows[0]
    if (!canMod(cat, req.user)) return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบ' })
    const used = await crmDB.query('SELECT COUNT(*) FROM crm_webboard_threads WHERE category_id=$1', [req.params.id])
    if (parseInt(used.rows[0].count) > 0) {
      return res.status(400).json({ error: `ไม่สามารถลบได้ มีกระทู้ ${used.rows[0].count} รายการที่ใช้หมวดหมู่นี้` })
    }
    await crmDB.query('DELETE FROM crm_webboard_categories WHERE id=$1', [req.params.id])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/webboard/threads  ?category_id&page&limit&search
// ─────────────────────────────────────────────────────────────
router.get('/threads', async (req, res) => {
  try {
    const { category_id, page = 1, limit = 20, search } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)

    const conditions = []
    const params     = []

    if (category_id) {
      params.push(parseInt(category_id))
      conditions.push(`t.category_id = $${params.length}`)
    }
    if (search?.trim()) {
      params.push(`%${search.trim()}%`)
      conditions.push(`(t.title ILIKE $${params.length} OR t.content ILIKE $${params.length})`)
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : ''

    const countRes = await crmDB.query(
      `SELECT COUNT(*) FROM crm_webboard_threads t ${where}`, params
    )

    const dataParams = [...params, parseInt(limit), offset]
    const dataRes = await crmDB.query(`
      SELECT
        t.*,
        u.name    AS author_name,
        c.name    AS category_name,
        c.color   AS category_color,
        c.icon    AS category_icon,
        (SELECT COUNT(*) FROM crm_webboard_comments WHERE thread_id = t.id)::int AS comment_count
      FROM crm_webboard_threads t
      JOIN crm_users               u ON u.id = t.created_by
      JOIN crm_webboard_categories c ON c.id = t.category_id
      ${where}
      ORDER BY t.is_pinned DESC, t.is_announcement DESC, t.created_at DESC
      LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}
    `, dataParams)

    const total = parseInt(countRes.rows[0].count)
    res.json({
      data: dataRes.rows,
      pagination: {
        total,
        page:  parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit)) || 1
      }
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/webboard/threads/:id
// ─────────────────────────────────────────────────────────────
router.get('/threads/:id', async (req, res) => {
  try {
    await crmDB.query(
      `UPDATE crm_webboard_threads SET view_count = view_count + 1 WHERE id = $1`,
      [req.params.id]
    )

    const threadRes = await crmDB.query(`
      SELECT t.*, u.name AS author_name,
             c.name AS category_name, c.color AS category_color, c.icon AS category_icon
      FROM crm_webboard_threads t
      JOIN crm_users               u ON u.id = t.created_by
      JOIN crm_webboard_categories c ON c.id = t.category_id
      WHERE t.id = $1
    `, [req.params.id])

    if (!threadRes.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })

    const commentsRes = await crmDB.query(`
      SELECT cm.*, u.name AS author_name
      FROM crm_webboard_comments cm
      JOIN crm_users u ON u.id = cm.created_by
      WHERE cm.thread_id = $1
      ORDER BY cm.created_at ASC
    `, [req.params.id])

    const threadAttRes = await crmDB.query(
      `SELECT * FROM crm_webboard_thread_attachments WHERE thread_id=$1 ORDER BY created_at ASC`,
      [req.params.id]
    )

    const commentIds = commentsRes.rows.map(c => c.id)
    let commentAttMap = {}
    if (commentIds.length) {
      const caRes = await crmDB.query(
        `SELECT * FROM crm_webboard_comment_attachments WHERE comment_id = ANY($1) ORDER BY created_at ASC`,
        [commentIds]
      )
      caRes.rows.forEach(a => {
        if (!commentAttMap[a.comment_id]) commentAttMap[a.comment_id] = []
        commentAttMap[a.comment_id].push(a)
      })
    }

    const followRes = await crmDB.query(
      `SELECT 1 FROM crm_webboard_follows WHERE user_id=$1 AND thread_id=$2`,
      [req.user.id, req.params.id]
    )

    const thread = { ...threadRes.rows[0], attachments: threadAttRes.rows }
    const comments = commentsRes.rows.map(c => ({ ...c, attachments: commentAttMap[c.id] || [] }))

    res.json({ thread, comments, is_following: followRes.rows.length > 0 })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/webboard/threads
// ─────────────────────────────────────────────────────────────
router.post('/threads', upload.array('files', MAX_FILES), async (req, res) => {
  try {
    const { category_id, title, content, is_announcement = false } = req.body
    if (!category_id || !title?.trim() || !content?.trim()) {
      return res.status(400).json({ error: 'กรุณากรอกข้อมูลให้ครบ' })
    }

    const announcementFlag = canPin(req.user) && is_announcement ? true : false

    const r = await crmDB.query(`
      INSERT INTO crm_webboard_threads (category_id, title, content, created_by, is_announcement)
      VALUES ($1, $2, $3, $4, $5) RETURNING *
    `, [category_id, title.trim(), content.trim(), req.user.id, announcementFlag])

    const thread = r.rows[0]
    thread.attachments = await saveWbFiles('threads', thread.id, req.files || [], req.user.id)

    // Auto-follow own thread
    await crmDB.query(
      `INSERT INTO crm_webboard_follows (user_id, thread_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [req.user.id, thread.id]
    )

    await logAudit({
      tableName: 'crm_webboard_threads', recordId: thread.id,
      action: 'INSERT', newData: thread
    }, req)

    res.status(201).json(thread)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/webboard/threads/:id
// ─────────────────────────────────────────────────────────────
router.patch('/threads/:id', upload.array('files', MAX_FILES), async (req, res) => {
  try {
    const existing = await crmDB.query(
      'SELECT * FROM crm_webboard_threads WHERE id=$1', [req.params.id]
    )
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })
    const thread = existing.rows[0]

    if (!canMod(thread, req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์แก้ไขกระทู้นี้' })
    }

    const { title, content, category_id, is_announcement } = req.body
    const announcementVal = canPin(req.user) && is_announcement !== undefined
      ? (is_announcement === true || is_announcement === 'true')
      : thread.is_announcement
    const r = await crmDB.query(`
      UPDATE crm_webboard_threads
      SET title=$1, content=$2, category_id=$3, is_announcement=$5, updated_at=NOW()
      WHERE id=$4 RETURNING *
    `, [
      title?.trim()   || thread.title,
      content?.trim() || thread.content,
      category_id     || thread.category_id,
      req.params.id,
      announcementVal,
    ])

    // แนบไฟล์ใหม่ (ถ้ามี)
    const newAtts = await saveWbFiles('threads', thread.id, req.files || [], req.user.id)

    await logAudit({
      tableName: 'crm_webboard_threads', recordId: thread.id,
      action: 'UPDATE', oldData: thread, newData: r.rows[0]
    }, req)

    const allAtts = await crmDB.query(
      'SELECT * FROM crm_webboard_thread_attachments WHERE thread_id=$1 ORDER BY created_at ASC',
      [thread.id]
    )
    res.json({ ...r.rows[0], attachments: allAtts.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// DELETE /api/webboard/threads/:id
// ─────────────────────────────────────────────────────────────
router.delete('/threads/:id', async (req, res) => {
  try {
    const existing = await crmDB.query(
      'SELECT * FROM crm_webboard_threads WHERE id=$1', [req.params.id]
    )
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })
    const thread = existing.rows[0]

    if (!canMod(thread, req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบกระทู้นี้' })
    }

    // ลบ files บน disk (thread + comment attachments — CASCADE ลบ records)
    const tAtts = await crmDB.query('SELECT * FROM crm_webboard_thread_attachments WHERE thread_id=$1', [req.params.id])
    const cAtts = await crmDB.query(
      `SELECT ca.* FROM crm_webboard_comment_attachments ca
       JOIN crm_webboard_comments cm ON cm.id=ca.comment_id WHERE cm.thread_id=$1`, [req.params.id]
    )
    deleteFiles([...tAtts.rows, ...cAtts.rows])

    await crmDB.query('DELETE FROM crm_webboard_threads WHERE id=$1', [req.params.id])

    await logAudit({
      tableName: 'crm_webboard_threads', recordId: req.params.id,
      action: 'DELETE', oldData: thread
    }, req)

    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/webboard/threads/:id/pin  (manager+ only)
// ─────────────────────────────────────────────────────────────
router.patch('/threads/:id/pin', async (req, res) => {
  try {
    if (!canPin(req.user)) {
      return res.status(403).json({ error: 'เฉพาะ Manager ขึ้นไปเท่านั้น' })
    }

    const r = await crmDB.query(
      `UPDATE crm_webboard_threads SET is_pinned = NOT is_pinned WHERE id=$1 RETURNING *`,
      [req.params.id]
    )
    if (!r.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })
    res.json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/webboard/threads/:id/comments
// ─────────────────────────────────────────────────────────────
// ── DELETE /api/webboard/threads/:id/attachments/:attId ─────────
router.delete('/threads/:id/attachments/:attId', async (req, res) => {
  try {
    const att = await crmDB.query(
      'SELECT * FROM crm_webboard_thread_attachments WHERE id=$1 AND thread_id=$2',
      [req.params.attId, req.params.id]
    )
    if (!att.rows.length) return res.status(404).json({ error: 'ไม่พบไฟล์' })
    if (att.rows[0].user_id !== req.user.id && !canPin(req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบไฟล์นี้' })
    }
    deleteFiles([att.rows[0]])
    await crmDB.query('DELETE FROM crm_webboard_thread_attachments WHERE id=$1', [req.params.attId])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── POST /api/webboard/threads/:id/comments ──────────────────────
router.post('/threads/:id/comments', upload.array('files', MAX_FILES), async (req, res) => {
  try {
    const { content } = req.body
    if (!content?.trim()) return res.status(400).json({ error: 'กรุณากรอกข้อความ' })

    const threadRes = await crmDB.query(
      'SELECT * FROM crm_webboard_threads WHERE id=$1', [req.params.id]
    )
    if (!threadRes.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })
    const thread = threadRes.rows[0]

    const r = await crmDB.query(`
      INSERT INTO crm_webboard_comments (thread_id, content, created_by)
      VALUES ($1, $2, $3) RETURNING *
    `, [req.params.id, content.trim(), req.user.id])

    // Auto-follow thread when commenting
    await crmDB.query(
      `INSERT INTO crm_webboard_follows (user_id, thread_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [req.user.id, req.params.id]
    )

    // Notify followers (except the commenter)
    const followersRes = await crmDB.query(
      `SELECT user_id FROM crm_webboard_follows WHERE thread_id=$1 AND user_id != $2`,
      [req.params.id, req.user.id]
    )
    const followerIds = followersRes.rows.map(f => f.user_id)

    if (followerIds.length) {
      await notifyMany(followerIds, {
        notiType: 'webboard_comment',
        title:    `${req.user.name} แสดงความคิดเห็นในกระทู้ที่คุณติดตาม`,
        message:  thread.title,
        refType:  'webboard',
        refId:    parseInt(req.params.id)
      })
    }

    const commentId = r.rows[0].id
    const attachments = await saveWbFiles('comments', commentId, req.files || [], req.user.id)

    // Return comment with author_name + attachments
    const full = await crmDB.query(`
      SELECT cm.*, u.name AS author_name
      FROM crm_webboard_comments cm
      JOIN crm_users u ON u.id = cm.created_by
      WHERE cm.id = $1
    `, [commentId])

    res.status(201).json({ ...full.rows[0], attachments })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/webboard/comments/:id
// ─────────────────────────────────────────────────────────────
router.patch('/comments/:id', async (req, res) => {
  try {
    const existing = await crmDB.query(
      'SELECT * FROM crm_webboard_comments WHERE id=$1', [req.params.id]
    )
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบความคิดเห็น' })
    const comment = existing.rows[0]

    if (comment.created_by !== req.user.id && !canPin(req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์แก้ไข' })
    }

    const { content } = req.body
    if (!content?.trim()) return res.status(400).json({ error: 'กรุณากรอกข้อความ' })

    const r = await crmDB.query(
      `UPDATE crm_webboard_comments SET content=$1, updated_at=NOW() WHERE id=$2 RETURNING *`,
      [content.trim(), req.params.id]
    )
    res.json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// DELETE /api/webboard/comments/:id
// ─────────────────────────────────────────────────────────────
router.delete('/comments/:id', async (req, res) => {
  try {
    const existing = await crmDB.query(
      'SELECT * FROM crm_webboard_comments WHERE id=$1', [req.params.id]
    )
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบความคิดเห็น' })
    const comment = existing.rows[0]

    if (comment.created_by !== req.user.id && !canPin(req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบ' })
    }

    const cAtts = await crmDB.query('SELECT * FROM crm_webboard_comment_attachments WHERE comment_id=$1', [req.params.id])
    deleteFiles(cAtts.rows)
    await crmDB.query('DELETE FROM crm_webboard_comments WHERE id=$1', [req.params.id])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── DELETE /api/webboard/comments/:id/attachments/:attId ────────
router.delete('/comments/:id/attachments/:attId', async (req, res) => {
  try {
    const att = await crmDB.query(
      'SELECT * FROM crm_webboard_comment_attachments WHERE id=$1 AND comment_id=$2',
      [req.params.attId, req.params.id]
    )
    if (!att.rows.length) return res.status(404).json({ error: 'ไม่พบไฟล์' })
    if (att.rows[0].user_id !== req.user.id && !canPin(req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบไฟล์นี้' })
    }
    deleteFiles([att.rows[0]])
    await crmDB.query('DELETE FROM crm_webboard_comment_attachments WHERE id=$1', [req.params.attId])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/webboard/threads/:id/follow  (toggle)
// ─────────────────────────────────────────────────────────────
router.post('/threads/:id/follow', async (req, res) => {
  try {
    const existing = await crmDB.query(
      `SELECT 1 FROM crm_webboard_follows WHERE user_id=$1 AND thread_id=$2`,
      [req.user.id, req.params.id]
    )
    if (existing.rows.length) {
      await crmDB.query(
        `DELETE FROM crm_webboard_follows WHERE user_id=$1 AND thread_id=$2`,
        [req.user.id, req.params.id]
      )
      res.json({ following: false })
    } else {
      await crmDB.query(
        `INSERT INTO crm_webboard_follows (user_id, thread_id) VALUES ($1,$2)`,
        [req.user.id, req.params.id]
      )
      res.json({ following: true })
    }
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
