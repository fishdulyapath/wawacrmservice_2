const express  = require('express')
const router   = express.Router({ mergeParams: true }) // รับ :id (activity_id) จาก parent
const multer   = require('multer')
const sharp    = require('sharp')
const path     = require('path')
const fs       = require('fs')
const { crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { notifyMany } = require('../services/notifyService')

router.use(authMiddleware)

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

async function touchActivity(activityId) {
  await crmDB.query(`UPDATE crm_activities SET updated_at = NOW() WHERE id = $1`, [activityId])
}

async function saveCommentFiles(activityId, commentId, files, userId) {
  if (!files?.length) return []

  const dir = path.join(UPLOAD_ROOT, 'comments', safeSegment(activityId), String(commentId))
  fs.mkdirSync(dir, { recursive: true })

  const saved = []

  for (const file of files) {
    const isImage  = IMAGE_MIME.includes(file.mimetype)
    const ext      = path.extname(file.originalname).toLowerCase() || (isImage ? '.jpg' : '')
    const baseName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`

    try {
      if (isImage) {
        const outName  = `${baseName}.webp`
        const outPath  = path.join(dir, outName)
        const info = await sharp(file.buffer)
          .rotate()
          .resize(1920, 1920, { fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 80 })
          .toFile(outPath)

        const thumbName = `thumb_${baseName}.webp`
        const thumbPath = path.join(dir, thumbName)
        await sharp(file.buffer)
          .rotate()
          .resize(400, 400, { fit: 'cover' })
          .webp({ quality: 70 })
          .toFile(thumbPath)

        const relOut   = `comments/${safeSegment(activityId)}/${commentId}/${outName}`
        const relThumb = `comments/${safeSegment(activityId)}/${commentId}/${thumbName}`

        const row = await crmDB.query(
          `INSERT INTO crm_activity_comment_attachments
             (comment_id, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,'image/webp',$5,$6,$7) RETURNING *`,
          [commentId, userId, outName, file.originalname, info.size, relOut, relThumb]
        )
        saved.push(row.rows[0])
      } else {
        const fileName = `${baseName}${ext}`
        const filePath = path.join(dir, fileName)
        fs.writeFileSync(filePath, file.buffer)

        const relPath = `comments/${safeSegment(activityId)}/${commentId}/${fileName}`
        const row = await crmDB.query(
          `INSERT INTO crm_activity_comment_attachments
             (comment_id, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,NULL) RETURNING *`,
          [commentId, userId, fileName, file.originalname, file.mimetype, file.size, relPath]
        )
        saved.push(row.rows[0])
      }
    } catch (err) {
      console.error('[Comment Upload Error]', file.originalname, err.message)
    }
  }

  return saved
}

// ── GET /api/activities/:id/comments ────────────────────────────
router.get('/', async (req, res) => {
  try {
    const result = await crmDB.query(`
      SELECT c.*, u.name AS user_name, u.code AS user_code,
        COALESCE(
          json_agg(ca ORDER BY ca.id) FILTER (WHERE ca.id IS NOT NULL),
          '[]'::json
        ) AS attachments
      FROM crm_activity_comments c
      JOIN crm_users u ON u.id = c.user_id
      LEFT JOIN crm_activity_comment_attachments ca ON ca.comment_id = c.id
      WHERE c.activity_id = $1
      GROUP BY c.id, u.name, u.code
      ORDER BY c.created_at ASC
    `, [req.params.id])
    res.json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── POST /api/activities/:id/comments ───────────────────────────
router.post('/', upload.array('files', MAX_FILES), async (req, res) => {
  const commentText = String(req.body.comment_text || '').trim()
  if (!commentText) {
    return res.status(400).json({ error: 'กรุณาระบุข้อความความคิดเห็น' })
  }

  const activityId = req.params.id

  try {
    // ตรวจว่า activity มีอยู่ + สิทธิ์
    const act = await crmDB.query('SELECT id, created_by FROM crm_activities WHERE id=$1 AND status != $2', [activityId, 'deleted'])
    if (!act.rows.length) return res.status(404).json({ error: 'ไม่พบกิจกรรม' })

    if (req.user.role === 'sales_rep') {
      const ownerCheck = await crmDB.query(
        'SELECT 1 FROM crm_activity_owners WHERE activity_id=$1 AND user_id=$2 AND removed_at IS NULL',
        [activityId, req.user.id]
      )
      const isCreator = act.rows[0].created_by === req.user.id
      if (!ownerCheck.rows.length && !isCreator) {
        return res.status(403).json({ error: 'ไม่มีสิทธิ์แสดงความคิดเห็น' })
      }
    }

    const result = await crmDB.query(
      `INSERT INTO crm_activity_comments (activity_id, user_id, comment_text)
       VALUES ($1,$2,$3) RETURNING *`,
      [activityId, req.user.id, commentText]
    )

    const comment = result.rows[0]
    comment.attachments = await saveCommentFiles(activityId, comment.id, req.files || [], req.user.id)
    comment.user_name = req.user.name || req.user.code || null
    comment.user_code = req.user.code || null
    await touchActivity(activityId)

    // แจ้ง owners (ที่ไม่ใช่คนสร้าง comment) และ followers ที่ไม่ใช่ owner
    try {
      const ownersRes = await crmDB.query(
        `SELECT user_id FROM crm_activity_owners
         WHERE activity_id=$1 AND removed_at IS NULL AND user_id != $2`,
        [activityId, req.user.id]
      )
      const followersRes = await crmDB.query(
        `SELECT f.user_id FROM crm_activity_follows f
         WHERE f.activity_id=$1
           AND f.user_id != $2
           AND NOT EXISTS (
             SELECT 1 FROM crm_activity_owners ao
             WHERE ao.activity_id=$1 AND ao.user_id=f.user_id AND ao.removed_at IS NULL
           )`,
        [activityId, req.user.id]
      )
      const notifyIds = [
        ...ownersRes.rows.map(r => r.user_id),
        ...followersRes.rows.map(r => r.user_id),
      ]
      if (notifyIds.length) {
        await notifyMany(notifyIds, {
          notiType: 'activity_update',
          title: `${req.user.name || req.user.code} แสดงความคิดเห็นในกิจกรรม`,
          message: commentText.length > 80 ? commentText.slice(0, 80) + '...' : commentText,
          refType: 'activity',
          refId: parseInt(activityId),
        })
      }
    } catch (notifyErr) {
      console.error('[Comments] notify error:', notifyErr.message)
    }

    res.status(201).json(comment)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── DELETE /api/activities/:id/comments/:commentId ───────────────
router.delete('/:commentId', async (req, res) => {
  try {
    const result = await crmDB.query(
      'SELECT * FROM crm_activity_comments WHERE id=$1 AND activity_id=$2',
      [req.params.commentId, req.params.id]
    )
    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบความคิดเห็น' })

    const comment = result.rows[0]
    if (comment.user_id !== req.user.id && !['admin', 'manager'].includes(req.user.role)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบความคิดเห็นนี้' })
    }

    // ลบไฟล์บน disk
    const attachments = await crmDB.query(
      'SELECT * FROM crm_activity_comment_attachments WHERE comment_id=$1',
      [req.params.commentId]
    )
    attachments.rows.forEach(att => {
      ;[att.file_path, att.thumb_path].filter(Boolean).forEach(p => {
        try { fs.unlinkSync(path.join(UPLOAD_ROOT, p)) } catch {}
      })
    })

    await crmDB.query('DELETE FROM crm_activity_comments WHERE id=$1', [req.params.commentId])
    await touchActivity(req.params.id)
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
