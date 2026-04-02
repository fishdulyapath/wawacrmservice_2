const express = require('express')
const router  = express.Router()
const { crmDB }          = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { logAudit }       = require('../middleware/audit')
const { notify, notifyMany } = require('../services/notifyService')

router.use(authMiddleware)

// ── Permission helpers ───────────────────────────────────────
const isSA   = u => u.code?.toUpperCase() === 'SUPERADMIN'
const canPin = u => isSA(u) || ['admin', 'manager'].includes(u.role)
const canMod = (obj, u) => isSA(u) || obj.created_by === u.id

// ─────────────────────────────────────────────────────────────
// GET /api/webboard/categories
// ─────────────────────────────────────────────────────────────
router.get('/categories', async (req, res) => {
  try {
    const r = await crmDB.query(
      `SELECT * FROM crm_webboard_categories WHERE is_active = TRUE ORDER BY sort_order ASC`
    )
    res.json(r.rows)
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

    const followRes = await crmDB.query(
      `SELECT 1 FROM crm_webboard_follows WHERE user_id=$1 AND thread_id=$2`,
      [req.user.id, req.params.id]
    )

    res.json({
      thread:       threadRes.rows[0],
      comments:     commentsRes.rows,
      is_following: followRes.rows.length > 0
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/webboard/threads
// ─────────────────────────────────────────────────────────────
router.post('/threads', async (req, res) => {
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

    // Auto-follow own thread
    await crmDB.query(
      `INSERT INTO crm_webboard_follows (user_id, thread_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [req.user.id, r.rows[0].id]
    )

    await logAudit({
      tableName: 'crm_webboard_threads', recordId: r.rows[0].id,
      action: 'INSERT', newData: r.rows[0]
    }, req)

    res.status(201).json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/webboard/threads/:id
// ─────────────────────────────────────────────────────────────
router.patch('/threads/:id', async (req, res) => {
  try {
    const existing = await crmDB.query(
      'SELECT * FROM crm_webboard_threads WHERE id=$1', [req.params.id]
    )
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบกระทู้' })
    const thread = existing.rows[0]

    if (!canMod(thread, req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์แก้ไขกระทู้นี้' })
    }

    const { title, content, category_id } = req.body
    const r = await crmDB.query(`
      UPDATE crm_webboard_threads
      SET title=$1, content=$2, category_id=$3, updated_at=NOW()
      WHERE id=$4 RETURNING *
    `, [
      title?.trim()   || thread.title,
      content?.trim() || thread.content,
      category_id     || thread.category_id,
      req.params.id
    ])

    await logAudit({
      tableName: 'crm_webboard_threads', recordId: thread.id,
      action: 'UPDATE', oldData: thread, newData: r.rows[0]
    }, req)

    res.json(r.rows[0])
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
router.post('/threads/:id/comments', async (req, res) => {
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

    // Return comment with author_name
    const full = await crmDB.query(`
      SELECT cm.*, u.name AS author_name
      FROM crm_webboard_comments cm
      JOIN crm_users u ON u.id = cm.created_by
      WHERE cm.id = $1
    `, [r.rows[0].id])

    res.status(201).json(full.rows[0])
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

    await crmDB.query('DELETE FROM crm_webboard_comments WHERE id=$1', [req.params.id])
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
