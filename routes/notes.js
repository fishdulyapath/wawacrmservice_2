const express = require('express')
const router  = express.Router()
const { crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')

router.use(authMiddleware)

// GET /api/notes?ar_code=xxx
router.get('/', async (req, res) => {
  const { ar_code } = req.query
  if (!ar_code) return res.status(400).json({ error: 'ต้องระบุ ar_code' })
  try {
    const result = await crmDB.query(`
      SELECT n.*, u.name AS created_by_name
      FROM crm_notes n
      LEFT JOIN crm_users u ON u.id = n.created_by
      WHERE n.ar_code = $1
      ORDER BY n.is_pinned DESC, n.created_at DESC
    `, [ar_code])
    res.json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /api/notes
router.post('/', async (req, res) => {
  const { ar_code, note_text, is_pinned = false } = req.body
  if (!ar_code || !note_text?.trim()) {
    return res.status(400).json({ error: 'ต้องระบุ ar_code และ note_text' })
  }
  try {
    const result = await crmDB.query(`
      INSERT INTO crm_notes (ar_code, note_text, is_pinned, created_by)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `, [ar_code, note_text.trim(), is_pinned, req.user.id])
    res.status(201).json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/notes/:id/pin — toggle pin
router.patch('/:id/pin', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_notes WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบบันทึก' })
    const result = await crmDB.query(
      `UPDATE crm_notes SET is_pinned = NOT is_pinned WHERE id=$1 RETURNING *`,
      [req.params.id]
    )
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// DELETE /api/notes/:id
router.delete('/:id', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_notes WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบบันทึก' })

    const note = existing.rows[0]
    // ลบได้เฉพาะเจ้าของหรือ admin/manager
    if (note.created_by !== req.user.id && !['admin','manager'].includes(req.user.role)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบบันทึกนี้' })
    }

    await crmDB.query('DELETE FROM crm_notes WHERE id=$1', [req.params.id])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
