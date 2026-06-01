const express = require('express')
const router  = express.Router()
const { crmDB, posDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')

router.use(authMiddleware)

// GET /api/notifications — inbox ของ user
router.get('/', async (req, res) => {
  try {
    const { page = 1, limit = 30, unread_only } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)

    const conditions = ['user_id = $1']
    const params = [req.user.id]

    if (unread_only === 'true') {
      conditions.push('is_read = FALSE')
    }

    const where = 'WHERE ' + conditions.join(' AND ')

    const countResult = await crmDB.query(
      `SELECT COUNT(*) FROM crm_notifications ${where}`, params
    )
    const unreadResult = await crmDB.query(
      `SELECT COUNT(*) FROM crm_notifications WHERE user_id=$1 AND is_read=FALSE`, [req.user.id]
    )

    params.push(parseInt(limit), offset)
    const data = await crmDB.query(
      `SELECT * FROM crm_notifications ${where}
       ORDER BY created_at DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    )

    const total = parseInt(countResult.rows[0].count)

    // ดึงชื่อลูกค้าจาก POS สำหรับ notification ที่มี ar_code
    const arCodes = [...new Set(data.rows.map(r => r.ar_code).filter(Boolean))]
    let customerMap = {}
    if (arCodes.length > 0) {
      try {
        const placeholders = arCodes.map((_, i) => `$${i + 1}`).join(',')
        const posRes = await posDB.query(
          `SELECT code, name_1 FROM ar_customer WHERE code IN (${placeholders})`, arCodes
        )
        posRes.rows.forEach(r => { customerMap[r.code] = r.name_1 })
      } catch {}
    }
    const rows = data.rows.map(r => ({
      ...r,
      customer_name: customerMap[r.ar_code] || null,
    }))

    res.json({
      data: rows,
      unread_count: parseInt(unreadResult.rows[0].count),
      pagination: {
        total,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit))
      }
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/notifications/read-all — อ่านทั้งหมด (ต้องอยู่ก่อน /:id/read)
router.patch('/read-all', async (req, res) => {
  try {
    await crmDB.query(
      `UPDATE crm_notifications SET is_read=TRUE WHERE user_id=$1 AND is_read=FALSE`,
      [req.user.id]
    )
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/notifications/:id/read — อ่านแล้ว
router.patch('/:id/read', async (req, res) => {
  try {
    await crmDB.query(
      `UPDATE crm_notifications SET is_read=TRUE WHERE id=$1 AND user_id=$2`,
      [req.params.id, req.user.id]
    )
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
