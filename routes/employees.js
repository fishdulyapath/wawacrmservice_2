const express = require('express')
const router = express.Router()
const { posDB, crmDB } = require('../db')

// GET /api/employees  — ดึง erp_user จาก POS + crm_users
router.get('/', async (req, res) => {
  try {
    const { search = '' } = req.query
    const params = search ? [`%${search}%`] : []
    const where = search ? `WHERE code ILIKE $1 OR name_1 ILIKE $1` : ''

    const result = await posDB.query(
      `SELECT code, name_1 FROM erp_user ${where} ORDER BY code`,
      params
    )
    res.json(result.rows)
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/employees/crm-users — ดึง crm_users
router.get('/crm-users', async (req, res) => {
  try {
    const result = await crmDB.query(`
      SELECT id, code, name, email, phone, role, is_active
      FROM crm_users
      WHERE is_active = TRUE
      ORDER BY name
    `)
    res.json(result.rows)
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// POST /api/employees/sync — Sync erp_user → crm_users
// ไม่ต้อง JWT — ใช้ตอน first-time setup ก่อนมี user
// ตั้ง SYNC_SECRET ใน .env เพื่อป้องกัน (ถ้าไม่ตั้งจะเปิดกว้าง)
router.post('/sync', async (req, res) => {
  const secret = process.env.SYNC_SECRET
  if (secret) {
    const provided = req.headers['x-sync-secret'] || req.body?.sync_secret
    if (provided !== secret) {
      return res.status(401).json({ error: 'Unauthorized — ต้องใส่ x-sync-secret header' })
    }
  }
  try {
    const posResult = await posDB.query(`SELECT code, name_1 FROM erp_user`)
    let synced = 0
    for (const u of posResult.rows) {
      await crmDB.query(`
        INSERT INTO crm_users (code, name, role)
        VALUES ($1, $2, 'sales_rep')
        ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
      `, [u.code, u.name_1])
      synced++
    }
    res.json({ success: true, synced })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/employees/crm-users/:id/role — เปลี่ยน role ของ user
// เข้าถึงได้เฉพาะ admin / superadmin
const { authMiddleware } = require('../middleware/auth')
router.patch('/crm-users/:id/role', authMiddleware, async (req, res) => {
  const callerCode = req.user.code?.toUpperCase()
  const callerRole = req.user.role
  const isSuperAdmin = callerCode === 'SUPERADMIN'
  if (!isSuperAdmin && callerRole !== 'admin') {
    return res.status(403).json({ error: 'ไม่มีสิทธิ์เปลี่ยนสิทธิ์ผู้ใช้งาน' })
  }

  const { role } = req.body
  const allowed = ['sales_rep', 'supervisor', 'manager', 'admin']
  if (!allowed.includes(role)) {
    return res.status(400).json({ error: `role ไม่ถูกต้อง (${allowed.join(', ')})` })
  }

  // ป้องกันแก้ตัวเอง
  if (req.params.id == req.user.id) {
    return res.status(400).json({ error: 'ไม่สามารถเปลี่ยนสิทธิ์ตัวเองได้' })
  }

  try {
    const result = await crmDB.query(
      `UPDATE crm_users SET role = $1 WHERE id = $2 RETURNING id, code, name, role`,
      [role, req.params.id]
    )
    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบผู้ใช้งาน' })
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
