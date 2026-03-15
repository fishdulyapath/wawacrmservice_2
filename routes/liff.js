const express = require('express')
const router  = express.Router()
const { posDB, crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { logAudit } = require('../middleware/audit')

// ทุก LIFF endpoint ต้อง login
router.use(authMiddleware)

// ─────────────────────────────────────────────────────────────
// GET /api/liff/tasks
// งานของ user ที่ login (open + due soon)
// ─────────────────────────────────────────────────────────────
router.get('/tasks', async (req, res) => {
  try {
    const userId = req.user.id
    const result = await crmDB.query(`
      SELECT
        a.id, a.ar_code, a.activity_type, a.subject,
        a.description, a.priority,
        a.due_date, a.start_datetime, a.end_datetime, a.location,
        a.call_direction, a.call_result, a.call_phone, a.duration_sec,
        a.outcome, a.cdr_recording_url,
        ao.status AS my_status,
        (SELECT string_agg(u.name, ', ' ORDER BY u.name)
         FROM crm_activity_owners o2
         JOIN crm_users u ON u.id = o2.user_id
         WHERE o2.activity_id = a.id AND o2.removed_at IS NULL) AS owners_names
      FROM crm_activities a
      JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.user_id = $1 AND ao.removed_at IS NULL
      WHERE ao.status NOT IN ('done','cancelled')
      ORDER BY
        CASE WHEN a.due_date < CURRENT_DATE THEN 0 ELSE 1 END,
        CASE WHEN DATE(a.due_date) = CURRENT_DATE THEN 0 ELSE 1 END,
        CASE WHEN a.activity_type = 'meeting' AND DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok') = (CURRENT_DATE AT TIME ZONE 'Asia/Bangkok') THEN 0 ELSE 1 END,
        a.due_date ASC NULLS LAST,
        a.start_datetime ASC NULLS LAST,
        CASE a.priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END
      LIMIT 100
    `, [userId])

    const rows = result.rows

    // ดึง customer_name จาก POS
    const arCodes = [...new Set(rows.map(r => r.ar_code).filter(Boolean))]
    let nameMap = {}
    if (arCodes.length > 0) {
      try {
        const posResult = await posDB.query(
          `SELECT code, name_1 FROM ar_customer WHERE code = ANY($1)`, [arCodes]
        )
        for (const r of posResult.rows) nameMap[r.code] = r.name_1
      } catch {}
    }

    // ดึง contactors (ผู้ติดต่อ) จาก POS สำหรับ ar_code ของแต่ละงาน
    let contactorMap = {}
    if (arCodes.length > 0) {
      try {
        const cRes = await posDB.query(`
          SELECT ar_code, name, telephone, work_title
          FROM ar_contactor
          WHERE ar_code = ANY($1)
          ORDER BY ar_code, name
        `, [arCodes])
        for (const c of cRes.rows) {
          if (!contactorMap[c.ar_code]) contactorMap[c.ar_code] = []
          contactorMap[c.ar_code].push({
            name: c.name,
            work_title: c.work_title || null,
            phones: (c.telephone || '').split(',').map(p => p.trim()).filter(Boolean)
          })
        }
      } catch {}
    }

    res.json(rows.map(r => ({
      ...r,
      customer_name: nameMap[r.ar_code] || null,
      contactors: contactorMap[r.ar_code] || []
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/liff/tasks/:id/done
// ปิดงาน
// ─────────────────────────────────────────────────────────────
router.patch('/tasks/:id/done', async (req, res) => {
  try {
    const { id } = req.params
    const userId  = req.user.id
    const {
      outcome, call_phone, call_result, call_direction, duration_sec
    } = req.body

    // ตรวจว่าเป็น active owner ของงานนี้
    const check = await crmDB.query(
      `SELECT a.id, a.activity_type FROM crm_activities a
       WHERE a.id=$1 AND EXISTS (
         SELECT 1 FROM crm_activity_owners ao
         WHERE ao.activity_id = a.id AND ao.user_id = $2 AND ao.removed_at IS NULL
       )`, [id, userId]
    )
    if (check.rows.length === 0)
      return res.status(403).json({ error: 'ไม่พบงานนี้' })

    // อัปเดต owner status → done
    await crmDB.query(
      `UPDATE crm_activity_owners SET status='done' WHERE activity_id=$1 AND user_id=$2 AND removed_at IS NULL`,
      [id, userId]
    )

    // อัปเดต call/outcome fields บน activity row
    await crmDB.query(
      `UPDATE crm_activities
       SET outcome     = COALESCE($2, outcome),
           call_phone  = COALESCE($3, call_phone),
           call_result = COALESCE($4, call_result),
           call_direction = COALESCE($5, call_direction),
           duration_sec   = COALESCE($6, duration_sec),
           updated_at  = NOW()
       WHERE id = $1`,
      [
        id,
        outcome     || null,
        call_phone  || null,
        call_result || null,
        call_direction || null,
        duration_sec != null ? parseInt(duration_sec) : null
      ]
    )
    await logAudit({ tableName: 'crm_activities', recordId: id, action: 'UPDATE',
      newData: { status: 'done', outcome, call_phone, call_result, call_direction, duration_sec } }, req)

    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/liff/customers
// ลูกค้าที่ user ดูแล (is_primary)
// ─────────────────────────────────────────────────────────────
router.get('/customers', async (req, res) => {
  try {
    const userId = req.user.id
    const { search = '', limit = 50, page = 1 } = req.query
    const offset = (Number(page) - 1) * Number(limit)

    // ดึง ar_code ที่ user ดูแล
    const ownerResult = await crmDB.query(
      `SELECT ar_code FROM crm_customer_owner WHERE user_id=$1 AND is_primary=TRUE`,
      [userId]
    )
    const arCodes = ownerResult.rows.map(r => r.ar_code)
    if (arCodes.length === 0) return res.json([])

    // Query จาก POS DB
    const searchClause = search
      ? `AND (ac.name_1 ILIKE $2 OR ac.code ILIKE $2)`
      : ''
    const params = search
      ? [arCodes, `%${search}%`]
      : [arCodes]

    const cusResult = await posDB.query(`
      SELECT ac.code, ac.name_1, ac.province, ac.address, ac.zip_code, ac.remark
      FROM ar_customer ac
      WHERE ac.code = ANY($1)
      ${searchClause}
      ORDER BY ac.name_1
      LIMIT ${Number(limit)} OFFSET ${offset}
    `, params)

    // Merge CRM status
    const codes = cusResult.rows.map(r => r.code)
    if (codes.length === 0) return res.json([])

    const crmResult = await crmDB.query(`
      SELECT p.ar_code, p.status AS crm_status, p.priority, p.customer_type,
             p.last_contacted, p.crm_remark,
             u.name AS owner_name
      FROM crm_customer_profile p
      LEFT JOIN crm_customer_owner o ON o.ar_code = p.ar_code AND o.is_primary = TRUE
      LEFT JOIN crm_users u ON u.id = o.user_id
      WHERE p.ar_code = ANY($1)
    `, [codes])

    const crmMap = {}
    for (const r of crmResult.rows) crmMap[r.ar_code] = r

    const rows = cusResult.rows.map(c => ({ ...c, crm: crmMap[c.code] || null }))
    res.json(rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/liff/customers/:code
// ดูรายละเอียดลูกค้า (contactors, crm)
// ─────────────────────────────────────────────────────────────
router.get('/customers/:code', async (req, res) => {
  const { code } = req.params
  try {
    const [cusResult, contactResult] = await Promise.all([
      posDB.query(`
        SELECT c.code, c.name_1, c.country, c.address, c.province,
               c.amper, c.tambon, c.zip_code, c.website, c.remark
        FROM ar_customer c WHERE c.code = $1
      `, [code]),
      posDB.query(`
        SELECT name, email, telephone, work_title FROM ar_contactor WHERE ar_code = $1 ORDER BY name
      `, [code])
    ])

    if (cusResult.rows.length === 0)
      return res.status(404).json({ error: 'ไม่พบลูกค้า' })

    const crmResult = await crmDB.query(`
      SELECT p.status AS crm_status, p.priority, p.customer_type, p.last_contacted, p.crm_remark,
             u.name AS owner_name
      FROM crm_customer_profile p
      LEFT JOIN crm_customer_owner o ON o.ar_code = p.ar_code AND o.is_primary = TRUE
      LEFT JOIN crm_users u ON u.id = o.user_id
      WHERE p.ar_code = $1
    `, [code])

    res.json({
      customer: cusResult.rows[0],
      contactors: contactResult.rows,
      crm: crmResult.rows[0] || null
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/liff/quick-activity
// สร้าง Activity เร็วๆ จาก LINE (call log, follow-up task)
// ─────────────────────────────────────────────────────────────
router.post('/quick-activity', async (req, res) => {
  try {
    const userId = req.user.id
    const {
      ar_code, activity_type = 'call', subject,
      description, status = 'open', priority = 'normal',
      due_date, start_datetime, call_direction,
      call_result, call_phone, duration_sec
    } = req.body

    if (!ar_code || !subject)
      return res.status(400).json({ error: 'ar_code และ subject จำเป็น' })

    const result = await crmDB.query(`
      INSERT INTO crm_activities
        (ar_code, owner_id, activity_type, subject, description,
         status, priority, due_date, start_datetime,
         call_direction, call_result, call_phone, duration_sec)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
      RETURNING id
    `, [
      ar_code, userId, activity_type, subject, description || null,
      status, priority,
      due_date || null,
      start_datetime || (activity_type === 'call' ? new Date() : null),
      call_direction || null, call_result || null,
      call_phone || null, duration_sec || null
    ])

    const activityId = result.rows[0].id

    // insert owner row — ถ้าไม่มีจะทำให้ tasks query ไม่เจองานนี้
    await crmDB.query(`
      INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
      VALUES ($1, $2, TRUE, $3, $2)
      ON CONFLICT (activity_id, user_id) DO NOTHING
    `, [activityId, userId, status])

    await logAudit({
      tableName: 'crm_activities', recordId: activityId,
      arCode: ar_code, action: 'INSERT', newData: req.body
    }, req)

    res.status(201).json({ success: true, id: activityId })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/liff/summary
// Dashboard สรุปสำหรับ LINE home
// ─────────────────────────────────────────────────────────────
router.get('/summary', async (req, res) => {
  try {
    const userId = req.user.id
    const result = await crmDB.query(`
      SELECT * FROM v_daily_summary_per_user WHERE user_id=$1
    `, [userId])
    res.json(result.rows[0] || {})
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
