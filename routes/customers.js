const express = require('express')
const router = express.Router()
const { posDB, crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { logAudit } = require('../middleware/audit')

// ทุก endpoint ต้อง Login ก่อน
router.use(authMiddleware)

// ─────────────────────────────────────────────
// GET /api/customers  — List ลูกค้าทั้งหมด
// ─────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const { search = '', page = 1, limit = 20, status, owner } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)

    // ── 1. สร้าง ar_code whitelist จาก filter ──────────────
    // filter by owner: รวม CRM owner_code + POS sale_code
    let ownerArCodes = null
    if (owner) {
      const [crmOwnerRes, salePosRes] = await Promise.all([
        crmDB.query(
          `SELECT o.ar_code FROM crm_customer_owner o
           JOIN crm_users u ON u.id = o.user_id
           WHERE u.code = $1 AND o.is_primary = TRUE`, [owner]
        ),
        posDB.query(
          `SELECT ar_code FROM ar_customer_detail WHERE sale_code = $1`, [owner]
        )
      ])
      const set = new Set([
        ...crmOwnerRes.rows.map(r => r.ar_code),
        ...salePosRes.rows.map(r => r.ar_code)
      ])
      ownerArCodes = [...set]
      if (ownerArCodes.length === 0) {
        return res.json({ data: [], total: 0, page: parseInt(page), limit: parseInt(limit) })
      }
    }

    // filter by crm_status: ดึง ar_code ที่มี status ตรง
    let statusArCodes = null
    if (status) {
      const statusRes = await crmDB.query(
        `SELECT ar_code FROM crm_customer_profile WHERE status = $1`, [status]
      )
      statusArCodes = statusRes.rows.map(r => r.ar_code)
      if (statusArCodes.length === 0) {
        return res.json({ data: [], total: 0, page: parseInt(page), limit: parseInt(limit) })
      }
    }

    // ── 2. Build WHERE สำหรับ POS query ───────────────────
    let where = ['1=1']
    let params = []

    if (search) {
      params.push(`%${search}%`)
      where.push(`(c.code ILIKE $${params.length} OR c.name_1 ILIKE $${params.length})`)
    }
    if (ownerArCodes) {
      params.push(ownerArCodes)
      where.push(`c.code = ANY($${params.length})`)
    }
    if (statusArCodes) {
      params.push(statusArCodes)
      where.push(`c.code = ANY($${params.length})`)
    }

    // ── 3. COUNT ──────────────────────────────────────────
    const countResult = await posDB.query(
      `SELECT COUNT(*) FROM ar_customer c WHERE ${where.join(' AND ')}`,
      params
    )
    const total = parseInt(countResult.rows[0].count)

    // ── 4. PAGE query ─────────────────────────────────────
    params.push(parseInt(limit))
    params.push(offset)

    const posResult = await posDB.query(`
      SELECT
        c.code, c.name_1, c.country, c.address, c.province,
        c.amper, c.tambon, c.zip_code, c.website, c.remark,
        d.sale_code, u.name_1 AS sale_name
      FROM ar_customer c
      LEFT JOIN ar_customer_detail d ON d.ar_code = c.code
      LEFT JOIN erp_user u           ON u.code = d.sale_code
      WHERE ${where.join(' AND ')}
      ORDER BY c.code
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    // ── 5. ดึง CRM info เฉพาะ ar_code ที่ได้ ─────────────
    const codes = posResult.rows.map(r => r.code)
    const crmMap = {}
    if (codes.length > 0) {
      const crmResult = await crmDB.query(`
        SELECT
          p.ar_code, p.customer_type, p.status AS crm_status,
          p.priority, p.last_contacted, p.next_followup, p.crm_remark, p.tags,
          o.user_id AS owner_user_id, u.code AS owner_code, u.name AS owner_name
        FROM crm_customer_profile p
        LEFT JOIN crm_customer_owner o ON o.ar_code = p.ar_code AND o.is_primary = TRUE
        LEFT JOIN crm_users u          ON u.id = o.user_id
        WHERE p.ar_code = ANY($1)
      `, [codes])
      crmResult.rows.forEach(r => { crmMap[r.ar_code] = r })
    }

    // ── Fallback: ลูกค้าที่ไม่มี crm owner ให้ดึงจาก sale_code ──
    const noOwnerSaleCodes = posResult.rows
      .filter(c => !crmMap[c.code]?.owner_user_id && c.sale_code)
      .map(c => c.sale_code)
    const uniqueSaleCodes = [...new Set(noOwnerSaleCodes)]
    const saleUserMap = {}
    if (uniqueSaleCodes.length > 0) {
      const saleUsers = await crmDB.query(
        `SELECT id, code, name FROM crm_users WHERE code = ANY($1) AND is_active = TRUE`,
        [uniqueSaleCodes]
      )
      saleUsers.rows.forEach(u => { saleUserMap[u.code] = u })
    }

    const data = posResult.rows.map(c => {
      let crm = crmMap[c.code] || null
      if (crm && !crm.owner_user_id && c.sale_code && saleUserMap[c.sale_code]) {
        const su = saleUserMap[c.sale_code]
        crm = { ...crm, owner_user_id: su.id, owner_code: su.code, owner_name: su.name, owner_from_sale_code: true }
      }
      return { ...c, crm }
    })

    res.json({ data, total, page: parseInt(page), limit: parseInt(limit) })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────
// GET /api/customers/provinces
// GET /api/customers/ampers?province=xx
// GET /api/customers/tambons?province=xx&amper=yy
// ─────────────────────────────────────────────
router.get('/provinces', async (req, res) => {
  try {
    const r = await posDB.query(`SELECT code, name_1 FROM erp_province ORDER BY name_1`)
    res.json(r.rows)
  } catch (err) { res.status(500).json({ error: err.message }) }
})

router.get('/ampers', async (req, res) => {
  try {
    const { province } = req.query
    if (!province) return res.json([])
    const r = await posDB.query(
      `SELECT code, name_1, province FROM erp_amper WHERE province=$1 ORDER BY name_1`, [province]
    )
    res.json(r.rows)
  } catch (err) { res.status(500).json({ error: err.message }) }
})

router.get('/tambons', async (req, res) => {
  try {
    const { province, amper } = req.query
    if (!province || !amper) return res.json([])
    const r = await posDB.query(
      `SELECT code, name_1, amper, province FROM erp_tambon WHERE province=$1 AND amper=$2 ORDER BY name_1`,
      [province, amper]
    )
    res.json(r.rows)
  } catch (err) { res.status(500).json({ error: err.message }) }
})

// ─────────────────────────────────────────────
// GET /api/customers/:code  — ดูลูกค้าคนเดียว
// ─────────────────────────────────────────────
router.get('/:code', async (req, res) => {
  const { code } = req.params
  try {
    // ข้อมูลหลักจาก POS
    const [cusResult, contactResult, detailResult, transportResult] = await Promise.all([
      posDB.query(`
        SELECT c.*, d.sale_code, u.name_1 AS sale_name
        FROM ar_customer c
        LEFT JOIN ar_customer_detail d ON d.ar_code = c.code
        LEFT JOIN erp_user u           ON u.code = d.sale_code
        WHERE c.code = $1
      `, [code]),

      posDB.query(`
        SELECT * FROM ar_contactor WHERE ar_code = $1 ORDER BY name
      `, [code]),

      posDB.query(`
        SELECT d.*, u.name_1 AS sale_name
        FROM ar_customer_detail d
        LEFT JOIN erp_user u ON u.code = d.sale_code
        WHERE d.ar_code = $1
      `, [code]),

      posDB.query(`
        SELECT * FROM ap_ar_transport_label WHERE cust_code = $1
      `, [code])
    ])

    if (cusResult.rows.length === 0) {
      return res.status(404).json({ error: 'ไม่พบลูกค้า' })
    }

    // CRM data
    const crmResult = await crmDB.query(`
      SELECT
        p.*,
        o.user_id AS owner_user_id,
        u.code    AS owner_code,
        u.name    AS owner_name
      FROM crm_customer_profile p
      LEFT JOIN crm_customer_owner o ON o.ar_code = p.ar_code AND o.is_primary = TRUE
      LEFT JOIN crm_users u          ON u.id = o.user_id
      WHERE p.ar_code = $1
    `, [code])

    let crm = crmResult.rows[0] || null

    // ── Fallback: ถ้าไม่มี crm owner ให้ดึงจาก sale_code ใน POS ──
    if (crm && !crm.owner_user_id) {
      const saleCode = detailResult.rows[0]?.sale_code || cusResult.rows[0]?.sale_code
      if (saleCode) {
        const fallback = await crmDB.query(
          `SELECT id, code, name FROM crm_users WHERE code = $1 AND is_active = TRUE LIMIT 1`,
          [saleCode]
        )
        if (fallback.rows.length) {
          crm = { ...crm, owner_user_id: fallback.rows[0].id, owner_code: fallback.rows[0].code, owner_name: fallback.rows[0].name, owner_from_sale_code: true }
        }
      }
    }

    // แปลง website "lat,lng" → latitude, longitude
    const cus = { ...cusResult.rows[0] }
    if (cus.website && /^-?\d+\.?\d*,-?\d+\.?\d*$/.test(cus.website.trim())) {
      const [lat, lng] = cus.website.trim().split(',').map(Number)
      cus.latitude  = lat
      cus.longitude = lng
      cus.website   = null
    } else {
      cus.latitude  = null
      cus.longitude = null
    }

    res.json({
      customer: cus,
      contactors: contactResult.rows,
      detail: detailResult.rows[0] || null,
      transport_labels: transportResult.rows,
      crm,
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────
// POST /api/customers  — เพิ่มลูกค้าใหม่
// ─────────────────────────────────────────────
router.post('/', async (req, res) => {
  const posClient = await posDB.connect()
  const crmClient = await crmDB.connect()
  try {
    await posClient.query('BEGIN')
    await crmClient.query('BEGIN')

    const {
      code, name_1, country, address, province, amper, tambon, zip_code, remark,
      latitude, longitude,
      sale_code,
      contactors = [],
      transport_labels = [],
      crm = {}
    } = req.body

    if (!code || !name_1) {
      return res.status(400).json({ error: 'รหัสลูกค้าและชื่อลูกค้าต้องกรอก' })
    }

    // เก็บพิกัดใน website column เป็น "lat,lng"
    const geoWebsite = (latitude && longitude) ? `${latitude},${longitude}` : null

    // Insert ar_customer
    await posClient.query(`
      INSERT INTO ar_customer (code, name_1, country, address, province, amper, tambon, zip_code, remark, website)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `, [code, name_1, country, address, province, amper, tambon, zip_code, remark, geoWebsite])

    // Insert ar_customer_detail (sale owner)
    if (sale_code) {
      await posClient.query(`
        INSERT INTO ar_customer_detail (ar_code, sale_code)
        VALUES ($1, $2)
        ON CONFLICT (ar_code) DO UPDATE SET sale_code = EXCLUDED.sale_code
      `, [code, sale_code])
    }

    // Insert ar_contactor (ผู้ติดต่อ)
    for (const c of contactors) {
      await posClient.query(`
        INSERT INTO ar_contactor (ar_code, name, email, telephone, birthday, work_title)
        VALUES ($1,$2,$3,$4,$5,$6)
      `, [code, c.name, c.email, c.telephone, c.birthday || null, c.work_title])
    }

    // Insert transport labels
    for (const t of transport_labels) {
      await posClient.query(`
        INSERT INTO ap_ar_transport_label (cust_code, country, address, province, amper, tambon, zip_code, latitude, longitude)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      `, [code, t.country, t.address, t.province, t.amper, t.tambon, t.zip_code, t.latitude || 0.0, t.longitude || 0.0])
    }

    // Insert CRM profile
    await crmClient.query(`
      INSERT INTO crm_customer_profile (ar_code, customer_type, status, priority, source, crm_remark)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (ar_code) DO NOTHING
    `, [
      code,
      crm.customer_type || 'B2C',
      crm.status || 'active',
      crm.priority || 'normal',
      crm.source || null,
      crm.crm_remark || null
    ])

    // Assign owner ใน CRM ถ้ามี
    if (crm.owner_user_id) {
      await crmClient.query(`
        INSERT INTO crm_customer_owner (ar_code, user_id, is_primary)
        VALUES ($1,$2,TRUE)
        ON CONFLICT (ar_code, user_id) DO NOTHING
      `, [code, crm.owner_user_id])
    }

    await posClient.query('COMMIT')
    await crmClient.query('COMMIT')

    // Audit Log — INSERT
    await logAudit({ tableName: 'ar_customer', recordId: code, arCode: code, action: 'INSERT', newData: req.body }, req)

    res.status(201).json({ success: true, code })
  } catch (err) {
    await posClient.query('ROLLBACK').catch(() => {})
    await crmClient.query('ROLLBACK').catch(() => {})
    console.error(err)
    res.status(500).json({ error: err.message })
  } finally {
    posClient.release()
    crmClient.release()
  }
})

// ─────────────────────────────────────────────
// PUT /api/customers/:code  — แก้ไขลูกค้า
// ─────────────────────────────────────────────
router.put('/:code', async (req, res) => {
  const { code } = req.params
  const posClient = await posDB.connect()
  const crmClient = await crmDB.connect()
  try {
    await posClient.query('BEGIN')
    await crmClient.query('BEGIN')

    const {
      name_1, country, address, province, amper, tambon, zip_code, remark,
      latitude, longitude,
      sale_code,
      contactors = [],
      transport_labels = [],
      crm = {}
    } = req.body

    // เก็บพิกัดใน website column เป็น "lat,lng"
    const geoWebsite = (latitude && longitude) ? `${latitude},${longitude}` : null

    // Update ar_customer
    await posClient.query(`
      UPDATE ar_customer
      SET name_1=$1, country=$2, address=$3, province=$4, amper=$5,
          tambon=$6, zip_code=$7, remark=$8, website=$9
      WHERE code=$10
    `, [name_1, country, address, province, amper, tambon, zip_code, remark, geoWebsite, code])

    // Upsert ar_customer_detail (sale_code)
    if (sale_code !== undefined) {
      await posClient.query(`
        INSERT INTO ar_customer_detail (ar_code, sale_code)
        VALUES ($1, $2)
        ON CONFLICT (ar_code) DO UPDATE SET sale_code = EXCLUDED.sale_code
      `, [code, sale_code])
    }

    // Replace ar_contactor (ลบเก่า + เพิ่มใหม่)
    await posClient.query(`DELETE FROM ar_contactor WHERE ar_code = $1`, [code])
    for (const c of contactors) {
      await posClient.query(`
        INSERT INTO ar_contactor (ar_code, name, email, telephone, birthday, work_title)
        VALUES ($1,$2,$3,$4,$5,$6)
      `, [code, c.name, c.email, c.telephone, c.birthday || null, c.work_title])
    }

    // Replace transport labels
    await posClient.query(`DELETE FROM ap_ar_transport_label WHERE cust_code = $1`, [code])
    for (const t of transport_labels) {
      await posClient.query(`
        INSERT INTO ap_ar_transport_label (cust_code, country, address, province, amper, tambon, zip_code, latitude, longitude)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      `, [code, t.country, t.address, t.province, t.amper, t.tambon, t.zip_code, t.latitude || 0.0, t.longitude || 0.0])
    }

    // Upsert CRM profile
    await crmClient.query(`
      INSERT INTO crm_customer_profile (ar_code, customer_type, status, priority, source, crm_remark, next_followup)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (ar_code) DO UPDATE SET
        customer_type = EXCLUDED.customer_type,
        status        = EXCLUDED.status,
        priority      = EXCLUDED.priority,
        source        = EXCLUDED.source,
        crm_remark    = EXCLUDED.crm_remark,
        next_followup = EXCLUDED.next_followup,
        updated_at    = NOW()
    `, [
      code,
      crm.customer_type || 'B2C',
      crm.status || 'active',
      crm.priority || 'normal',
      crm.source || null,
      crm.crm_remark || null,
      crm.next_followup || null
    ])

    // Update owner
    if (crm.owner_user_id) {
      await crmClient.query(`
        DELETE FROM crm_customer_owner WHERE ar_code = $1 AND is_primary = TRUE
      `, [code])
      await crmClient.query(`
        INSERT INTO crm_customer_owner (ar_code, user_id, is_primary)
        VALUES ($1,$2,TRUE)
        ON CONFLICT (ar_code, user_id) DO NOTHING
      `, [code, crm.owner_user_id])
    }

    await posClient.query('COMMIT')
    await crmClient.query('COMMIT')

    // Audit Log — UPDATE
    await logAudit({ tableName: 'ar_customer', recordId: code, arCode: code, action: 'UPDATE', newData: req.body }, req)

    res.json({ success: true })
  } catch (err) {
    await posClient.query('ROLLBACK').catch(() => {})
    await crmClient.query('ROLLBACK').catch(() => {})
    console.error(err)
    res.status(500).json({ error: err.message })
  } finally {
    posClient.release()
    crmClient.release()
  }
})

// ─────────────────────────────────────────────
// DELETE /api/customers/:code  — ลบลูกค้า
// ─────────────────────────────────────────────
router.delete('/:code', async (req, res) => {
  const { code } = req.params
  const posClient = await posDB.connect()
  const crmClient = await crmDB.connect()
  try {
    await posClient.query('BEGIN')
    await crmClient.query('BEGIN')

    // ลบ POS tables ตามลำดับ FK
    await posClient.query(`DELETE FROM ap_ar_transport_label WHERE cust_code = $1`, [code])
    await posClient.query(`DELETE FROM ar_contactor        WHERE ar_code = $1`, [code])
    await posClient.query(`DELETE FROM ar_customer_detail  WHERE ar_code = $1`, [code])
    await posClient.query(`DELETE FROM ar_customer         WHERE code = $1`, [code])

    // ลบ CRM tables
    await crmClient.query(`DELETE FROM crm_customer_owner   WHERE ar_code = $1`, [code])
    await crmClient.query(`DELETE FROM crm_customer_profile WHERE ar_code = $1`, [code])

    await posClient.query('COMMIT')
    await crmClient.query('COMMIT')

    // Audit Log — DELETE
    await logAudit({ tableName: 'ar_customer', recordId: code, arCode: code, action: 'DELETE' }, req)

    res.json({ success: true })
  } catch (err) {
    await posClient.query('ROLLBACK').catch(() => {})
    await crmClient.query('ROLLBACK').catch(() => {})
    console.error(err)
    res.status(500).json({ error: err.message })
  } finally {
    posClient.release()
    crmClient.release()
  }
})

module.exports = router
