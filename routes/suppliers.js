const express = require('express')
const crypto = require('crypto')
const { posDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

const router = express.Router()
const SUPPLIER_CODE_PATTERN = /^[A-Z0-9_-]+$/

router.use(authMiddleware, requireRole('admin'))

function normalizeText(value) {
  return String(value || '').trim()
}

function normalizeInt(value) {
  const num = parseInt(value, 10)
  return Number.isFinite(num) && num > 0 ? num : 0
}

function httpError(message, statusCode = 400) {
  const err = new Error(message)
  err.statusCode = statusCode
  return err
}

async function withPosTransaction(fn) {
  const client = await posDB.connect()
  try {
    await client.query('BEGIN')
    const result = await fn(client)
    await client.query('COMMIT')
    return result
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    throw err
  } finally {
    client.release()
  }
}

function supplierPayload(body) {
  return {
    code: normalizeText(body.code).toUpperCase(),
    name_1: normalizeText(body.name_1),
    name_2: normalizeText(body.name_2),
    name_eng_1: normalizeText(body.name_eng_1),
    name_eng_2: normalizeText(body.name_eng_2),
    address: normalizeText(body.address),
    tambon: normalizeText(body.tambon),
    amper: normalizeText(body.amper),
    province: normalizeText(body.province),
    zip_code: normalizeText(body.zip_code),
    telephone: normalizeText(body.telephone),
    fax: normalizeText(body.fax),
    email: normalizeText(body.email),
    website: normalizeText(body.website),
    remark: normalizeText(body.remark),
    tax_id: normalizeText(body.tax_id),
    branch_code: normalizeText(body.branch_code),
    branch_type: normalizeInt(body.branch_type),
    credit_day: normalizeInt(body.credit_day),
  }
}

function validateSupplierPayload(payload) {
  if (!payload.code) throw httpError('กรุณาระบุรหัสเจ้าหนี้')
  if (!SUPPLIER_CODE_PATTERN.test(payload.code)) {
    throw httpError('รูปแบบรหัสเจ้าหนี้ไม่ถูกต้อง (อนุญาต A-Z, 0-9, -, _)')
  }
  if (!payload.name_1) throw httpError('กรุณาระบุชื่อเจ้าหนี้')
  if (payload.email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(payload.email)) {
    throw httpError('รูปแบบอีเมลไม่ถูกต้อง')
  }
  if (payload.tax_id && !/^[0-9-]+$/.test(payload.tax_id)) {
    throw httpError('เลขประจำตัวผู้เสียภาษีใช้ได้เฉพาะตัวเลขและ -')
  }
}

router.get('/', async (req, res) => {
  const search = normalizeText(req.query.search)
  const page = Math.max(1, parseInt(req.query.page, 10) || 1)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 20))
  const offset = (page - 1) * limit
  const sortBy = normalizeText(req.query.sort_by) || 'code'
  const sortDir = normalizeText(req.query.sort_dir).toLowerCase() === 'desc' ? 'DESC' : 'ASC'
  const sortWhitelist = {
    code: 's.code',
    name_1: 's.name_1',
    province: 's.province',
    telephone: 's.telephone',
    tax_id: 'd.tax_id',
  }
  const orderBy = `${sortWhitelist[sortBy] || sortWhitelist.code} ${sortDir}`

  const params = []
  const whereParts = []
  const keywords = search.split(/\s+/).filter(Boolean)
  for (const keyword of keywords) {
    params.push(`%${keyword}%`)
    const p = `$${params.length}`
    whereParts.push(`(
      s.code ILIKE ${p}
      OR s.name_1 ILIKE ${p}
      OR COALESCE(s.name_2,'') ILIKE ${p}
      OR COALESCE(s.name_eng_1,'') ILIKE ${p}
      OR COALESCE(s.name_eng_2,'') ILIKE ${p}
      OR COALESCE(s.telephone,'') ILIKE ${p}
      OR COALESCE(d.tax_id,'') ILIKE ${p}
    )`)
  }
  const whereSql = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : ''

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ap_supplier s
       LEFT JOIN ap_supplier_detail d ON d.ap_code = s.code
       ${whereSql}`,
      params,
    )
    const dataResult = await posDB.query(
      `SELECT s.code,
              COALESCE(s.name_1,'') AS name_1,
              COALESCE(s.address,'') AS address,
              COALESCE(s.province,'') AS province,
              COALESCE(s.telephone,'') AS telephone,
              COALESCE(s.email,'') AS email,
              COALESCE(d.tax_id,'') AS tax_id,
              COALESCE(d.branch_code,'') AS branch_code,
              COALESCE(d.branch_type,0) AS branch_type,
              COALESCE(d.credit_day,0) AS credit_day
       FROM ap_supplier s
       LEFT JOIN ap_supplier_detail d ON d.ap_code = s.code
       ${whereSql}
       ORDER BY ${orderBy}
       OFFSET $${params.length + 1} LIMIT $${params.length + 2}`,
      [...params, offset, limit],
    )

    res.json({
      data: dataResult.rows,
      total: countResult.rows[0]?.total || 0,
      page,
      limit,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/:code', async (req, res) => {
  const code = normalizeText(req.params.code)
  try {
    const result = await posDB.query(
      `SELECT s.code,
              COALESCE(s.name_1,'') AS name_1,
              COALESCE(s.name_2,'') AS name_2,
              COALESCE(s.name_eng_1,'') AS name_eng_1,
              COALESCE(s.name_eng_2,'') AS name_eng_2,
              COALESCE(s.address,'') AS address,
              COALESCE(s.tambon,'') AS tambon,
              COALESCE(s.amper,'') AS amper,
              COALESCE(s.province,'') AS province,
              COALESCE(s.zip_code,'') AS zip_code,
              COALESCE(s.telephone,'') AS telephone,
              COALESCE(s.fax,'') AS fax,
              COALESCE(s.email,'') AS email,
              COALESCE(s.website,'') AS website,
              COALESCE(s.remark,'') AS remark,
              COALESCE(d.tax_id,'') AS tax_id,
              COALESCE(d.branch_code,'') AS branch_code,
              COALESCE(d.branch_type,0) AS branch_type,
              COALESCE(d.credit_day,0) AS credit_day
       FROM ap_supplier s
       LEFT JOIN ap_supplier_detail d ON d.ap_code = s.code
       WHERE s.code = $1::text
       LIMIT 1`,
      [code],
    )
    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบเจ้าหนี้' })
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/', async (req, res) => {
  const payload = supplierPayload(req.body || {})
  try {
    validateSupplierPayload(payload)
    await withPosTransaction(async (client) => {
      const exists = await client.query('SELECT 1 FROM ap_supplier WHERE code=$1::text LIMIT 1', [payload.code])
      if (exists.rows.length) throw httpError('รหัสเจ้าหนี้นี้มีอยู่แล้ว')

      await client.query(
        `INSERT INTO ap_supplier (
           code, name_1, name_2, name_eng_1, name_eng_2,
           address, tambon, amper, province, zip_code,
           telephone, fax, email, website, remark,
           status, guid_code, ap_status, create_datetime, last_update_date_time,
           create_code, last_update_code
         ) VALUES (
           $1::text,$2::text,$3::text,$4::text,$5::text,
           $6::text,$7::text,$8::text,$9::text,$10::text,
           $11::text,$12::text,$13::text,$14::text,$15::text,
           0,$16::text,0,NOW(),NOW(),
           $17::text,$17::text
         )`,
        [
          payload.code,
          payload.name_1,
          payload.name_2,
          payload.name_eng_1,
          payload.name_eng_2,
          payload.address,
          payload.tambon,
          payload.amper,
          payload.province,
          payload.zip_code,
          payload.telephone,
          payload.fax,
          payload.email,
          payload.website,
          payload.remark,
          crypto.randomUUID(),
          normalizeText(req.user?.code),
        ],
      )

      await client.query(
        `INSERT INTO ap_supplier_detail (ap_code, tax_id, branch_code, branch_type, credit_day)
         VALUES ($1::text,$2::text,$3::text,$4::integer,$5::integer)
         ON CONFLICT (ap_code) DO UPDATE SET
           tax_id = EXCLUDED.tax_id,
           branch_code = EXCLUDED.branch_code,
           branch_type = EXCLUDED.branch_type,
           credit_day = EXCLUDED.credit_day`,
        [payload.code, payload.tax_id, payload.branch_code, payload.branch_type, payload.credit_day],
      )
    })
    res.status(201).json({ success: true, code: payload.code })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

router.put('/:code', async (req, res) => {
  const payload = supplierPayload({ ...(req.body || {}), code: req.params.code })
  try {
    validateSupplierPayload(payload)
    await withPosTransaction(async (client) => {
      const result = await client.query(
        `UPDATE ap_supplier SET
           name_1=$1::text,
           name_2=$2::text,
           name_eng_1=$3::text,
           name_eng_2=$4::text,
           address=$5::text,
           tambon=$6::text,
           amper=$7::text,
           province=$8::text,
           zip_code=$9::text,
           telephone=$10::text,
           fax=$11::text,
           email=$12::text,
           website=$13::text,
           remark=$14::text,
           last_update_date_time=NOW(),
           last_update_code=$15::text
         WHERE code=$16::text`,
        [
          payload.name_1,
          payload.name_2,
          payload.name_eng_1,
          payload.name_eng_2,
          payload.address,
          payload.tambon,
          payload.amper,
          payload.province,
          payload.zip_code,
          payload.telephone,
          payload.fax,
          payload.email,
          payload.website,
          payload.remark,
          normalizeText(req.user?.code),
          payload.code,
        ],
      )
      if (result.rowCount === 0) throw httpError('ไม่พบเจ้าหนี้', 404)

      await client.query(
        `INSERT INTO ap_supplier_detail (ap_code, tax_id, branch_code, branch_type, credit_day)
         VALUES ($1::text,$2::text,$3::text,$4::integer,$5::integer)
         ON CONFLICT (ap_code) DO UPDATE SET
           tax_id = EXCLUDED.tax_id,
           branch_code = EXCLUDED.branch_code,
           branch_type = EXCLUDED.branch_type,
           credit_day = EXCLUDED.credit_day`,
        [payload.code, payload.tax_id, payload.branch_code, payload.branch_type, payload.credit_day],
      )
    })
    res.json({ success: true, code: payload.code })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

module.exports = router
