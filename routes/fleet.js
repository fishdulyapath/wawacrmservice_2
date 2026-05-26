'use strict'

const express = require('express')
const router = express.Router()
const path = require('path')
const fs = require('fs')
const { google } = require('googleapis')
const { authMiddleware, requireRole } = require('../middleware/auth')
const { crmDB, posDB } = require('../db')
const { syncAllFleetSheets, syncSingleSheet, SYNCS } = require('../services/fleetSync')

const managerRoles = ['admin', 'manager']
const managerOnly = [authMiddleware, requireRole(...managerRoles)]
const adminOnly   = [authMiddleware, requireRole('admin')]
const DRIVE_ROOT_FOLDER_ID = process.env.GOOGLE_DRIVE_APP_FOLDER_ID
  || process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID
  || process.env.APPSHEET_DRIVE_FOLDER_ID

let driveClient = null
const driveFileCache = new Map()
let fleetProblemImagesTableReady = false
let customerStoreLinkReady = false

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------
function dateFilter(from, to, col, startIndex = 1) {
  const parts = []
  const vals  = []
  let i = startIndex
  if (from) { parts.push(`${col} >= $${i++}`); vals.push(from) }
  if (to)   { parts.push(`${col} < ($${i++}::date + interval '1 day')`); vals.push(to) }
  return { clause: parts.length ? 'AND ' + parts.join(' AND ') : '', vals }
}

function pushDateFilter(conds, vals, from, to, col) {
  if (from) { vals.push(from); conds.push(`${col} >= $${vals.length}`) }
  if (to)   { vals.push(to);   conds.push(`${col} < ($${vals.length}::date + interval '1 day')`) }
}

function parseLimit(value, fallback, max) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 1) return fallback
  return Math.min(parsed, max)
}

function parseOffset(value) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed) || parsed < 0) return 0
  return parsed
}

function toNumber(value) {
  const n = Number(value || 0)
  return Number.isFinite(n) ? n : 0
}

async function ensureCustomerStoreLinkTable() {
  if (customerStoreLinkReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS crm_customer_store_link (
      ar_code     TEXT NOT NULL,
      store_id    TEXT NOT NULL,
      link_type   TEXT NOT NULL DEFAULT 'manual',
      confidence  NUMERIC,
      note        TEXT,
      created_by  TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at  TIMESTAMPTZ,
      PRIMARY KEY (ar_code, store_id)
    );
    CREATE INDEX IF NOT EXISTS idx_crm_customer_store_link_ar_code
      ON crm_customer_store_link(ar_code) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_crm_customer_store_link_store_id
      ON crm_customer_store_link(store_id) WHERE deleted_at IS NULL;
  `)
  customerStoreLinkReady = true
}

async function resolveStoreCustomerCode(storeId) {
  await ensureCustomerStoreLinkTable()
  const result = await crmDB.query(`
    SELECT ar_code, link_type, confidence
    FROM crm_customer_store_link
    WHERE store_id = $1
      AND deleted_at IS NULL
    ORDER BY
      CASE WHEN link_type = 'manual' THEN 0 ELSE 1 END,
      confidence DESC NULLS LAST,
      updated_at DESC NULLS LAST,
      created_at DESC NULLS LAST
    LIMIT 1
  `, [storeId])
  const link = result.rows[0] || null
  return {
    cust_code: link?.ar_code || storeId,
    match_type: link ? 'linked' : 'store_id_fallback',
    link,
  }
}

function posDateFilter(from, to, vals, alias = 't') {
  const conds = []
  if (from) { vals.push(from); conds.push(`${alias}.doc_date >= $${vals.length}`) }
  if (to)   { vals.push(to);   conds.push(`${alias}.doc_date <= $${vals.length}`) }
  return conds
}

function paymentFields(row = {}) {
  return {
    cash_amount:     toNumber(row.cash_amount),
    chq_amount:      toNumber(row.chq_amount),
    tranfer_amount:  toNumber(row.tranfer_amount),
    card_amount:     toNumber(row.card_amount),
    deposit_amount:  toNumber(row.deposit_amount),
    advance_amount:  toNumber(row.advance_amount),
    coupon_amount:   toNumber(row.coupon_amount),
    discount_amount: toNumber(row.discount_amount),
    wallet_amount:   toNumber(row.wallet_amount),
  }
}

function netPaymentFields(receive = {}, refund = {}) {
  const received = paymentFields(receive)
  const refunded = paymentFields(refund)
  return Object.fromEntries(
    Object.keys(received).map(key => [key, received[key] - refunded[key]])
  )
}

async function loadStorePosFinancial(storeId, from, to) {
  const customer = await resolveStoreCustomerCode(storeId)
  const salesVals = [customer.cust_code]
  const salesConds = ['t.trans_flag = 44', 't.last_status = 0', 't.cust_code = $1', ...posDateFilter(from, to, salesVals, 't')]
  const returnVals = [customer.cust_code]
  const returnConds = ['t.trans_flag = 48', 't.last_status = 0', 't.cust_code = $1', ...posDateFilter(from, to, returnVals, 't')]
  const receiveVals = [customer.cust_code]
  const receiveDateConds = posDateFilter(from, to, receiveVals, 't')
  const receiveDateWhere = receiveDateConds.length ? `AND ${receiveDateConds.join(' AND ')}` : ''
  const refundVals = [customer.cust_code]
  const refundConds = ['t.trans_flag = 48', 't.status = 0', 'i.last_status = 0', 'i.cust_code = $1', ...posDateFilter(from, to, refundVals, 't')]
  const paymentSelect = `
    COALESCE(SUM(t.cash_amount), 0) AS cash_amount,
    COALESCE(SUM(t.chq_amount), 0) AS chq_amount,
    COALESCE(SUM(t.tranfer_amount), 0) AS tranfer_amount,
    COALESCE(SUM(t.card_amount), 0) AS card_amount,
    COALESCE(SUM(t.deposit_amount), 0) AS deposit_amount,
    COALESCE(SUM(t.advance_amount), 0) AS advance_amount,
    COALESCE(SUM(t.coupon_amount), 0) AS coupon_amount,
    COALESCE(SUM(t.discount_amount), 0) AS discount_amount,
    COALESCE(SUM(t.wallet_amount), 0) AS wallet_amount
  `

  const [salesResult, returnResult, receiveResult, refundResult] = await Promise.all([
    posDB.query(`
      SELECT
        COALESCE(SUM(total_amount), 0) AS total,
        COALESCE(SUM(CASE WHEN inquiry_type = 0 THEN total_amount ELSE 0 END), 0) AS credit_total,
        COALESCE(SUM(CASE WHEN inquiry_type = 1 THEN total_amount ELSE 0 END), 0) AS cash_total
      FROM ic_trans t
      WHERE ${salesConds.join(' AND ')}
    `, salesVals),
    posDB.query(`SELECT COALESCE(SUM(total_amount), 0) AS total FROM ic_trans t WHERE ${returnConds.join(' AND ')}`, returnVals),
    posDB.query(`
      SELECT ${paymentSelect}
      FROM (
        SELECT
          t.cash_amount, t.chq_amount, t.tranfer_amount, t.card_amount,
          t.deposit_amount, t.advance_amount, t.coupon_amount,
          t.discount_amount, t.wallet_amount
        FROM cb_trans t
        JOIN ic_trans i ON i.doc_no = t.doc_no AND i.trans_flag = t.trans_flag
        WHERE t.trans_flag = 44
          AND t.status = 0
          AND i.last_status = 0
          AND i.cust_code = $1
          ${receiveDateWhere}
        UNION ALL
        SELECT
          t.cash_amount, t.chq_amount, t.tranfer_amount, t.card_amount,
          t.deposit_amount, t.advance_amount, t.coupon_amount,
          t.discount_amount, t.wallet_amount
        FROM cb_trans t
        WHERE t.trans_flag = 239
          AND t.status = 0
          AND t.ap_ar_code = $1
          ${receiveDateWhere}
      ) t
    `, receiveVals),
    posDB.query(`
      SELECT ${paymentSelect}
      FROM cb_trans t
      JOIN ic_trans i ON i.doc_no = t.doc_no AND i.trans_flag = t.trans_flag
      WHERE ${refundConds.join(' AND ')}
    `, refundVals),
  ])

  const grossSales = toNumber(salesResult.rows[0]?.total)
  const creditSales = toNumber(salesResult.rows[0]?.credit_total)
  const cashSales = toNumber(salesResult.rows[0]?.cash_total)
  const returnTotal = toNumber(returnResult.rows[0]?.total)
  const netSales = grossSales - returnTotal
  const paymentNet = netPaymentFields(receiveResult.rows[0], refundResult.rows[0])

  return {
    cust_code: customer.cust_code,
    match_type: customer.match_type,
    period: { from: from || null, to: to || null },
    gross_sales: grossSales,
    credit_sales: creditSales,
    cash_sales: cashSales,
    return_total: returnTotal,
    net_sales: netSales,
    return_rate: netSales ? Math.round((returnTotal / netSales) * 1000) / 10 : 0,
    payment_received: paymentFields(receiveResult.rows[0]),
    payment_refunded: paymentFields(refundResult.rows[0]),
    payment_net: paymentNet,
  }
}

function resolveKeyPath() {
  const envPath = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH
  if (envPath) {
    const abs = path.isAbsolute(envPath) ? envPath : path.resolve(__dirname, '..', envPath)
    if (fs.existsSync(abs)) return abs
  }
  try {
    const backendDir = path.resolve(__dirname, '..')
    const match = fs.readdirSync(backendDir).find(f => /^appsheet-.*\.json$/.test(f))
    if (match) return path.join(backendDir, match)
  } catch {}
  return null
}

function escapeDriveQuery(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'")
}

async function ensureFleetProblemImagesTable() {
  if (fleetProblemImagesTableReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS fleet_problem_images (
      image_problem_id TEXT PRIMARY KEY,
      problem_id       TEXT NOT NULL,
      image_path       TEXT,
      note             TEXT,
      created_at       TIMESTAMPTZ,
      synced_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at       TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_problem_images_problem_id ON fleet_problem_images(problem_id);
    CREATE INDEX IF NOT EXISTS idx_fleet_problem_images_created_at ON fleet_problem_images(created_at);
  `)
  fleetProblemImagesTableReady = true
}

async function getDriveClient() {
  if (driveClient) return driveClient
  const keyFile = resolveKeyPath()
  if (!keyFile) throw new Error('ไม่พบ Google service account key สำหรับโหลดรูป')

  const auth = new google.auth.GoogleAuth({
    keyFile,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  })
  driveClient = google.drive({ version: 'v3', auth })
  return driveClient
}

async function findChildFolder(drive, parentId, folderName) {
  const result = await drive.files.list({
    q: `'${parentId}' in parents and name = '${escapeDriveQuery(folderName)}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    fields: 'files(id,name)',
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  })
  return result.data.files?.[0]?.id || null
}

async function findDriveFileByPath(filePath) {
  const normalized = decodeURIComponent(String(filePath)).replace(/\\/g, '/').replace(/^\/+/, '')
  if (!normalized) throw new Error('path รูปไม่ถูกต้อง')
  if (driveFileCache.has(normalized)) return driveFileCache.get(normalized)

  const drive = await getDriveClient()
  const parts = normalized.split('/').filter(Boolean)
  const fileName = parts.pop()
  let parentId = DRIVE_ROOT_FOLDER_ID || null

  if (parentId && parts.length) {
    for (const folder of parts) {
      const nextParent = await findChildFolder(drive, parentId, folder)
      if (!nextParent) {
        parentId = null
        break
      }
      parentId = nextParent
    }
  }

  const parentClause = parentId ? `'${parentId}' in parents and ` : ''
  const result = await drive.files.list({
    q: `${parentClause}name = '${escapeDriveQuery(fileName)}' and trashed = false`,
    fields: 'files(id,name,mimeType,modifiedTime,size)',
    orderBy: 'modifiedTime desc',
    pageSize: 1,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  })

  const file = result.data.files?.[0]
  if (!file) throw new Error(`ไม่พบไฟล์รูปใน Google Drive: ${normalized}`)
  driveFileCache.set(normalized, file)
  return file
}

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/summary
// KPI: รายได้รวม, จำนวน trips, จำนวน drivers, จำนวน stores ที่แวะ
// ---------------------------------------------------------------------------
router.get('/dashboard/summary', managerOnly, async (req, res) => {
  try {
    const { from, to } = req.query
    const { clause: crClause, vals: crVals } = dateFilter(from, to, 'cr.trip_date')
    const { clause: coClause, vals: coVals } = dateFilter(from, to, 'co.date_time_check_out')

    // revenue/bills/stores จาก check_outs (มี timestamp จริง)
    const coSql = `
      SELECT
        COALESCE(SUM(co.amount), 0)         AS total_revenue,
        COALESCE(SUM(co.cash), 0)           AS total_cash,
        COALESCE(SUM(co.transfer), 0)       AS total_transfer,
        COUNT(DISTINCT co.check_out_id)     AS total_checkouts,
        COUNT(DISTINCT ls.store_id)         AS total_stores_visited
      FROM fleet_check_outs co
      JOIN fleet_list_stores ls ON ls.list_id = co.list_id
      WHERE co.deleted_at IS NULL AND ls.deleted_at IS NULL ${coClause}`
    const coResult = await crmDB.query(coSql, coVals)

    // trips/drivers จาก car_releases (ใช้ trip_date)
    const crSql = `
      SELECT
        COUNT(DISTINCT cr.car_release_id)   AS total_trips,
        COUNT(DISTINCT cr.user_id)          AS total_drivers,
        COUNT(DISTINCT cr.car_id)           AS total_cars
      FROM fleet_car_releases cr
      WHERE cr.deleted_at IS NULL ${crClause}`
    const crResult = await crmDB.query(crSql, crVals)

    res.json({ success: true, data: { ...crResult.rows[0], ...coResult.rows[0] } })
  } catch (err) {
    console.error('[fleet/summary]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/monthly
// รายได้รายเดือน
// ---------------------------------------------------------------------------
router.get('/dashboard/monthly', managerOnly, async (req, res) => {
  try {
    const { from, to } = req.query
    const { clause, vals } = dateFilter(from, to, 'co.date_time_check_out')
    const sql = `
      SELECT
        TO_CHAR(DATE_TRUNC('month', co.date_time_check_out AT TIME ZONE 'Asia/Bangkok'), 'YYYY-MM') AS month,
        COUNT(DISTINCT ls.group_store_id)                AS trips,    -- 1:1 with car_release
        COALESCE(SUM(co.amount), 0)                      AS revenue,
        COALESCE(SUM(co.cash), 0)                        AS cash,
        COALESCE(SUM(co.transfer), 0)                    AS transfer,
        COUNT(DISTINCT co.check_out_id)                  AS checkouts
      FROM fleet_check_outs co
      JOIN fleet_list_stores ls ON ls.list_id = co.list_id
      WHERE co.deleted_at IS NULL AND ls.deleted_at IS NULL ${clause}
      GROUP BY DATE_TRUNC('month', co.date_time_check_out AT TIME ZONE 'Asia/Bangkok')
      ORDER BY 1`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/monthly]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/top-drivers?limit=10
// ---------------------------------------------------------------------------
router.get('/dashboard/top-drivers', managerOnly, async (req, res) => {
  try {
    const { from, to, limit = 10 } = req.query
    const limitVal = parseInt(limit) || 10
    const { clause, vals: dateVals } = dateFilter(from, to, 'co.date_time_check_out', 1)
    const vals = [...dateVals, limitVal]
    const sql = `
      SELECT
        cr.user_id,
        u.name                                AS driver_name,
        COUNT(DISTINCT cr.car_release_id)     AS trips,
        COALESCE(SUM(co.amount), 0)           AS revenue,
        COUNT(DISTINCT co.check_out_id)       AS checkouts
      FROM fleet_check_outs co
      JOIN fleet_list_stores ls ON ls.list_id = co.list_id
      JOIN fleet_car_releases cr ON cr.group_store_id = ls.group_store_id
      LEFT JOIN fleet_users u ON u.user_id = cr.user_id
      WHERE co.deleted_at IS NULL AND ls.deleted_at IS NULL AND cr.deleted_at IS NULL ${clause}
      GROUP BY cr.user_id, u.name
      ORDER BY revenue DESC
      LIMIT $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/top-drivers]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/top-drivers-trips?limit=10
// ---------------------------------------------------------------------------
router.get('/dashboard/top-drivers-trips', managerOnly, async (req, res) => {
  try {
    const { from, to, limit = 10 } = req.query
    const limitVal = parseInt(limit) || 10
    const { clause, vals: dateVals } = dateFilter(from, to, 'cr.trip_date', 1)
    const vals = [...dateVals, limitVal]
    const sql = `
      SELECT
        cr.user_id,
        u.name                                AS driver_name,
        COUNT(DISTINCT cr.car_release_id)     AS trips,
        COALESCE(SUM(co.amount), 0)           AS revenue
      FROM fleet_car_releases cr
      LEFT JOIN fleet_users u ON u.user_id = cr.user_id
      LEFT JOIN fleet_list_stores ls ON ls.group_store_id = cr.group_store_id AND ls.deleted_at IS NULL
      LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
      WHERE cr.deleted_at IS NULL ${clause}
      GROUP BY cr.user_id, u.name
      ORDER BY trips DESC
      LIMIT $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/top-drivers-trips]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})
router.get('/dashboard/top-cars', managerOnly, async (req, res) => {
  try {
    const { from, to, limit = 10 } = req.query
    const limitVal = parseInt(limit) || 10
    const { clause, vals: dateVals } = dateFilter(from, to, 'co.date_time_check_out', 1)
    const vals = [...dateVals, limitVal]
    const sql = `
      SELECT
        cr.car_id,
        c.car_name,
        c.license_plate,
        COUNT(DISTINCT cr.car_release_id)   AS trips,
        COALESCE(SUM(co.amount), 0)         AS revenue
      FROM fleet_check_outs co
      JOIN fleet_list_stores ls ON ls.list_id = co.list_id
      JOIN fleet_car_releases cr ON cr.group_store_id = ls.group_store_id
      LEFT JOIN fleet_cars c ON c.car_id = cr.car_id
      WHERE co.deleted_at IS NULL AND ls.deleted_at IS NULL AND cr.deleted_at IS NULL ${clause}
      GROUP BY cr.car_id, c.car_name, c.license_plate
      ORDER BY revenue DESC
      LIMIT $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/top-cars]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/top-stores?by=visits|revenue&limit=10
// ---------------------------------------------------------------------------
router.get('/dashboard/top-stores', managerOnly, async (req, res) => {
  try {
    const { from, to, by = 'revenue', limit = 10 } = req.query
    const orderCol = by === 'visits' ? 'visits' : 'revenue'
    const limitVal = parseInt(limit) || 10
    const { clause, vals: dateVals } = dateFilter(from, to, 'co.date_time_check_out', 1)
    const vals = [...dateVals, limitVal]
    const sql = `
      SELECT
        s.store_id,
        s.store_name,
        s.zone,
        COUNT(DISTINCT co.check_out_id)     AS visits,
        COALESCE(SUM(co.amount), 0)         AS revenue
      FROM fleet_check_outs co
      JOIN fleet_list_stores ls ON ls.list_id = co.list_id
      JOIN fleet_stores s ON s.store_id = ls.store_id
      WHERE co.deleted_at IS NULL AND ls.deleted_at IS NULL AND s.store_id IS NOT NULL ${clause}
      GROUP BY s.store_id, s.store_name, s.zone
      ORDER BY ${orderCol} DESC
      LIMIT $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/top-stores]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/day-of-week
// จำนวน checkouts และ revenue ตามวันในสัปดาห์
// ---------------------------------------------------------------------------
router.get('/dashboard/day-of-week', managerOnly, async (req, res) => {
  try {
    const { from, to } = req.query
    const { clause, vals } = dateFilter(from, to, 'co.date_time_check_out')
    const days = ['อาทิตย์','จันทร์','อังคาร','พุธ','พฤหัสบดี','ศุกร์','เสาร์']
    const sql = `
      SELECT
        EXTRACT(DOW FROM co.date_time_check_out AT TIME ZONE 'Asia/Bangkok')::int  AS dow,
        COUNT(DISTINCT co.check_out_id)   AS checkouts,
        COALESCE(SUM(co.amount), 0)       AS revenue
      FROM fleet_check_outs co
      WHERE co.deleted_at IS NULL ${clause}
      GROUP BY EXTRACT(DOW FROM co.date_time_check_out AT TIME ZONE 'Asia/Bangkok')
      ORDER BY 1`
    const result = await crmDB.query(sql, vals)
    const data = result.rows.map(r => ({ ...r, day_name: days[r.dow] }))
    res.json({ success: true, data })
  } catch (err) {
    console.error('[fleet/day-of-week]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/dashboard/problems
// ประเภทปัญหาและความถี่
// ---------------------------------------------------------------------------
router.get('/dashboard/problems', managerOnly, async (req, res) => {
  try {
    const { from, to } = req.query
    const { clause, vals } = dateFilter(from, to, 'p.created_at')
    const sql = `
      SELECT
        COALESCE(p.problem_type, 'ไม่ระบุ') AS problem_type,
        COUNT(*)                             AS count,
        SUM(CASE WHEN p.is_resolved THEN 1 ELSE 0 END) AS resolved_count
      FROM fleet_problems p
      WHERE p.deleted_at IS NULL ${clause}
      GROUP BY p.problem_type
      ORDER BY count DESC`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/problems]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/problems?type=ร้านปิด&from=&to=&limit=30&offset=0
// รายการปัญหาย่อยสำหรับ drill-down
// ---------------------------------------------------------------------------
router.get('/problems', managerOnly, async (req, res) => {
  try {
    const { type, from, to, limit = 30, offset = 0 } = req.query
    const vals = []
    const conds = ['p.deleted_at IS NULL', "p.problem_type IS NOT NULL"]
    if (type) { vals.push(type); conds.push(`p.problem_type = $${vals.length}`) }
    if (from) { vals.push(from); conds.push(`p.created_at >= $${vals.length}`) }
    if (to)   { vals.push(to);   conds.push(`p.created_at < ($${vals.length}::date + interval '1 day')`) }
    const where = 'WHERE ' + conds.join(' AND ')

    const countResult = await crmDB.query(`SELECT COUNT(*) FROM fleet_problems p ${where}`, vals)
    const total = parseInt(countResult.rows[0].count)

    vals.push(parseInt(limit) || 30)
    vals.push(parseInt(offset) || 0)
    const sql = `
      SELECT
        p.problem_id, p.problem_type, p.description, p.image_problem,
        p.is_resolved, p.created_at,
        s.store_name, s.zone,
        cr.car_release_code, cr.trip_date,
        u.name AS driver_name
      FROM fleet_problems p
      LEFT JOIN fleet_list_stores ls ON ls.list_id = p.list_id AND ls.deleted_at IS NULL
      LEFT JOIN fleet_stores s ON s.store_id = ls.store_id
      LEFT JOIN fleet_car_releases cr ON cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
      LEFT JOIN fleet_users u ON u.user_id = cr.user_id
      ${where}
      ORDER BY p.created_at DESC
      LIMIT $${vals.length - 1} OFFSET $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows, meta: { total, limit: parseInt(limit), offset: parseInt(offset) } })
  } catch (err) {
    console.error('[fleet/problems]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/image?path=... — proxy รูปจาก AppSheet/Google Drive
// ---------------------------------------------------------------------------
// GET /api/fleet/stores/search?q=
router.get('/stores/search', managerOnly, async (req, res) => {
  try {
    const { q = '', limit = 20 } = req.query
    const keyword = String(q || '').trim()
    const limitVal = parseLimit(limit, 20, 50)
    const vals = []
    const conds = ['s.deleted_at IS NULL']

    if (keyword) {
      vals.push(`%${keyword}%`)
      conds.push(`(
        s.store_id ILIKE $${vals.length}
        OR s.store_name ILIKE $${vals.length}
        OR s.phone ILIKE $${vals.length}
        OR s.address ILIKE $${vals.length}
        OR s.zone ILIKE $${vals.length}
        OR EXISTS (
          SELECT 1
          FROM fleet_list_stores lsq
          LEFT JOIN fleet_check_outs coq ON coq.list_id = lsq.list_id AND coq.deleted_at IS NULL
          WHERE lsq.deleted_at IS NULL
            AND lsq.store_id = s.store_id
            AND (
              lsq.store_name_result ILIKE $${vals.length}
              OR lsq.data_store_no ILIKE $${vals.length}
              OR coq.check_out_id ILIKE $${vals.length}
              OR coq.payment_id ILIKE $${vals.length}
            )
        )
      )`)
    }

    vals.push(limitVal)
    const sql = `
      SELECT
        s.store_id, s.store_name, s.address, s.phone, s.location, s.zone,
        COALESCE(stats.deliveries, 0)::int AS deliveries,
        COALESCE(stats.revenue, 0) AS revenue,
        COALESCE(stats.problem_count, 0)::int AS problem_count,
        COALESCE(stats.return_count, 0)::int AS return_count,
        stats.latest_delivery_at
      FROM fleet_stores s
      LEFT JOIN LATERAL (
        SELECT
          COUNT(DISTINCT ls.list_id) AS deliveries,
          COALESCE(SUM(co.amount), 0) AS revenue,
          COALESCE(SUM(prob.problem_count), 0) AS problem_count,
          COALESCE(SUM(ret.return_count), 0) AS return_count,
          MAX(COALESCE(co.date_time_check_out, ci.date_time_check_in, ls.created_at)) AS latest_delivery_at
        FROM fleet_list_stores ls
        LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
        LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS return_count
          FROM fleet_return_products rp
          WHERE rp.deleted_at IS NULL AND rp.check_out_id = co.check_out_id
        ) ret ON TRUE
        LEFT JOIN LATERAL (
          SELECT COUNT(*) AS problem_count
          FROM fleet_problems p
          WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
        ) prob ON TRUE
        WHERE ls.deleted_at IS NULL AND ls.store_id = s.store_id
      ) stats ON TRUE
      WHERE ${conds.join(' AND ')}
      ORDER BY stats.latest_delivery_at DESC NULLS LAST, s.store_name NULLS LAST, s.store_id
      LIMIT $${vals.length}`

    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    console.error('[fleet/stores/search]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// GET /api/fleet/stores/:storeId/report?from=&to=&bill=&limit=&offset=
router.get('/stores/:storeId/report', managerOnly, async (req, res) => {
  try {
    const { storeId } = req.params
    const { from, to, pos_from, pos_to, bill = '', limit = 20, offset = 0 } = req.query
    const limitVal = parseLimit(limit, 20, 100)
    const offsetVal = parseOffset(offset)

    const storeResult = await crmDB.query(
      `SELECT store_id, store_name, address, phone, location, zone, created_at, synced_at
       FROM fleet_stores
       WHERE deleted_at IS NULL AND store_id = $1
       LIMIT 1`,
      [storeId]
    )
    if (!storeResult.rows.length) return res.status(404).json({ success: false, error: 'ไม่พบร้านค้า' })

    const baseDateCol = `COALESCE(co.date_time_check_out, ci.date_time_check_in, ls.created_at)`
    const baseConds = ['ls.deleted_at IS NULL', 'ls.store_id = $1']
    const baseVals = [storeId]
    pushDateFilter(baseConds, baseVals, from, to, baseDateCol)
    const baseWhere = baseConds.join(' AND ')

    const timelineConds = [...baseConds]
    const timelineValsBase = [...baseVals]
    const billQuery = String(bill || '').trim()
    if (billQuery) {
      timelineValsBase.push(`%${billQuery}%`)
      timelineConds.push(`(
        ls.data_store_no ILIKE $${timelineValsBase.length}
        OR ls.store_name_result ILIKE $${timelineValsBase.length}
        OR co.check_out_id::text ILIKE $${timelineValsBase.length}
        OR cr.car_release_code ILIKE $${timelineValsBase.length}
      )`)
    }
    const timelineWhere = timelineConds.join(' AND ')

    const summaryResult = await crmDB.query(
      `SELECT
         COUNT(DISTINCT ls.list_id)::int AS total_visits,
         COUNT(DISTINCT ci.check_in_id)::int AS checkins,
         COUNT(DISTINCT co.check_out_id)::int AS checkouts,
         COUNT(DISTINCT cr.car_release_id)::int AS trips,
         COALESCE(SUM(co.amount), 0) AS revenue,
         COALESCE(SUM(co.cash), 0) AS cash,
         COALESCE(SUM(co.transfer), 0) AS transfer,
         COALESCE(SUM(co.transfer_according), 0) AS transfer_according,
         COALESCE(SUM(ret.return_total), 0) AS return_total,
         COALESCE(SUM(ret.return_count), 0)::int AS return_items,
         COALESCE(SUM(prob.problem_count), 0)::int AS problem_count,
         COALESCE(SUM(prob.store_closed_count), 0)::int AS store_closed_count,
         COUNT(DISTINCT CASE WHEN ls.bypass THEN ls.list_id END)::int AS bypass_count,
         COUNT(DISTINCT CASE WHEN ls.off_site THEN ls.list_id END)::int AS off_site_count,
         MIN(${baseDateCol}) AS first_visit_at,
         MAX(${baseDateCol}) AS latest_visit_at
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, group_store_id
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS return_count, COALESCE(SUM(total), 0) AS return_total
         FROM fleet_return_products rp
         WHERE rp.deleted_at IS NULL AND rp.check_out_id = co.check_out_id
       ) ret ON TRUE
       LEFT JOIN LATERAL (
         SELECT
           COUNT(*) AS problem_count,
           COUNT(CASE WHEN problem_type ILIKE '%ร้านปิด%' THEN 1 END) AS store_closed_count
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
       WHERE ${baseWhere}`,
      baseVals
    )

    const monthResult = await crmDB.query(
      `SELECT
         TO_CHAR(DATE_TRUNC('month', ${baseDateCol} AT TIME ZONE 'Asia/Bangkok'), 'YYYY-MM') AS month,
         COUNT(DISTINCT ls.list_id)::int AS visits,
         COUNT(DISTINCT co.check_out_id)::int AS checkouts,
         COALESCE(SUM(co.amount), 0) AS revenue,
         COALESCE(SUM(ret.return_total), 0) AS return_total,
         COALESCE(SUM(prob.problem_count), 0)::int AS problem_count,
         COALESCE(SUM(prob.store_closed_count), 0)::int AS store_closed_count
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS return_count, COALESCE(SUM(total), 0) AS return_total
         FROM fleet_return_products rp
         WHERE rp.deleted_at IS NULL AND rp.check_out_id = co.check_out_id
       ) ret ON TRUE
       LEFT JOIN LATERAL (
         SELECT
           COUNT(*) AS problem_count,
           COUNT(CASE WHEN problem_type ILIKE '%ร้านปิด%' THEN 1 END) AS store_closed_count
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
       WHERE ${baseWhere}
       GROUP BY DATE_TRUNC('month', ${baseDateCol} AT TIME ZONE 'Asia/Bangkok')
       ORDER BY 1`,
      baseVals
    )

    const driverResult = await crmDB.query(
      `SELECT
         cr.user_id,
         u.name AS driver_name,
         COUNT(DISTINCT ls.list_id)::int AS visits,
         COUNT(DISTINCT co.check_out_id)::int AS checkouts,
         COALESCE(SUM(co.amount), 0) AS revenue,
         COALESCE(SUM(ret.return_total), 0) AS return_total,
         COALESCE(SUM(prob.problem_count), 0)::int AS problem_count
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, user_id
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN LATERAL (
         SELECT COALESCE(SUM(total), 0) AS return_total
         FROM fleet_return_products rp
         WHERE rp.deleted_at IS NULL AND rp.check_out_id = co.check_out_id
       ) ret ON TRUE
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS problem_count
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
       WHERE ${baseWhere}
       GROUP BY cr.user_id, u.name
       ORDER BY visits DESC, revenue DESC
       LIMIT 8`,
      baseVals
    )

    const paymentResult = await crmDB.query(
      `SELECT
         COALESCE(pay.payment_name, co.payment_id, 'ไม่ระบุ') AS payment_name,
         COUNT(DISTINCT co.check_out_id)::int AS checkouts,
         COALESCE(SUM(co.amount), 0) AS amount
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN fleet_payments pay ON pay.payment_id = co.payment_id AND pay.deleted_at IS NULL
       WHERE ${baseWhere}
       GROUP BY COALESCE(pay.payment_name, co.payment_id, 'ไม่ระบุ')
       ORDER BY amount DESC, checkouts DESC`,
      baseVals
    )

    const posFinancial = await loadStorePosFinancial(storeId, pos_from, pos_to)

    const visitResult = await crmDB.query(
      `SELECT
         COALESCE(v.visit_name, co.visit, 'ไม่ระบุ') AS visit_name,
         COUNT(DISTINCT co.check_out_id)::int AS checkouts,
         COALESCE(SUM(co.amount), 0) AS amount
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN fleet_visits v ON v.visit_id = co.visit AND v.deleted_at IS NULL
       WHERE ${baseWhere}
       GROUP BY COALESCE(v.visit_name, co.visit, 'ไม่ระบุ')
       ORDER BY checkouts DESC, amount DESC`,
      baseVals
    )

    const returnProductResult = await crmDB.query(
      `SELECT
         COALESCE(rp.product_name, 'ไม่ระบุสินค้า') AS product_name,
         COUNT(*)::int AS return_count,
         COALESCE(SUM(rp.quantity), 0) AS quantity,
         COALESCE(SUM(rp.total), 0) AS total
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       JOIN fleet_return_products rp ON rp.check_out_id = co.check_out_id AND rp.deleted_at IS NULL
       WHERE ${baseWhere}
       GROUP BY COALESCE(rp.product_name, 'ไม่ระบุสินค้า')
       ORDER BY total DESC, return_count DESC
       LIMIT 10`,
      baseVals
    )

    const problemBreakdownResult = await crmDB.query(
      `SELECT
         COALESCE(p.problem_type, 'ไม่ระบุ') AS problem_type,
         COUNT(*)::int AS count,
         COUNT(NULLIF(p.image_problem, ''))::int AS image_count,
         SUM(CASE WHEN LOWER(TRIM(COALESCE(p.normal_bill::text, ''))) NOT IN ('', 'false', '0', 'no', 'n') THEN 1 ELSE 0 END)::int AS normal_bill_count,
         SUM(CASE WHEN LOWER(TRIM(COALESCE(p.edit_bill::text, ''))) NOT IN ('', 'false', '0', 'no', 'n') THEN 1 ELSE 0 END)::int AS edit_bill_count,
         SUM(CASE WHEN LOWER(TRIM(COALESCE(p.product_swap::text, ''))) NOT IN ('', 'false', '0', 'no', 'n') THEN 1 ELSE 0 END)::int AS product_swap_count,
         SUM(CASE WHEN LOWER(TRIM(COALESCE(p.out_of_stock::text, ''))) NOT IN ('', 'false', '0', 'no', 'n') THEN 1 ELSE 0 END)::int AS out_of_stock_count,
         SUM(CASE WHEN LOWER(TRIM(COALESCE(p.overstock::text, ''))) NOT IN ('', 'false', '0', 'no', 'n') THEN 1 ELSE 0 END)::int AS overstock_count
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       JOIN fleet_problems p ON p.list_id = ls.list_id AND p.deleted_at IS NULL
       WHERE ${baseWhere}
       GROUP BY COALESCE(p.problem_type, 'ไม่ระบุ')
       ORDER BY count DESC
       LIMIT 10`,
      baseVals
    )

    const issueEventResult = await crmDB.query(
      `SELECT
         ls.list_id, ls.sequence_no, ls.store_name_result, ls.data_store_no,
         ls.off_site, ls.bypass,
         ci.date_time_check_in,
         co.check_out_id, co.date_time_check_out, co.amount,
         cr.car_release_id, cr.car_release_code, cr.trip_date,
         u.name AS driver_name,
         COALESCE(prob.problem_count, 0)::int AS problem_count,
         COALESCE(prob.problem_types, '') AS problem_types
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, car_release_code, trip_date, user_id
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS problem_count, STRING_AGG(DISTINCT COALESCE(problem_type, 'ไม่ระบุ'), ', ') AS problem_types
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
       WHERE ${baseWhere}
         AND (ls.bypass = TRUE OR ls.off_site = TRUE OR co.check_out_id IS NULL OR COALESCE(prob.problem_count, 0) > 0)
       ORDER BY ${baseDateCol} DESC NULLS LAST, ls.created_at DESC NULLS LAST
       LIMIT 50`,
      baseVals
    )

    const storeClosedEventResult = await crmDB.query(
      `SELECT
         ls.list_id, ls.sequence_no, ls.store_name_result, ls.data_store_no,
         ls.off_site, ls.bypass,
         ci.date_time_check_in,
         co.check_out_id, co.date_time_check_out, co.amount,
         cr.car_release_id, cr.car_release_code, cr.trip_date,
         u.name AS driver_name,
         COALESCE(prob.problem_count, 0)::int AS problem_count,
         COALESCE(prob.problem_types, '') AS problem_types
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, car_release_code, trip_date, user_id
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS problem_count, STRING_AGG(DISTINCT COALESCE(problem_type, 'ไม่ระบุ'), ', ') AS problem_types
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
       WHERE ${baseWhere}
         AND EXISTS (
           SELECT 1
           FROM fleet_problems p
           WHERE p.deleted_at IS NULL
             AND p.list_id = ls.list_id
             AND p.problem_type ILIKE '%ร้านปิด%'
         )
       ORDER BY ${baseDateCol} DESC NULLS LAST, ls.created_at DESC NULLS LAST
       LIMIT 100`,
      baseVals
    )

    const timelineCountResult = await crmDB.query(
      `SELECT COUNT(DISTINCT ls.list_id)::int AS total
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_code
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       WHERE ${timelineWhere}`,
      timelineValsBase
    )

    const timelineVals = [...timelineValsBase, limitVal, offsetVal]
    const timelineResult = await crmDB.query(
      `SELECT
         ls.list_id, ls.sequence_no, ls.store_id, ls.store_name_result, ls.data_store_no,
         ls.off_site, ls.bypass, ls.group_store_id,
         ci.check_in_id, ci.date_time_check_in, ci.image_check_in, ci.latitude, ci.longitude,
         co.check_out_id, co.date_time_check_out, co.amount, co.cash, co.transfer,
         co.transfer_according, co.image_bill, co.payment_id, co.visit, co.visit_customer, co.visit_note,
         pay.payment_name,
         COALESCE(v.visit_name, co.visit) AS visit_name,
         cr.car_release_id, cr.car_release_code, cr.trip_date, cr.description AS trip_description,
         u.name AS driver_name, c.car_name, c.license_plate,
         COALESCE(ret.return_total, 0) AS return_total,
         COALESCE(ret.return_count, 0)::int AS return_count,
         COALESCE(prob.problem_count, 0)::int AS problem_count,
         COALESCE(prob.problem_types, '') AS problem_types
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       LEFT JOIN fleet_payments pay ON pay.payment_id = co.payment_id AND pay.deleted_at IS NULL
       LEFT JOIN fleet_visits v ON v.visit_id = co.visit AND v.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, car_release_code, trip_date, description, user_id, car_id
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN fleet_cars c ON c.car_id = cr.car_id
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS return_count, COALESCE(SUM(total), 0) AS return_total
         FROM fleet_return_products rp
         WHERE rp.deleted_at IS NULL AND rp.check_out_id = co.check_out_id
       ) ret ON TRUE
       LEFT JOIN LATERAL (
         SELECT COUNT(*) AS problem_count, STRING_AGG(DISTINCT COALESCE(problem_type, 'ไม่ระบุ'), ', ') AS problem_types
         FROM fleet_problems p
         WHERE p.deleted_at IS NULL AND p.list_id = ls.list_id
       ) prob ON TRUE
        WHERE ${timelineWhere}
        ORDER BY ${baseDateCol} DESC NULLS LAST, ls.created_at DESC NULLS LAST, ls.sequence_no
        LIMIT $${timelineVals.length - 1}
        OFFSET $${timelineVals.length}`,
      timelineVals
    )

    const checkOutIds = timelineResult.rows.map(row => row.check_out_id).filter(Boolean)
    const carReleaseIds = timelineResult.rows.map(row => row.car_release_id).filter(Boolean)

    let checkOutImagesById = {}
    if (checkOutIds.length) {
      const result = await crmDB.query(
        `SELECT image_check_out_id, check_out_id, image_path, note, created_at
         FROM fleet_check_out_images
         WHERE deleted_at IS NULL AND check_out_id = ANY($1)
         ORDER BY created_at NULLS LAST, image_check_out_id`,
        [checkOutIds]
      )
      checkOutImagesById = result.rows.reduce((acc, image) => {
        if (!acc[image.check_out_id]) acc[image.check_out_id] = []
        acc[image.check_out_id].push(image)
        return acc
      }, {})
    }

    let returnsByCheckOut = {}
    let returns = []
    const returnResult = await crmDB.query(
      `SELECT rp.return_product_id, rp.check_out_id, rp.no, rp.product_name, rp.quantity, rp.total,
              co.date_time_check_out, ls.list_id, ls.data_store_no,
              cr.car_release_id, cr.car_release_code, cr.trip_date
       FROM fleet_list_stores ls
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       JOIN fleet_return_products rp ON rp.check_out_id = co.check_out_id AND rp.deleted_at IS NULL
       LEFT JOIN LATERAL (
         SELECT car_release_id, car_release_code, trip_date
         FROM fleet_car_releases cr
         WHERE cr.group_store_id = ls.group_store_id AND cr.deleted_at IS NULL
         ORDER BY cr.trip_date DESC NULLS LAST, cr.created_at DESC NULLS LAST, cr.car_release_id
         LIMIT 1
       ) cr ON TRUE
       WHERE ${baseWhere}
       ORDER BY co.date_time_check_out DESC NULLS LAST, rp.no NULLS LAST`,
      baseVals
    )
    returns = returnResult.rows
    returnsByCheckOut = returns.reduce((acc, item) => {
      if (!acc[item.check_out_id]) acc[item.check_out_id] = []
      acc[item.check_out_id].push(item)
      return acc
    }, {})

    let returnDocumentsByRelease = {}
    if (carReleaseIds.length) {
      const result = await crmDB.query(
        `SELECT return_document_id, car_release_id, image_path, created_by, created_at
         FROM fleet_return_documents
         WHERE deleted_at IS NULL AND car_release_id = ANY($1)
         ORDER BY created_at DESC NULLS LAST, return_document_id`,
        [carReleaseIds]
      )
      returnDocumentsByRelease = result.rows.reduce((acc, doc) => {
        if (!acc[doc.car_release_id]) acc[doc.car_release_id] = []
        acc[doc.car_release_id].push(doc)
        return acc
      }, {})
    }

    let problemsByList = {}
    let problemImagesById = {}
    let problems = []
    const problemResult = await crmDB.query(
      `SELECT *
       FROM (
         SELECT DISTINCT ON (p.problem_id)
                p.problem_id, p.list_id, p.problem_type, p.description, p.image_problem, p.is_resolved,
                p.normal_bill, p.normal_bill_note, p.edit_bill, p.edit_bill_note,
                p.product_swap, p.product_swap_note, p.out_of_stock, p.out_of_stock_note,
                p.overstock, p.overstock_note, p.created_at
         FROM fleet_list_stores ls
         LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
         LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
         JOIN fleet_problems p ON p.list_id = ls.list_id AND p.deleted_at IS NULL
         WHERE ${baseWhere}
         ORDER BY p.problem_id, p.created_at DESC NULLS LAST
       ) store_problems
       ORDER BY created_at DESC NULLS LAST`,
      baseVals
    )
    problems = problemResult.rows
    problemsByList = problems.reduce((acc, problem) => {
      if (!acc[problem.list_id]) acc[problem.list_id] = []
      acc[problem.list_id].push(problem)
      return acc
    }, {})

    const problemIds = problems.map(problem => problem.problem_id).filter(Boolean)
    if (problemIds.length) {
      await ensureFleetProblemImagesTable()
      const imageResult = await crmDB.query(
        `SELECT image_problem_id, problem_id, image_path, note, created_at
         FROM fleet_problem_images
         WHERE deleted_at IS NULL AND problem_id = ANY($1)
         ORDER BY created_at NULLS LAST, image_problem_id`,
        [problemIds]
      )
      problemImagesById = imageResult.rows.reduce((acc, image) => {
        if (!acc[image.problem_id]) acc[image.problem_id] = []
        acc[image.problem_id].push(image)
        return acc
      }, {})
    }

    const problemRows = problems.map(problem => ({
      ...problem,
      problem_image: problem.image_problem,
      problem_images: problemImagesById[problem.problem_id] || [],
    }))

    const timeline = timelineResult.rows.map(row => ({
      ...row,
      check_in_image: row.image_check_in,
      check_out_image: row.image_bill,
      check_out_images: checkOutImagesById[row.check_out_id] || [],
      returns: returnsByCheckOut[row.check_out_id] || [],
      return_documents: returnDocumentsByRelease[row.car_release_id] || [],
      problems: (problemsByList[row.list_id] || []).map(problem => ({
        ...problem,
        problem_image: problem.image_problem,
        problem_images: problemImagesById[problem.problem_id] || [],
      })),
    }))

    const images = []
    for (const item of timeline) {
      const base = {
        list_id: item.list_id,
        check_out_id: item.check_out_id,
        car_release_id: item.car_release_id,
        date: item.date_time_check_out || item.date_time_check_in || item.trip_date,
        data_store_no: item.data_store_no,
      }
      if (item.check_in_image) images.push({ ...base, type: 'check_in', label: 'Check-in', path: item.check_in_image })
      if (item.check_out_image) images.push({ ...base, type: 'bill', label: 'บิล', path: item.check_out_image })
      for (const image of item.check_out_images || []) {
        images.push({ ...base, type: 'check_out', label: image.note || 'ส่งของ', path: image.image_path, image_id: image.image_check_out_id })
      }
      for (const doc of item.return_documents || []) {
        images.push({ ...base, type: 'return_document', label: 'เอกสารคืนของ', path: doc.image_path, image_id: doc.return_document_id })
      }
      for (const problem of item.problems || []) {
        if (problem.problem_image) images.push({ ...base, type: 'problem', label: problem.problem_type || 'ปัญหา', path: problem.problem_image, problem_id: problem.problem_id })
        for (const image of problem.problem_images || []) {
          images.push({ ...base, type: 'problem', label: image.note || problem.problem_type || 'ปัญหา', path: image.image_path, image_id: image.image_problem_id, problem_id: problem.problem_id })
        }
      }
    }

    res.json({
      success: true,
      data: {
        store: storeResult.rows[0],
        summary: summaryResult.rows[0],
        pos_financial: posFinancial,
        analysis: {
          monthly: monthResult.rows,
          top_drivers: driverResult.rows,
          payment_breakdown: paymentResult.rows,
          visit_breakdown: visitResult.rows,
          top_return_products: returnProductResult.rows,
          problem_breakdown: problemBreakdownResult.rows,
          issue_events: issueEventResult.rows,
          store_closed_events: storeClosedEventResult.rows,
        },
        timeline,
        timeline_meta: {
          total: Number(timelineCountResult.rows[0]?.total || 0),
          limit: limitVal,
          offset: offsetVal,
          bill: billQuery,
        },
        returns,
        problems: problemRows,
        images,
      },
    })
  } catch (err) {
    console.error('[fleet/stores/:storeId/report]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

router.get('/image', managerOnly, async (req, res) => {
  try {
    const imagePath = String(req.query.path || '').trim()
    if (!imagePath) return res.status(400).json({ success: false, error: 'กรุณาระบุ path รูป' })

    if (/^https?:\/\//i.test(imagePath)) {
      return res.redirect(imagePath)
    }

    const file = await findDriveFileByPath(imagePath)
    const drive = await getDriveClient()
    const result = await drive.files.get(
      { fileId: file.id, alt: 'media', supportsAllDrives: true },
      { responseType: 'stream' }
    )

    res.setHeader('Content-Type', file.mimeType || 'application/octet-stream')
    res.setHeader('Cache-Control', 'private, max-age=3600')
    result.data
      .on('error', err => {
        console.error('[fleet/image stream]', err.message)
        if (!res.headersSent) res.status(500).end()
      })
      .pipe(res)
  } catch (err) {
    console.error('[fleet/image]', err.message)
    res.status(404).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/trips?from=&to=&q=&driver=&car=&limit=20&offset=0
// ---------------------------------------------------------------------------
router.get('/trips', managerOnly, async (req, res) => {
  try {
    const { from, to, q, driver, car, store, bill, limit = 20, offset = 0 } = req.query
    const vals = []
    const conds = ['cr.deleted_at IS NULL']
    if (from) { vals.push(from); conds.push(`cr.trip_date >= $${vals.length}`) }
    if (to)   { vals.push(to);   conds.push(`cr.trip_date <= $${vals.length}`) }
    if (q) {
      vals.push(`%${q}%`)
      conds.push(`(
        cr.car_release_code ILIKE $${vals.length}
        OR cr.car_release_id ILIKE $${vals.length}
        OR u.name ILIKE $${vals.length}
        OR cr.user_id ILIKE $${vals.length}
      )`)
    }
    if (driver) { vals.push(`%${driver}%`); conds.push(`(u.name ILIKE $${vals.length} OR cr.user_id ILIKE $${vals.length})`) }
    if (car)    { vals.push(`%${car}%`);    conds.push(`(c.car_name ILIKE $${vals.length} OR c.license_plate ILIKE $${vals.length})`) }
    if (store) {
      vals.push(`%${store}%`)
      conds.push(`EXISTS (
        SELECT 1 FROM fleet_list_stores ls2
        LEFT JOIN fleet_stores s2 ON s2.store_id = ls2.store_id
        WHERE ls2.group_store_id = cr.group_store_id
          AND ls2.deleted_at IS NULL
          AND (ls2.store_name_result ILIKE $${vals.length} OR s2.store_name ILIKE $${vals.length})
      )`)
    }
    if (bill) {
      vals.push(`%${bill}%`)
      conds.push(`EXISTS (
        SELECT 1 FROM fleet_list_stores ls3
        JOIN fleet_check_outs co3 ON co3.list_id = ls3.list_id AND co3.deleted_at IS NULL
        WHERE ls3.group_store_id = cr.group_store_id
          AND ls3.deleted_at IS NULL
          AND (co3.normal_bill ILIKE $${vals.length} OR co3.edit_bill ILIKE $${vals.length})
      )`)
    }

    const where = 'WHERE ' + conds.join(' AND ')
    const countResult = await crmDB.query(
      `SELECT COUNT(*) FROM fleet_car_releases cr
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN fleet_cars c ON c.car_id = cr.car_id
       ${where}`, vals)
    const total = parseInt(countResult.rows[0].count)

    vals.push(parseInt(limit) || 20)
    vals.push(parseInt(offset) || 0)
    const sql = `
      SELECT
        cr.car_release_id, cr.car_release_code, cr.description, cr.trip_date, cr.created_at,
        cr.total_number_of_bills, cr.accounting_status,
        COALESCE(SUM(co.amount), 0) AS total_amount,
        u.name AS driver_name, c.car_name, c.license_plate
      FROM fleet_car_releases cr
      LEFT JOIN fleet_users u ON u.user_id = cr.user_id
      LEFT JOIN fleet_cars c ON c.car_id = cr.car_id
      LEFT JOIN fleet_list_stores ls ON ls.group_store_id = cr.group_store_id AND ls.deleted_at IS NULL
      LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
      ${where}
      GROUP BY cr.car_release_id, cr.car_release_code, cr.description, cr.trip_date, cr.created_at,
               cr.total_number_of_bills, cr.accounting_status,
               u.name, c.car_name, c.license_plate
      ORDER BY cr.created_at DESC
      LIMIT $${vals.length - 1} OFFSET $${vals.length}`
    const result = await crmDB.query(sql, vals)
    res.json({ success: true, data: result.rows, meta: { total, limit: parseInt(limit), offset: parseInt(offset) } })
  } catch (err) {
    console.error('[fleet/trips]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/trips/:id/stops/:listId
// ---------------------------------------------------------------------------
router.get('/trips/:id/stops/:listId', managerOnly, async (req, res) => {
  try {
    const { id, listId } = req.params
    const stopResult = await crmDB.query(
      `SELECT ls.list_id, ls.sequence_no, ls.store_id, ls.store_name_result,
              ls.off_site, ls.bypass, ls.data_store_no,
              s.store_name, s.address, s.zone,
              ci.check_in_id, ci.image_check_in, ci.latitude, ci.longitude,
              ci.date_time_check_in, co.date_time_check_out,
              co.check_out_id, co.amount, co.cash, co.transfer, co.transfer_according, co.image_bill,
              co.payment_id, co.visit_customer, co.visit, co.visit_note
       FROM fleet_car_releases cr
       JOIN fleet_list_stores ls ON ls.group_store_id = cr.group_store_id AND ls.deleted_at IS NULL
       LEFT JOIN fleet_stores s ON s.store_id = ls.store_id
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       WHERE cr.car_release_id = $1 AND ls.list_id = $2 AND cr.deleted_at IS NULL
       LIMIT 1`, [id, listId])

    if (!stopResult.rows.length) return res.status(404).json({ success: false, error: 'ไม่พบบิลใน trip นี้' })

    const stop = stopResult.rows[0]
    let checkOutImages = []
    if (stop.check_out_id) {
      const checkOutImagesResult = await crmDB.query(
        `SELECT image_check_out_id, check_out_id, image_path, note, created_at
         FROM fleet_check_out_images
         WHERE deleted_at IS NULL AND check_out_id = $1
         ORDER BY created_at NULLS LAST, image_check_out_id`,
        [stop.check_out_id]
      )
      checkOutImages = checkOutImagesResult.rows
    }

    const problemsResult = await crmDB.query(
      `SELECT problem_id, list_id, problem_type, description, image_problem, is_resolved,
              normal_bill, normal_bill_note, edit_bill, edit_bill_note,
              product_swap, product_swap_note, out_of_stock, out_of_stock_note,
              overstock, overstock_note, created_at
       FROM fleet_problems
       WHERE deleted_at IS NULL AND list_id = $1
       ORDER BY created_at DESC`,
      [stop.list_id]
    )

    const problemIds = problemsResult.rows.map(problem => problem.problem_id).filter(Boolean)
    let problemImagesById = {}
    if (problemIds.length) {
      await ensureFleetProblemImagesTable()
      const problemImagesResult = await crmDB.query(
        `SELECT image_problem_id, problem_id, image_path, note, created_at
         FROM fleet_problem_images
         WHERE deleted_at IS NULL AND problem_id = ANY($1)
         ORDER BY created_at NULLS LAST, image_problem_id`,
        [problemIds]
      )
      problemImagesById = problemImagesResult.rows.reduce((acc, image) => {
        if (!acc[image.problem_id]) acc[image.problem_id] = []
        acc[image.problem_id].push(image)
        return acc
      }, {})
    }

    res.json({
      success: true,
      data: {
        ...stop,
        check_in_image: stop.image_check_in,
        check_out_image: stop.image_bill,
        check_out_images: checkOutImages,
        problems: problemsResult.rows.map(problem => ({
          ...problem,
          problem_image: problem.image_problem,
          problem_images: problemImagesById[problem.problem_id] || [],
        })),
      },
    })
  } catch (err) {
    console.error('[fleet/trips/:id/stops/:listId]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/trips/:id
// ---------------------------------------------------------------------------
router.get('/trips/:id', managerOnly, async (req, res) => {
  try {
    const { id } = req.params
    const summaryOnly = ['1', 'true', 'summary'].includes(String(req.query.summary || '').toLowerCase())
    const tripResult = await crmDB.query(
      `SELECT cr.car_release_id, cr.car_release_code, cr.description, cr.trip_date, cr.created_at,
              cr.total_number_of_bills, cr.accounting_status, cr.mileage, cr.mileage_return,
              cr.car_release_image, cr.car_return_image,
              cr.image_mileage, cr.image_front, cr.image_around_1, cr.image_around_2,
              cr.image_around_3, cr.image_around_4,
              cr.car_return_id, cr.return_image_mileage, cr.return_image_front,
              cr.return_image_around_1, cr.return_image_around_2,
              cr.return_image_around_3, cr.return_image_around_4,
              cr.user_id, cr.car_id, cr.group_store_id, cr.name_car_release_id,
              u.name AS driver_name, c.car_name, c.license_plate,
              COALESCE(SUM(co.amount), 0) AS total_amount
       FROM fleet_car_releases cr
       LEFT JOIN fleet_users u ON u.user_id = cr.user_id
       LEFT JOIN fleet_cars c ON c.car_id = cr.car_id
       LEFT JOIN fleet_list_stores ls ON ls.group_store_id = cr.group_store_id AND ls.deleted_at IS NULL
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
       WHERE cr.car_release_id = $1 AND cr.deleted_at IS NULL
       GROUP BY cr.car_release_id, cr.car_release_code, cr.description, cr.trip_date, cr.created_at,
                cr.total_number_of_bills, cr.accounting_status, cr.mileage, cr.mileage_return,
                cr.car_release_image, cr.car_return_image,
                cr.image_mileage, cr.image_front, cr.image_around_1, cr.image_around_2,
                cr.image_around_3, cr.image_around_4,
                cr.car_return_id, cr.return_image_mileage, cr.return_image_front,
                cr.return_image_around_1, cr.return_image_around_2,
                cr.return_image_around_3, cr.return_image_around_4,
                cr.user_id, cr.car_id, cr.group_store_id, cr.name_car_release_id,
                u.name, c.car_name, c.license_plate`, [id])
    if (!tripResult.rows.length) return res.status(404).json({ success: false, error: 'ไม่พบ trip' })

    const trip = tripResult.rows[0]

    if (summaryOnly) {
      await ensureFleetProblemImagesTable()
      const stopsResult = await crmDB.query(
        `SELECT ls.list_id, ls.sequence_no, ls.store_id, ls.store_name_result,
                ls.off_site, ls.bypass, ls.data_store_no,
                s.store_name, s.zone,
                ci.check_in_id, ci.date_time_check_in,
                co.check_out_id, co.date_time_check_out, co.amount, co.payment_id, co.visit,
                (
                  CASE WHEN NULLIF(ci.image_check_in, '') IS NULL THEN 0 ELSE 1 END
                  + CASE WHEN NULLIF(co.image_bill, '') IS NULL THEN 0 ELSE 1 END
                  + COALESCE(coi.image_count, 0)
                  + COALESCE(p.problem_image_count, 0)
                  + COALESCE(pimg.problem_image_count, 0)
                )::int AS image_count,
                COALESCE(p.problem_count, 0)::int AS problem_count
         FROM fleet_list_stores ls
         LEFT JOIN fleet_stores s ON s.store_id = ls.store_id
         LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
         LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
         LEFT JOIN (
           SELECT check_out_id, COUNT(*)::int AS image_count
           FROM fleet_check_out_images
           WHERE deleted_at IS NULL AND NULLIF(image_path, '') IS NOT NULL
           GROUP BY check_out_id
         ) coi ON coi.check_out_id = co.check_out_id
         LEFT JOIN (
           SELECT list_id,
                  COUNT(*)::int AS problem_count,
                  COUNT(NULLIF(image_problem, ''))::int AS problem_image_count
           FROM fleet_problems
           WHERE deleted_at IS NULL
           GROUP BY list_id
         ) p ON p.list_id = ls.list_id
         LEFT JOIN (
           SELECT p.list_id, COUNT(*)::int AS problem_image_count
           FROM fleet_problem_images pi
           JOIN fleet_problems p ON p.problem_id = pi.problem_id AND p.deleted_at IS NULL
           WHERE pi.deleted_at IS NULL AND NULLIF(pi.image_path, '') IS NOT NULL
           GROUP BY p.list_id
         ) pimg ON pimg.list_id = ls.list_id
         WHERE ls.group_store_id = $1 AND ls.deleted_at IS NULL
         ORDER BY ls.sequence_no`, [trip.group_store_id])

      return res.json({ success: true, data: { ...trip, stops: stopsResult.rows } })
    }

    const stopsResult = await crmDB.query(
      `SELECT ls.list_id, ls.sequence_no, ls.store_id, ls.store_name_result,
              ls.off_site, ls.bypass, ls.data_store_no,
              s.store_name, s.address, s.zone,
              ci.check_in_id, ci.image_check_in, ci.latitude, ci.longitude,
              ci.date_time_check_in, co.date_time_check_out,
              co.check_out_id, co.amount, co.cash, co.transfer, co.transfer_according, co.image_bill,
              co.payment_id, co.visit_customer, co.visit, co.visit_note
       FROM fleet_list_stores ls
       LEFT JOIN fleet_stores s ON s.store_id = ls.store_id
       LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id
       LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id
       WHERE ls.group_store_id = $1 AND ls.deleted_at IS NULL
       ORDER BY ls.sequence_no`, [trip.group_store_id])

    const listIds = stopsResult.rows.map(r => r.list_id).filter(Boolean)
    const checkOutIds = stopsResult.rows.map(r => r.check_out_id).filter(Boolean)
    let checkOutImagesById = {}
    if (checkOutIds.length) {
      const checkOutImagesResult = await crmDB.query(
        `SELECT image_check_out_id, check_out_id, image_path, note, created_at
         FROM fleet_check_out_images
         WHERE deleted_at IS NULL AND check_out_id = ANY($1)
         ORDER BY created_at NULLS LAST, image_check_out_id`,
        [checkOutIds]
      )
      checkOutImagesById = checkOutImagesResult.rows.reduce((acc, image) => {
        if (!acc[image.check_out_id]) acc[image.check_out_id] = []
        acc[image.check_out_id].push(image)
        return acc
      }, {})
    }

    let problemsByList = {}
    let problemImagesById = {}
    if (listIds.length) {
      const problemsResult = await crmDB.query(
        `SELECT problem_id, list_id, problem_type, description, image_problem, is_resolved,
                normal_bill, normal_bill_note, edit_bill, edit_bill_note,
                product_swap, product_swap_note, out_of_stock, out_of_stock_note,
                overstock, overstock_note, created_at
         FROM fleet_problems
         WHERE deleted_at IS NULL AND list_id = ANY($1)
         ORDER BY created_at DESC`,
        [listIds]
      )
      problemsByList = problemsResult.rows.reduce((acc, p) => {
        if (!acc[p.list_id]) acc[p.list_id] = []
        acc[p.list_id].push(p)
        return acc
      }, {})

      const problemIds = problemsResult.rows.map(problem => problem.problem_id).filter(Boolean)
      if (problemIds.length) {
        await ensureFleetProblemImagesTable()
        const problemImagesResult = await crmDB.query(
          `SELECT image_problem_id, problem_id, image_path, note, created_at
           FROM fleet_problem_images
           WHERE deleted_at IS NULL AND problem_id = ANY($1)
           ORDER BY created_at NULLS LAST, image_problem_id`,
          [problemIds]
        )
        problemImagesById = problemImagesResult.rows.reduce((acc, image) => {
          if (!acc[image.problem_id]) acc[image.problem_id] = []
          acc[image.problem_id].push(image)
          return acc
        }, {})
      }
    }

    const stops = stopsResult.rows.map(stop => ({
      ...stop,
      check_in_image: stop.image_check_in,
      check_out_image: stop.image_bill,
      check_out_images: checkOutImagesById[stop.check_out_id] || [],
      problems: (problemsByList[stop.list_id] || []).map(problem => ({
        ...problem,
        problem_image: problem.image_problem,
        problem_images: problemImagesById[problem.problem_id] || [],
      })),
    }))

    res.json({ success: true, data: { ...trip, stops } })
  } catch (err) {
    console.error('[fleet/trips/:id]', err.message)
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// GET /api/fleet/sync/status — สถานะ sync ล่าสุดของแต่ละ sheet
// ---------------------------------------------------------------------------
router.get('/sync/status', managerOnly, async (req, res) => {
  try {
    const result = await crmDB.query(`
      SELECT DISTINCT ON (sheet_name)
        sheet_name, status, rows_synced, rows_failed, duration_ms, error_message, started_at, finished_at
      FROM fleet_sync_logs
      ORDER BY sheet_name, started_at DESC`)
    res.json({ success: true, data: result.rows })
  } catch (err) {
    res.status(500).json({ success: false, error: err.message })
  }
})

// ---------------------------------------------------------------------------
// POST /api/fleet/sync/trigger — manual sync (admin only)
// ---------------------------------------------------------------------------
router.post('/sync/trigger', adminOnly, async (req, res) => {
  const { sheet } = req.body
  try {
    if (sheet) {
      const allowedSheets = SYNCS.map(s => s.name)
      if (!allowedSheets.includes(sheet)) {
        return res.status(400).json({
          success: false,
          error: `ไม่พบ sheet ที่รองรับ: ${sheet}`,
          allowed_sheets: allowedSheets,
        })
      }
      const { synced, failed } = await syncSingleSheet(sheet)
      res.json({ success: true, message: `sync ${sheet} เสร็จแล้ว`, synced, failed })
    } else {
      syncAllFleetSheets().catch(e => console.error('[fleet/sync/trigger]', e.message))
      res.json({ success: true, message: 'เริ่ม sync ทุก sheet แล้ว (background)' })
    }
  } catch (err) {
    res.status(500).json({ success: false, error: err.message })
  }
})

module.exports = router
