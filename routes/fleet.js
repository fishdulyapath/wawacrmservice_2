'use strict'

const express = require('express')
const router = express.Router()
const path = require('path')
const fs = require('fs')
const { google } = require('googleapis')
const { authMiddleware, requireRole } = require('../middleware/auth')
const { crmDB } = require('../db')
const { syncAllFleetSheets, syncSingleSheet, SYNCS } = require('../services/fleetSync')

const managerRoles = ['admin', 'manager']
const managerOnly = [authMiddleware, requireRole(...managerRoles)]
const adminOnly   = [authMiddleware, requireRole('admin')]
const DRIVE_ROOT_FOLDER_ID = process.env.GOOGLE_DRIVE_APP_FOLDER_ID
  || process.env.GOOGLE_DRIVE_ROOT_FOLDER_ID
  || process.env.APPSHEET_DRIVE_FOLDER_ID

let driveClient = null
const driveFileCache = new Map()

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
// GET /api/fleet/dashboard/top-cars?limit=10
// ---------------------------------------------------------------------------
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
    const { from, to, q, driver, car, limit = 20, offset = 0 } = req.query
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
// GET /api/fleet/trips/:id
// ---------------------------------------------------------------------------
router.get('/trips/:id', managerOnly, async (req, res) => {
  try {
    const { id } = req.params
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
    let problemsByList = {}
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
    }

    const stops = stopsResult.rows.map(stop => ({
      ...stop,
      check_in_image: stop.image_check_in,
      check_out_image: stop.image_bill,
      problems: (problemsByList[stop.list_id] || []).map(problem => ({
        ...problem,
        problem_image: problem.image_problem,
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
