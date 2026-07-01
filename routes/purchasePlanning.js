const express = require('express')
const crypto = require('crypto')
const path = require('path')
const multer = require('multer')
const XLSX = require('xlsx')
const { posDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

const router = express.Router()

router.use(authMiddleware, requireRole('admin'))

const reportJobs = new Map()
const REPORT_JOB_TTL_MS = 10 * 60 * 1000
const MIN_SLOW_MOVER_D_AVG = 0.1
const MIN_DISPLAY_QTY = 1

function clean(value) {
  return String(value || '').trim()
}

function linkableItemWhere(itemAlias = 'i', detailAlias = 'd') {
  return `COALESCE(${itemAlias}.code, '') <> ''
    AND COALESCE(${itemAlias}.item_type, 0) NOT IN (1, 3)
    AND COALESCE(${detailAlias}.is_hold_purchase, 0) <> 1`
}

function activeSupplierWhere(alias = 's') {
  return `COALESCE(${alias}.code, '') <> ''
    AND COALESCE(${alias}.status, 0) = 0`
}

function enabledPlanningSupplierWhere(alias = 's') {
  return `EXISTS (
      SELECT 1
      FROM purchase_planning_supplier_setting supplier_plan
      WHERE supplier_plan.ap_code = ${alias}.code
        AND COALESCE(supplier_plan.planning_enabled, 0) = 1
    )`
}

function toIntOrNull(value) {
  if (value === '' || value === null || value === undefined) return null
  const num = Number(value)
  if (!Number.isFinite(num)) return null
  return Math.max(0, Math.trunc(num))
}

function toNumber(value, fallback = 0) {
  const num = Number(value)
  return Number.isFinite(num) ? num : fallback
}

function boolQuery(value) {
  const text = String(value || '').trim().toLowerCase()
  return text === '1' || text === 'true' || text === 'yes'
}

function optionalDate(value) {
  const text = clean(value)
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null
}

function pageParams(req) {
  const page = Math.max(1, parseInt(req.query.page, 10) || 1)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 30))
  return { page, limit, offset: (page - 1) * limit }
}

function takeParams(source, names) {
  return names.reduce((acc, name) => {
    if (source[name] !== undefined && source[name] !== null && source[name] !== '') acc[name] = source[name]
    return acc
  }, {})
}

function reportJobKey(userCode, query) {
  const payload = {
    userCode: clean(userCode),
    query: takeParams(query, ['search', 'supplier_search', 'ap_search', 'days', 'as_of_date', 'warehouse', 'group_main', 'ap_code', 'alert_only']),
  }
  return crypto.createHash('sha1').update(JSON.stringify(payload)).digest('hex')
}

function pruneReportJobs() {
  const now = Date.now()
  for (const [id, job] of reportJobs.entries()) {
    if (now - job.updatedAt > REPORT_JOB_TTL_MS) reportJobs.delete(id)
  }
}

function searchClause(search, fields, startIndex = 1) {
  const parts = clean(search).split(/\s+/).filter(Boolean)
  if (!parts.length) return { sql: '', params: [] }
  const params = []
  const clauses = parts.map((part, idx) => {
    params.push(`%${part}%`)
    const p = `$${startIndex + idx}`
    return `(${fields.map((field) => `COALESCE(${field}, '') ILIKE ${p}::text`).join(' OR ')})`
  })
  return { sql: ` AND ${clauses.join(' AND ')}`, params }
}

// ค้นหาสินค้าแบบ multi-keyword (AND ระหว่าง keyword) ครบทุก field:
// รหัสสินค้า (i.code), ชื่อไทย (i.name_1), ชื่ออังกฤษ (i.name_eng_1), บาร์โค้ด (ผ่าน EXISTS)
// ใช้กับ query ที่มี ic_inventory เป็น alias "i" อยู่แล้ว
function itemSearchClause(search, startIndex = 1) {
  const parts = clean(search).split(/\s+/).filter(Boolean)
  if (!parts.length) return { sql: '', params: [] }
  const params = []
  const clauses = parts.map((part) => {
    params.push(`%${part}%`)
    const p = `$${startIndex + params.length - 1}`
    return `(i.code ILIKE ${p}::text
      OR COALESCE(i.name_1, '') ILIKE ${p}::text
      OR COALESCE(i.name_eng_1, '') ILIKE ${p}::text
      OR EXISTS (
        SELECT 1 FROM ic_inventory_barcode b
        WHERE b.ic_code = i.code AND b.barcode ILIKE ${p}::text
      ))`
  })
  return { sql: ` AND ${clauses.join(' AND ')}`, params }
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

// ── Excel export/import helpers ─────────────────────────────────────────────
// multer: เก็บไฟล์ใน memory (ไฟล์ Excel มักเล็ก < 1MB)
const importUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB
  fileFilter(_req, file, cb) {
    const ok = /\.(xlsx|xls)$/i.test(path.extname(file.originalname))
    cb(ok ? null : new Error('ไฟล์ต้องเป็น .xlsx หรือ .xls เท่านั้น'), ok)
  },
})

// จำกัดจำนวน row ที่ import ได้ต่อครั้ง (ป้องกัน DoS / ความผิดพลาด)
const IMPORT_MAX_ROWS = 50000

// คอลัมน์ที่ export/import ของแต่ละ tab (key = ฟิลด์ DB, header = ชื่อใน Excel)
const SUPPLIER_COLUMNS = [
  { key: 'ap_code', header: 'รหัสเจ้าหนี้', type: 'text', readOnly: true },
  { key: 'ap_name', header: 'ชื่อเจ้าหนี้', type: 'text', readOnly: true },
  { key: 'lead_time_days', header: 'Lead', type: 'int' },
  { key: 'late_buffer_days', header: 'Late', type: 'int' },
  { key: 'wholesale_buffer_days', header: 'Wholesale', type: 'int' },
  { key: 'order_cycle_days', header: 'Cycle', type: 'int' },
  { key: 'planning_enabled', header: 'ใช้', type: 'bool' },
  { key: 'remark', header: 'หมายเหตุ', type: 'text' },
]

const ITEM_SUPPLIER_COLUMNS = [
  { key: 'ic_code', header: 'รหัสสินค้า', type: 'text', readOnly: true },
  { key: 'ic_name', header: 'ชื่อสินค้า', type: 'text', readOnly: true },
  { key: 'ap_code', header: 'รหัสเจ้าหนี้', type: 'text', readOnly: true },
  { key: 'ap_name', header: 'ชื่อเจ้าหนี้', type: 'text', readOnly: true },
  { key: 'lead_time_days', header: 'Lead', type: 'int' },
  { key: 'late_buffer_days', header: 'Late', type: 'int' },
  { key: 'wholesale_buffer_days', header: 'Wholesale', type: 'int' },
  { key: 'order_cycle_days', header: 'Cycle', type: 'int' },
  { key: 'min_order_qty', header: 'MOQ', type: 'num' },
  { key: 'is_preferred', header: 'หลัก', type: 'bool' },
  { key: 'planning_enabled', header: 'ใช้', type: 'bool' },
  { key: 'remark', header: 'หมายเหตุ', type: 'text' },
]

function buildExportSheet(rows, columns) {
  return rows.map((row) => {
    const out = {}
    for (const col of columns) {
      let value = row[col.key]
      if (col.type === 'bool') value = Number(value) === 1 ? 1 : 0
      else if (col.type === 'int') value = value === null || value === undefined ? '' : Number(value)
      else if (col.type === 'num') value = value === null || value === undefined ? '' : Number(value)
      out[col.header] = value
    }
    return out
  })
}

// แปลงค่าจาก Excel cell ให้เป็นค่าที่จะเก็บใน DB ตาม type
function parseCellValue(rawValue, type) {
  const text = String(rawValue ?? '').trim()
  if (type === 'int') {
    if (text === '') return null
    const num = Number(text)
    return Number.isFinite(num) ? Math.max(0, Math.trunc(num)) : null
  }
  if (type === 'num') {
    if (text === '') return 0
    const num = Number(text)
    return Number.isFinite(num) ? Math.max(0, num) : 0
  }
  if (type === 'bool') {
    const lower = text.toLowerCase()
    // หลัก: 0 = ไม่, 1 = ใช่
    // รองรับค่าอื่นเผื่อไฟล์เก่า: ใช่/yes/true → 1, อื่นๆ → 0
    if (lower === '1' || lower === 'ใช่' || lower === 'yes' || lower === 'true') return 1
    return 0
  }
  // text
  return text.slice(0, 255)
}

// parse เซลล์ตัวเลขที่ Excel อาจเก็บเป็น string (เช่น "00123")
function parseCellText(rawValue) {
  return String(rawValue ?? '').trim()
}

function parseImportRows(buffer, columns) {
  const workbook = XLSX.read(buffer, { type: 'buffer' })
  const sheetName = workbook.SheetNames[0]
  if (!sheetName) throw new Error('ไม่พบ sheet ในไฟล์ Excel')
  const sheet = workbook.Sheets[sheetName]
  // header: ใช้ชื่อ header ของเรา ตามตัวอย่างที่ export ออกไป
  const json = XLSX.utils.sheet_to_json(sheet, { defval: '', raw: false })

  const rows = []
  const errors = []
  json.forEach((rawRow, idx) => {
    const lineNo = idx + 2 // +1 header, +1 1-based
    const parsed = {}
    for (const col of columns) {
      const cellValue = rawRow[col.header]
      if (col.type === 'bool') parsed[col.key] = parseCellValue(cellValue, 'bool')
      else if (col.type === 'int') parsed[col.key] = parseCellValue(cellValue, 'int')
      else if (col.type === 'num') parsed[col.key] = parseCellValue(cellValue, 'num')
      else parsed[col.key] = parseCellText(cellValue)
    }
    rows.push({ lineNo, parsed })
  })

  return { rows, errors }
}


function supplierHistoryCte({ fromDate, toDate, startIndex = 1 }) {
  const params = []
  const whereParts = [
    'd.trans_flag = 310',
    'COALESCE(d.status, 0) = 0',
    "COALESCE(d.cust_code, '') <> ''",
  ]
  const addParam = (value) => {
    params.push(value)
    return `$${startIndex + params.length - 1}`
  }

  if (fromDate) whereParts.push(`d.doc_date::date >= ${addParam(fromDate)}::date`)
  if (toDate) whereParts.push(`d.doc_date::date <= ${addParam(toDate)}::date`)

  return {
    params,
    sql: `
      WITH purchase_suppliers AS (
        SELECT
          d.cust_code AS ap_code,
          MAX(d.doc_date)::date AS latest_purchase_date,
          COUNT(*)::int AS purchase_line_count
        FROM ic_trans_detail d
        JOIN ap_supplier s ON s.code = d.cust_code
        WHERE ${whereParts.join(' AND ')}
          AND ${activeSupplierWhere('s')}
        GROUP BY d.cust_code
      ),
      missing_suppliers AS (
        SELECT
          p.ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          p.latest_purchase_date,
          p.purchase_line_count
        FROM purchase_suppliers p
        JOIN ap_supplier s ON s.code = p.ap_code
        WHERE NOT EXISTS (
          SELECT 1
          FROM purchase_planning_supplier_setting setting
          WHERE setting.ap_code = p.ap_code
        )
      )
    `,
  }
}

function itemSupplierHistoryCte({ fromDate, toDate, startIndex = 1 }) {
  const params = []
  const whereParts = [
    'd.trans_flag = 310',
    'COALESCE(d.status, 0) = 0',
    "COALESCE(d.item_code, '') <> ''",
    "COALESCE(d.cust_code, '') <> ''",
  ]
  const addParam = (value) => {
    params.push(value)
    return `$${startIndex + params.length - 1}`
  }

  if (fromDate) whereParts.push(`d.doc_date::date >= ${addParam(fromDate)}::date`)
  if (toDate) whereParts.push(`d.doc_date::date <= ${addParam(toDate)}::date`)

  return {
    params,
    sql: `
      WITH purchase_pairs AS (
        SELECT
          d.item_code AS ic_code,
          d.cust_code AS ap_code,
          MAX(d.doc_date)::date AS latest_purchase_date,
          COUNT(*)::int AS purchase_line_count
        FROM ic_trans_detail d
        JOIN ic_inventory i ON i.code = d.item_code
        LEFT JOIN ic_inventory_detail invd ON invd.ic_code = i.code
        JOIN ap_supplier s ON s.code = d.cust_code
        JOIN purchase_planning_supplier_setting supplier_plan
          ON supplier_plan.ap_code = d.cust_code
         AND COALESCE(supplier_plan.planning_enabled, 0) = 1
        WHERE ${whereParts.join(' AND ')}
          AND ${activeSupplierWhere('s')}
          AND ${linkableItemWhere('i', 'invd')}
        GROUP BY d.item_code, d.cust_code
      ),
      existing_line AS (
        SELECT ic_code, COALESCE(MAX(line_number), 0)::int AS max_line_number
        FROM ap_item_by_supplier
        GROUP BY ic_code
      ),
      missing_pairs AS (
        SELECT
          p.ic_code,
          COALESCE(i.name_1, '') AS ic_name,
          p.ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          p.latest_purchase_date,
          p.purchase_line_count,
          COALESCE(e.max_line_number, 0)::int AS max_line_number
        FROM purchase_pairs p
        JOIN ic_inventory i ON i.code = p.ic_code
        JOIN ap_supplier s ON s.code = p.ap_code
        LEFT JOIN existing_line e ON e.ic_code = p.ic_code
        WHERE NOT EXISTS (
          SELECT 1
          FROM ap_item_by_supplier link
          WHERE link.ic_code = p.ic_code
            AND link.ap_code = p.ap_code
        )
      )
    `,
  }
}

function planningOptions(req) {
  const today = new Date().toISOString().slice(0, 10)
  const asOfDate = optionalDate(req.query.as_of_date) || today
  const warehouse = clean(req.query.warehouse || 'MMA01') || 'MMA01'
  const days = Math.min(365, Math.max(1, parseInt(req.query.days, 10) || 30))
  return { asOfDate, warehouse, days }
}

function planningWhere(req, startIndex = 1) {
  const params = []
  const whereParts = [
    "COALESCE(i.code, '') <> ''",
    'COALESCE(i.item_type, 0) NOT IN (1, 3)',
    'COALESCE(d.is_hold_sale, 0) <> 1',
    'COALESCE(d.is_hold_purchase, 0) <> 1',
    `EXISTS (
      SELECT 1
      FROM purchase_planning_item_supplier_setting pss
      WHERE pss.ic_code = i.code
        AND pss.planning_enabled = 1
    )`,
  ]

  const addParam = (value) => {
    params.push(value)
    return `$${startIndex + params.length - 1}`
  }

  const search = clean(req.query.search)
  for (const keyword of search.split(/\s+/).filter(Boolean)) {
    const p = addParam(`%${keyword}%`)
    whereParts.push(`(
      i.code ILIKE ${p}::text
      OR COALESCE(i.name_1, '') ILIKE ${p}::text
      OR COALESCE(i.name_eng_1, '') ILIKE ${p}::text
      OR EXISTS (
        SELECT 1
        FROM ic_inventory_barcode b
        WHERE b.ic_code = i.code
          AND b.barcode ILIKE ${p}::text
      )
    )`)
  }

  const groupMain = clean(req.query.group_main)
  if (groupMain) whereParts.push(`COALESCE(i.group_main, '') = ${addParam(groupMain)}::text`)

  const apCode = clean(req.query.ap_code)
  if (apCode) {
    const p = addParam(apCode)
    whereParts.push(`EXISTS (
      SELECT 1
      FROM ap_item_by_supplier link
      WHERE link.ic_code = i.code
        AND link.ap_code = ${p}::text
    )`)
  }

  const supplierSearch = clean(req.query.supplier_search || req.query.ap_search)
  for (const keyword of supplierSearch.split(/\s+/).filter(Boolean)) {
    const p = addParam(`%${keyword}%`)
    whereParts.push(`EXISTS (
      SELECT 1
      FROM ap_item_by_supplier link
      JOIN ap_supplier s ON s.code = link.ap_code
      JOIN purchase_planning_item_supplier_resolved r
        ON r.ic_code = link.ic_code AND r.ap_code = link.ap_code
      WHERE link.ic_code = i.code
        AND COALESCE(r.planning_enabled, 0) = 1
        AND ${activeSupplierWhere('s')}
        AND ${enabledPlanningSupplierWhere('s')}
        AND (
          link.ap_code ILIKE ${p}::text
          OR COALESCE(s.name_1, '') ILIKE ${p}::text
        )
    )`)
  }

  const icCodes = Array.isArray(req.query.ic_codes)
    ? req.query.ic_codes.map(clean).filter(Boolean)
    : []
  if (icCodes.length) whereParts.push(`i.code = ANY(${addParam(icCodes)}::text[])`)

  return { sql: `WHERE ${whereParts.join(' AND ')}`, params }
}

// เช็คว่าสินค้ามีจริงในระบบไหม (แยกจาก "ไม่ผ่านเงื่อนไขวางแผน")
// คืนข้อความ error ที่ชัดเจน หรือ null ถ้าสินค้าผ่านเงื่อนไข (เรียก metric query แล้วเจอ)
async function resolveItemMissingError(icCode) {
  if (!icCode) return 'กรุณาระบุรหัสสินค้า'
  const result = await posDB.query(
    `SELECT
       i.code,
       COALESCE(i.item_type, 0) AS item_type,
       COALESCE(d.is_hold_sale, 0) AS is_hold_sale,
       COALESCE(d.is_hold_purchase, 0) AS is_hold_purchase,
       EXISTS (
         SELECT 1
         FROM ap_item_by_supplier link
         JOIN purchase_planning_item_supplier_resolved r
           ON r.ic_code = link.ic_code AND r.ap_code = link.ap_code
         WHERE link.ic_code = i.code
           AND COALESCE(r.planning_enabled, 0) = 1
       ) AS has_enabled_supplier
     FROM ic_inventory i
     LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
     WHERE i.code = $1::text`,
    [icCode],
  )
  if (!result.rows.length) return 'ไม่พบสินค้าในระบบ'
  const row = result.rows[0]
  const reasons = []
  if ([1, 3].includes(Number(row.item_type))) reasons.push('ประเภทสินค้าไม่รองรับการวางแผน')
  if (Number(row.is_hold_sale) === 1) reasons.push('สินค้าถูก hold sale')
  if (Number(row.is_hold_purchase) === 1) reasons.push('สินค้าถูก hold purchase')
  if (!row.has_enabled_supplier) reasons.push('ยังไม่มีเจ้าหนี้ที่เปิดใช้งานในการวางแผน (planning_enabled = 1)')
  if (!reasons.length) return 'สินค้าไม่ผ่านเงื่อนไขการวางแผนสั่งซื้อ'
  return `สินค้า "${icCode}" ไม่สามารถแสดงในรายงานวางแผนได้: ${reasons.join(', ')} — กรุณาตรวจสอบที่หน้า "กำหนดข้อมูลวางแผนสั่งซื้อ"`
}

function buildPlanningMetricsSql({
  whereSql,
  startIndex = 1,
  includePagination = true,
  singleItem = false,
  useDailyStock = true,
  paginationMode = 'candidate',
}) {
  const offsetParam = `$${startIndex}`
  const limitParam = `$${startIndex + 1}`
  const itemParam = `$${startIndex}`
  const finalPagination = includePagination && paginationMode === 'final'
    ? `OFFSET ${offsetParam} LIMIT ${limitParam}`
    : ''
  const candidateLimit = includePagination && paginationMode !== 'final'
    ? `ORDER BY i.code OFFSET ${offsetParam} LIMIT ${limitParam}`
    : singleItem
      ? `AND i.code = ${itemParam}::text ORDER BY i.code`
      : 'ORDER BY i.code'
  const stockDaysSql = useDailyStock
    ? `
    stock_days AS (
      SELECT
        ds.sales_date,
        sb.ic_code,
        sb.warehouse,
        SUM(COALESCE(sb.balance_qty, 0)) AS stock_qty
      FROM date_series ds
      CROSS JOIN item_code_list icl
      CROSS JOIN LATERAL sml_ic_function_stock_balance_warehouse_location(
        ds.sales_date::date,
        icl.codes::varchar,
        $2::varchar,
        ''::varchar
      ) sb
      GROUP BY ds.sales_date, sb.ic_code, sb.warehouse
    ),`
    : ''
  const davgSql = useDailyStock
    ? `
    davg_base AS (
      SELECT
        c.ic_code,
        COUNT(*) FILTER (WHERE COALESCE(sd.stock_qty, 0) > 0)::int AS active_stock_days,
        COUNT(*) FILTER (WHERE COALESCE(sd.stock_qty, 0) <= 0)::int AS stockout_days,
        COUNT(*) FILTER (WHERE COALESCE(sd.stock_qty, 0) > 0 AND COALESCE(sales.sales_qty, 0) > 0)::int AS sales_days,
        COALESCE(
          SUM(COALESCE(sales.sales_qty, 0)) FILTER (WHERE COALESCE(sd.stock_qty, 0) > 0),
          0
        ) AS total_sales_qty,
        COALESCE(
          (percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(sales.sales_qty, 0))
            FILTER (WHERE COALESCE(sd.stock_qty, 0) > 0))::numeric,
          0
        ) AS median_d_avg
      FROM candidates c
      CROSS JOIN date_series ds
      LEFT JOIN stock_days sd
        ON sd.ic_code = c.ic_code AND sd.sales_date = ds.sales_date AND sd.warehouse = $2::text
      LEFT JOIN sales_daily sales
        ON sales.ic_code = c.ic_code AND sales.sales_date = ds.sales_date
      GROUP BY c.ic_code
    ),
    davg AS (
      SELECT
        *,
        COALESCE(total_sales_qty / NULLIF(active_stock_days::numeric, 0), 0) AS mean_d_avg,
        COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) AS sales_frequency,
        CASE
          WHEN active_stock_days <= 0 OR sales_days <= 0 THEN 0
          WHEN COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) >= 0.60 THEN median_d_avg
          ELSE GREATEST(COALESCE(total_sales_qty / NULLIF(active_stock_days::numeric, 0), 0), ${MIN_SLOW_MOVER_D_AVG})
        END AS d_avg,
        CASE
          WHEN active_stock_days <= 0 OR sales_days <= 0 THEN 'none'
          WHEN COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) >= 0.60 THEN 'median'
          ELSE 'mean'
        END AS d_avg_method
      FROM davg_base
    )`
    : `
    davg_base AS (
      SELECT
        c.ic_code,
        $3::int AS active_stock_days,
        0::int AS stockout_days,
        COUNT(*) FILTER (WHERE COALESCE(sales.sales_qty, 0) > 0)::int AS sales_days,
        COALESCE(SUM(COALESCE(sales.sales_qty, 0)), 0) AS total_sales_qty,
        COALESCE(
          (percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(sales.sales_qty, 0)))::numeric,
          0
        ) AS median_d_avg
      FROM candidates c
      CROSS JOIN date_series ds
      LEFT JOIN sales_daily sales
        ON sales.ic_code = c.ic_code AND sales.sales_date = ds.sales_date
      GROUP BY c.ic_code
    ),
    davg AS (
      SELECT
        *,
        COALESCE(total_sales_qty / NULLIF(active_stock_days::numeric, 0), 0) AS mean_d_avg,
        COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) AS sales_frequency,
        CASE
          WHEN active_stock_days <= 0 OR sales_days <= 0 THEN 0
          WHEN COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) >= 0.60 THEN median_d_avg
          ELSE GREATEST(COALESCE(total_sales_qty / NULLIF(active_stock_days::numeric, 0), 0), ${MIN_SLOW_MOVER_D_AVG})
        END AS d_avg,
        CASE
          WHEN active_stock_days <= 0 OR sales_days <= 0 THEN 'none'
          WHEN COALESCE(sales_days::numeric / NULLIF(active_stock_days::numeric, 0), 0) >= 0.60 THEN 'median'
          ELSE 'mean'
        END AS d_avg_method
      FROM davg_base
    )`

  return `
    WITH candidates AS (
      SELECT
        i.code AS ic_code,
        COALESCE(i.name_1, '') AS ic_name,
        COALESCE(i.name_eng_1, '') AS ic_name_eng,
        COALESCE(i.unit_standard, '') AS unit_code,
        COALESCE(i.group_main, '') AS group_main,
        COALESCE(i.supplier_code, '') AS legacy_supplier_code,
        i.last_movement_date,
        COALESCE(d.is_hold_sale, 0) AS is_hold_sale,
        COALESCE(d.is_hold_purchase, 0) AS is_hold_purchase
      FROM ic_inventory i
      LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
      ${whereSql}
      ${candidateLimit}
    ),
    item_code_list AS (
      SELECT COALESCE(string_agg(ic_code, ',' ORDER BY ic_code), '') AS codes
      FROM candidates
    ),
    date_series AS (
      SELECT generate_series(
        ($1::date - (($3::int - 1) || ' day')::interval)::date,
        $1::date,
        '1 day'::interval
      )::date AS sales_date
    ),
    stock_balance AS (
      SELECT
        sb.ic_code,
        sb.warehouse,
        SUM(COALESCE(sb.balance_qty, 0)) AS balance_qty,
        MAX(COALESCE(sb.average_cost_end, 0)) AS average_cost
      FROM item_code_list icl
      CROSS JOIN LATERAL sml_ic_function_stock_balance_warehouse_location(
        $1::date,
        icl.codes::varchar,
        $2::varchar,
        ''::varchar
      ) sb
      GROUP BY sb.ic_code, sb.warehouse
    ),
    ${stockDaysSql}
    sales_daily AS (
      SELECT
        td.item_code AS ic_code,
        td.doc_date::date AS sales_date,
        SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS sales_qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag = 44
        AND COALESCE(td.status, 0) = 0
        AND td.doc_date::date BETWEEN ($1::date - (($3::int - 1) || ' day')::interval)::date AND $1::date
      GROUP BY td.item_code, td.doc_date::date
    ),
    ${davgSql},
    reserve_add AS (
      SELECT td.doc_no, td.item_code AS ic_code,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag = 34
        AND COALESCE(td.last_status, 0) = 0
      GROUP BY td.doc_no, td.item_code
    ),
    reserve_reduce AS (
      SELECT td.ref_doc_no AS doc_no, td.item_code AS ic_code,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE (
          (td.trans_flag = 44 AND COALESCE(td.doc_ref_type, 0) = 2)
          OR (td.trans_flag = 36 AND COALESCE(td.doc_ref_type, 0) = 2)
          OR (
            td.trans_flag = 39
            AND EXISTS (
              SELECT 1
              FROM ic_trans t_cancel
              WHERE t_cancel.doc_no = td.doc_no
                AND t_cancel.trans_flag = td.trans_flag
                AND COALESCE(t_cancel.cancel_type, 0) = 2
            )
          )
        )
        AND COALESCE(td.last_status, 0) = 0
        AND COALESCE(td.ref_doc_no, '') <> ''
      GROUP BY td.ref_doc_no, td.item_code
    ),
    book_out AS (
      SELECT a.ic_code, SUM(a.qty - COALESCE(r.qty, 0)) FILTER (WHERE a.qty - COALESCE(r.qty, 0) <> 0) AS book_out_qty
      FROM reserve_add a
      LEFT JOIN reserve_reduce r
        ON r.doc_no = a.doc_no
       AND r.ic_code = a.ic_code
      GROUP BY a.ic_code
    ),
    sale_order_add AS (
      SELECT td.doc_no, td.item_code AS ic_code,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag = 36
        AND COALESCE(td.last_status, 0) = 0
      GROUP BY td.doc_no, td.item_code
    ),
    sale_order_reduce AS (
      SELECT td.ref_doc_no AS doc_no, td.item_code AS ic_code,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE (
          (td.trans_flag = 44 AND COALESCE(td.doc_ref_type, 0) = 3)
          OR (
            td.trans_flag = 37
            AND EXISTS (
              SELECT 1
              FROM ic_trans t_cancel
              WHERE t_cancel.doc_no = td.doc_no
                AND t_cancel.trans_flag = td.trans_flag
                AND COALESCE(t_cancel.cancel_type, 0) = 2
            )
          )
        )
        AND COALESCE(td.last_status, 0) = 0
        AND COALESCE(td.ref_doc_no, '') <> ''
      GROUP BY td.ref_doc_no, td.item_code
    ),
    accrued_out AS (
      SELECT a.ic_code, SUM(a.qty - COALESCE(r.qty, 0)) FILTER (WHERE a.qty - COALESCE(r.qty, 0) <> 0) AS accrued_out_qty
      FROM sale_order_add a
      LEFT JOIN sale_order_reduce r
        ON r.doc_no = a.doc_no
       AND r.ic_code = a.ic_code
      GROUP BY a.ic_code
    ),
    po_lines AS (
      SELECT
        td.doc_no,
        td.item_code AS ic_code,
        SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag = 6
        AND COALESCE(td.last_status, 0) = 0
      GROUP BY td.doc_no, td.item_code
    ),
    po_reduce AS (
      SELECT
        td.ref_doc_no AS doc_no,
        td.item_code AS ic_code,
        SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE (
          td.trans_flag IN (12, 310)
          OR (
            td.trans_flag = 7
            AND EXISTS (
              SELECT 1
              FROM ic_trans t_cancel
              WHERE t_cancel.doc_no = td.doc_no
                AND t_cancel.trans_flag = td.trans_flag
                AND COALESCE(t_cancel.cancel_type, 0) = 2
            )
          )
        )
        AND COALESCE(td.last_status, 0) = 0
        AND COALESCE(td.ref_doc_no, '') <> ''
      GROUP BY td.ref_doc_no, td.item_code
    ),
    accrued_in AS (
      SELECT
        p.ic_code,
        SUM(p.qty - COALESCE(r.qty, 0)) FILTER (WHERE p.qty - COALESCE(r.qty, 0) <> 0) AS accrued_in_qty
      FROM po_lines p
      LEFT JOIN po_reduce r
        ON r.doc_no = p.doc_no
       AND r.ic_code = p.ic_code
      GROUP BY p.ic_code
    ),
    movement_3m AS (
      SELECT
        td.item_code AS ic_code,
        SUM(CASE WHEN td.trans_flag = 310 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS purchase_qty_3m,
        SUM(CASE WHEN td.trans_flag = 44 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS sale_qty_3m,
        SUM(CASE WHEN td.trans_flag = 310 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END) AS purchase_amount_3m,
        SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END) AS sale_amount_3m,
        -- กำไรขั้นต้น 90 วัน (ตามมาตรฐาน _stkProfit: JOIN ic_trans + last_status=0, แยก ขาย44/รับคืน48)
        SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END) AS amount_sale_3m,
        SUM(CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END) AS amount_sale_return_3m,
        SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END) AS cost_sale_3m,
        SUM(CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END) AS cost_sale_return_3m,
        MAX(td.doc_date)::date AS last_doc_date
      FROM ic_trans_detail td
      JOIN ic_trans t ON t.doc_no = td.doc_no AND t.trans_flag = td.trans_flag
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag IN (310, 44, 48)
        AND COALESCE(t.last_status, 0) = 0
        AND td.doc_date::date BETWEEN ($1::date - interval '90 day')::date AND $1::date
      GROUP BY td.item_code
    ),
    latest_purchase AS (
      SELECT DISTINCT ON (td.item_code, td.cust_code)
        td.item_code AS ic_code,
        td.cust_code AS ap_code,
        td.price_exclude_vat AS price,
        td.price_exclude_vat,
        td.unit_code,
        td.doc_no,
        td.doc_date::date AS doc_date,
        td.doc_time
      FROM ic_trans_detail td
      JOIN candidates c ON c.ic_code = td.item_code
      WHERE td.trans_flag = 310
        AND COALESCE(td.status, 0) = 0
        AND COALESCE(td.item_code, '') <> ''
        AND COALESCE(td.cust_code, '') <> ''
      ORDER BY td.item_code, td.cust_code, td.doc_date DESC, td.doc_time DESC, td.doc_no DESC
    ),
    supplier_options AS (
      SELECT
        link.ic_code,
        link.ap_code,
        COALESCE(s.name_1, '') AS ap_name,
        COALESCE(sd.tax_type, '') AS tax_type,
        lp.price_exclude_vat AS last_purchase_price,
        lp.price_exclude_vat AS last_purchase_price_exclude_vat,
        COALESCE(lp.unit_code, '') AS last_purchase_unit_code,
        lp.doc_date AS last_purchase_date,
        COALESCE(r.lead_time_days, 0) AS lead_time_days,
        COALESCE(r.late_buffer_days, 0) AS late_buffer_days,
        COALESCE(r.wholesale_buffer_days, 0) AS wholesale_buffer_days,
        COALESCE(r.order_cycle_days, 0) AS order_cycle_days,
        COALESCE(r.min_order_qty, 0) AS min_order_qty,
        COALESCE(NULLIF(r.pack_size, 0), 1) AS pack_size,
        COALESCE(r.purchase_unit_code, '') AS purchase_unit_code,
        COALESCE(r.is_preferred, 0) AS is_preferred,
        COUNT(*) OVER (PARTITION BY link.ic_code)::int AS supplier_count,
        ROW_NUMBER() OVER (
          PARTITION BY link.ic_code
          ORDER BY
            CASE WHEN COALESCE(r.is_preferred, 0) = 1 THEN 0 ELSE 1 END,
            COALESCE(lp.price_exclude_vat, 999999999999),
            lp.doc_date DESC NULLS LAST,
            link.ap_code
        ) AS rn
      FROM ap_item_by_supplier link
      JOIN candidates c ON c.ic_code = link.ic_code
      LEFT JOIN ap_supplier s ON s.code = link.ap_code
      LEFT JOIN ap_supplier_detail sd ON sd.ap_code = link.ap_code AND COALESCE(sd.tax_type, '') <> ''
      LEFT JOIN purchase_planning_item_supplier_resolved r
        ON r.ic_code = link.ic_code AND r.ap_code = link.ap_code
      LEFT JOIN latest_purchase lp
        ON lp.ic_code = link.ic_code AND lp.ap_code = link.ap_code
      WHERE COALESCE(link.ic_code, '') <> ''
        AND COALESCE(link.ap_code, '') <> ''
        AND COALESCE(r.planning_enabled, 0) = 1
    ),
    chosen_supplier AS (
      SELECT *
      FROM supplier_options
      WHERE rn = 1
    ),
    calculated AS (
      SELECT
        c.*,
        COALESCE(sb.balance_qty, 0) AS balance_qty,
        COALESCE(sb.average_cost, 0) AS average_cost,
        COALESCE(ai.accrued_in_qty, 0) AS accrued_in_qty_calc,
        COALESCE(bo.book_out_qty, 0) AS book_out_qty,
        COALESCE(ao.accrued_out_qty, 0) AS accrued_out_qty_calc,
        (COALESCE(sb.balance_qty, 0) + COALESCE(ai.accrued_in_qty, 0) - COALESCE(bo.book_out_qty, 0) - COALESCE(ao.accrued_out_qty, 0)) AS available_qty,
        COALESCE(dv.d_avg, 0) AS d_avg,
        COALESCE(dv.active_stock_days, 0) AS active_stock_days,
        COALESCE(dv.stockout_days, 0) AS stockout_days,
        COALESCE(dv.sales_days, 0) AS sales_days,
        COALESCE(dv.total_sales_qty, 0) AS total_sales_qty,
        COALESCE(dv.sales_frequency, 0) AS sales_frequency,
        COALESCE(dv.median_d_avg, 0) AS median_d_avg,
        COALESCE(dv.mean_d_avg, 0) AS mean_d_avg,
        COALESCE(dv.d_avg_method, 'none') AS d_avg_method,
        COALESCE(m.purchase_qty_3m, 0) AS purchase_qty_3m,
        COALESCE(m.sale_qty_3m, 0) AS sale_qty_3m,
        COALESCE(m.purchase_amount_3m, 0) AS purchase_amount_3m,
        COALESCE(m.sale_amount_3m, 0) AS sale_amount_3m,
        -- กำไรขั้นต้น 90 วัน (ตามมาตรฐาน _stkProfit)
        COALESCE(m.amount_sale_3m, 0) AS amount_sale_3m,
        COALESCE(m.amount_sale_return_3m, 0) AS amount_sale_return_3m,
        COALESCE(m.cost_sale_3m, 0) AS cost_sale_3m,
        COALESCE(m.cost_sale_return_3m, 0) AS cost_sale_return_3m,
        -- net = ขาย - รับคืน
        (COALESCE(m.amount_sale_3m, 0) - COALESCE(m.amount_sale_return_3m, 0)) AS amount_net_3m,
        (COALESCE(m.cost_sale_3m, 0) - COALESCE(m.cost_sale_return_3m, 0)) AS cost_net_3m,
        -- profit_lost_amount = amount_net - cost_net
        ((COALESCE(m.amount_sale_3m, 0) - COALESCE(m.amount_sale_return_3m, 0))
          - (COALESCE(m.cost_sale_3m, 0) - COALESCE(m.cost_sale_return_3m, 0))) AS profit_lost_amount_3m,
        GREATEST(COALESCE(c.last_movement_date, m.last_doc_date), COALESCE(m.last_doc_date, c.last_movement_date)) AS last_doc_date,
        cs.ap_code,
        cs.ap_name,
        cs.last_purchase_price,
        cs.last_purchase_price_exclude_vat,
        cs.last_purchase_unit_code,
        cs.last_purchase_date,
        COALESCE(cs.lead_time_days, 0) AS lead_time_days,
        COALESCE(cs.late_buffer_days, 0) AS late_buffer_days,
        COALESCE(cs.wholesale_buffer_days, 0) AS wholesale_buffer_days,
        COALESCE(cs.order_cycle_days, 0) AS order_cycle_days,
        COALESCE(cs.min_order_qty, 0) AS min_order_qty,
        COALESCE(NULLIF(cs.pack_size, 0), 1) AS pack_size,
        COALESCE(cs.purchase_unit_code, '') AS purchase_unit_code,
        COALESCE(cs.is_preferred, 0) AS is_preferred,
        COALESCE(cs.supplier_count, 0) AS supplier_count,
        COALESCE(cs.tax_type, '') AS tax_type
      FROM candidates c
      LEFT JOIN stock_balance sb ON sb.ic_code = c.ic_code AND sb.warehouse = $2::text
      LEFT JOIN davg dv ON dv.ic_code = c.ic_code
      LEFT JOIN accrued_in ai ON ai.ic_code = c.ic_code
      LEFT JOIN book_out bo ON bo.ic_code = c.ic_code
      LEFT JOIN accrued_out ao ON ao.ic_code = c.ic_code
      LEFT JOIN movement_3m m ON m.ic_code = c.ic_code
      LEFT JOIN chosen_supplier cs ON cs.ic_code = c.ic_code
    ),
    final_rows AS (
      SELECT
        *,
        CASE
          WHEN d_avg > 0 THEN CEIL(GREATEST(d_avg * (lead_time_days + late_buffer_days + wholesale_buffer_days), ${MIN_DISPLAY_QTY}))
          ELSE 0
        END AS min_stock,
        CASE
          WHEN d_avg > 0 THEN CEIL(GREATEST(
            d_avg * (lead_time_days + late_buffer_days + wholesale_buffer_days + order_cycle_days),
            d_avg * (lead_time_days + late_buffer_days + wholesale_buffer_days),
            ${MIN_DISPLAY_QTY}
          ))
          ELSE 0
        END AS max_stock
      FROM calculated
    )
    SELECT
      *,
      CASE
        WHEN d_avg <= 0 AND sale_qty_3m <= 0 AND purchase_qty_3m <= 0 THEN 'inactive'
        WHEN last_doc_date IS NULL OR last_doc_date < ($1::date - interval '60 day')::date THEN 'inactive'
        WHEN active_stock_days <= 0 THEN 'insufficient_sales_days'
        WHEN available_qty > max_stock THEN 'high'
        WHEN available_qty <= min_stock THEN 'low'
        ELSE 'normal'
      END AS stock_status,
      CASE
        WHEN d_avg > 0 AND active_stock_days > 0 AND available_qty <= min_stock
          THEN CEIL(GREATEST(max_stock - available_qty, min_order_qty, 0))
        ELSE 0
      END AS suggest_qty,
      CASE WHEN d_avg > 0 THEN available_qty / d_avg ELSE NULL END AS stock_cover_days
    FROM final_rows
    ORDER BY suggest_qty DESC NULLS LAST, ic_code
  `
}

// นับใบเสนอซื้อ (PR, trans_flag=2) ที่ยังไม่ถูกดึงไปทำซื้อ (doc_success=0)
// แยกตามเจ้าหนี้ และแยกตามคู่สินค้า+เจ้าหนี้สำหรับ badge ในรายงาน
async function getPendingPRCount() {
  const result = await posDB.query(
    `SELECT DISTINCT
       t.doc_no,
       t.cust_code AS ap_code,
       d.item_code AS ic_code
     FROM ic_trans t
     JOIN ic_trans_detail d ON d.doc_no = t.doc_no AND d.trans_flag = t.trans_flag
     WHERE t.trans_flag = 2
       AND COALESCE(t.doc_success, 0) = 0
       AND COALESCE(t.last_status, 0) = 0
       AND COALESCE(d.status, 0) = 0
       AND COALESCE(t.cust_code, '') <> ''
       AND COALESCE(d.item_code, '') <> ''`,
  )
  const byAp = {}
  const byApDocs = {}
  const byItemAp = {}
  const totalDocs = new Set()
  for (const row of result.rows) {
    const apCode = clean(row.ap_code)
    const icCode = clean(row.ic_code)
    const docNo = clean(row.doc_no)
    if (!apCode || !icCode || !docNo) continue
    totalDocs.add(docNo)
    if (!byApDocs[apCode]) byApDocs[apCode] = new Set()
    byApDocs[apCode].add(docNo)
    if (!byItemAp[icCode]) byItemAp[icCode] = {}
    byItemAp[icCode][apCode] = Number(byItemAp[icCode][apCode] || 0) + 1
  }
  for (const [apCode, docs] of Object.entries(byApDocs)) {
    byAp[apCode] = docs.size
  }
  return { total: totalDocs.size, byAp, byItemAp }
}

function summaryFromRows(rows) {
  const summary = rows.reduce((acc, row) => {
    const status = row.stock_status || 'unknown'
    const apCode = clean(row.ap_code) || 'NO_SUPPLIER'
    const apName = clean(row.ap_name) || 'ไม่ระบุเจ้าหนี้'
    const suggestAmount = Number(row.suggest_qty || 0) * Number(row.last_purchase_price || 0)

    acc.total += 1
    acc[status] = (acc[status] || 0) + 1
    acc.suggest_qty += Number(row.suggest_qty || 0)
    acc.suggest_amount += suggestAmount

    // กราฟ supplier: แสดงเฉพาะ supplier ที่มีจำนวนแนะนำซื้อ > 0 (ใช้ suggest_qty แทนยอดเงิน
    // เพราะยอดเงินต้องมีราคาซื้อล่าสุด ถ้ายังไม่เคยซื้อจาก supplier นี้จะได้ 0)
    if (Number(row.suggest_qty || 0) > 0) {
      if (!acc.supplier_order_map[apCode]) {
        acc.supplier_order_map[apCode] = {
          ap_code: apCode,
          ap_name: apName,
          suggest_amount: 0,
          suggest_qty: 0,
        }
      }
      acc.supplier_order_map[apCode].suggest_amount += suggestAmount
      acc.supplier_order_map[apCode].suggest_qty += Number(row.suggest_qty || 0)
    }

    if (!acc.stock_status_amount_map[status]) {
      acc.stock_status_amount_map[status] = {
        stock_status: status,
        sale_amount_3m: 0,
        purchase_amount_3m: 0,
      }
    }
    acc.stock_status_amount_map[status].sale_amount_3m += Number(row.sale_amount_3m || 0)
    acc.stock_status_amount_map[status].purchase_amount_3m += Number(row.purchase_amount_3m || 0)

    if (Number(row.supplier_count || 0) > 0) acc.with_supplier += 1
    else acc.without_supplier += 1
    return acc
  }, {
    total: 0,
    low: 0,
    normal: 0,
    high: 0,
    inactive: 0,
    insufficient_sales_days: 0,
    suggest_qty: 0,
    suggest_amount: 0,
    with_supplier: 0,
    without_supplier: 0,
    supplier_order_map: {},
    stock_status_amount_map: {},
  })

  const statusOrder = ['high', 'normal', 'low', 'insufficient_sales_days', 'inactive', 'unknown']
  const supplierOrderChart = Object.values(summary.supplier_order_map)
    .sort((a, b) => Number(b.suggest_amount || 0) - Number(a.suggest_amount || 0))
    .slice(0, 10)
  const stockStatusAmountChart = statusOrder
    .filter((status) => summary.stock_status_amount_map[status])
    .map((status) => summary.stock_status_amount_map[status])

  delete summary.supplier_order_map
  delete summary.stock_status_amount_map
  return {
    ...summary,
    supplier_order_chart: supplierOrderChart,
    stock_status_amount_chart: stockStatusAmountChart,
  }
}

function sortPlanningRows(rows) {
  return rows.sort((a, b) => {
    const suggestDiff = Number(b.suggest_qty || 0) - Number(a.suggest_qty || 0)
    if (suggestDiff !== 0) return suggestDiff
    return String(a.ic_code || '').localeCompare(String(b.ic_code || ''))
  })
}

const PLANNING_STOCK_STATUSES = new Set(['low', 'normal', 'high', 'inactive', 'insufficient_sales_days'])

async function runReportJob(job) {
  try {
    const options = job.options
    const queryReq = { query: job.query }
    const alertOnly = boolQuery(job.query.alert_only)
    job.status = 'running'
    job.updatedAt = Date.now()

    const orderWhere = planningWhere(queryReq, 4)
    const candidateResult = await posDB.query(
      `WITH candidates AS (
         SELECT
           i.code AS ic_code
         FROM ic_inventory i
         LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
         ${orderWhere.sql}
       ),
       item_code_list AS (
         SELECT COALESCE(string_agg(ic_code, ',' ORDER BY ic_code), '') AS codes
         FROM candidates
       ),
       stock_balance AS (
         SELECT
           sb.ic_code,
           sb.warehouse,
           SUM(COALESCE(sb.balance_qty, 0)) AS balance_qty
         FROM item_code_list icl
         CROSS JOIN LATERAL sml_ic_function_stock_balance_warehouse_location(
           $1::date,
           icl.codes::varchar,
           $3::varchar,
           ''::varchar
         ) sb
         GROUP BY sb.ic_code, sb.warehouse
       ),
       po_lines AS (
         SELECT
           td.doc_no,
           td.item_code AS ic_code,
           SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
         FROM ic_trans_detail td
         JOIN candidates c ON c.ic_code = td.item_code
         WHERE td.trans_flag = 6
           AND COALESCE(td.last_status, 0) = 0
         GROUP BY td.doc_no, td.item_code
       ),
       po_reduce AS (
         SELECT
           td.ref_doc_no AS doc_no,
           td.item_code AS ic_code,
           SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
         FROM ic_trans_detail td
         JOIN candidates c ON c.ic_code = td.item_code
         WHERE (
             td.trans_flag IN (12, 310)
             OR (
               td.trans_flag = 7
               AND EXISTS (
                 SELECT 1
                 FROM ic_trans t_cancel
                 WHERE t_cancel.doc_no = td.doc_no
                   AND t_cancel.trans_flag = td.trans_flag
                   AND COALESCE(t_cancel.cancel_type, 0) = 2
               )
             )
           )
           AND COALESCE(td.last_status, 0) = 0
           AND COALESCE(td.ref_doc_no, '') <> ''
         GROUP BY td.ref_doc_no, td.item_code
       ),
       accrued_in AS (
         SELECT
           p.ic_code,
           SUM(p.qty - COALESCE(r.qty, 0)) FILTER (WHERE p.qty - COALESCE(r.qty, 0) <> 0) AS accrued_in_qty
         FROM po_lines p
         LEFT JOIN po_reduce r
           ON r.doc_no = p.doc_no
          AND r.ic_code = p.ic_code
         GROUP BY p.ic_code
       ),
       sales_sort AS (
         SELECT
           td.item_code AS ic_code,
           SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS sale_qty
         FROM ic_trans_detail td
         WHERE td.trans_flag = 44
           AND COALESCE(td.status, 0) = 0
           AND td.doc_date::date BETWEEN ($1::date - (($2::int - 1) || ' day')::interval)::date AND $1::date
         GROUP BY td.item_code
       )
       SELECT c.ic_code
       FROM candidates c
       LEFT JOIN stock_balance sb ON sb.ic_code = c.ic_code AND sb.warehouse = $3::text
       LEFT JOIN accrued_in ai ON ai.ic_code = c.ic_code
       LEFT JOIN sales_sort ss ON ss.ic_code = c.ic_code
       ORDER BY
         GREATEST(
           ((COALESCE(ss.sale_qty, 0) / NULLIF($2::numeric, 0)) * 4)
             - (COALESCE(sb.balance_qty, 0) + COALESCE(ai.accrued_in_qty, 0)),
           0
         ) DESC,
         COALESCE(ss.sale_qty, 0) DESC,
         c.ic_code`,
      [options.asOfDate, options.days, options.warehouse, ...orderWhere.params],
    )

    const codes = candidateResult.rows.map((row) => row.ic_code).filter(Boolean)
    job.total = codes.length

    for (let offset = 0; offset < codes.length; offset += job.batchSize) {
      if (job.cancelled) return
      const batchCodes = codes.slice(offset, offset + job.batchSize)
      const batchReq = { query: { ...job.query, ic_codes: batchCodes } }
      const where = planningWhere(batchReq, 4)
      const startIndex = 4 + where.params.length
      const result = await posDB.query(
        buildPlanningMetricsSql({
          whereSql: where.sql,
          startIndex,
          includePagination: false,
          useDailyStock: true,
        }),
        [options.asOfDate, options.warehouse, options.days, ...where.params],
      )
      const rows = alertOnly
        ? result.rows.filter((row) => row.stock_status === 'low')
        : result.rows
      job.rows.push(...rows)
      job.processed = Math.min(codes.length, offset + batchCodes.length)
      job.partialSummary = summaryFromRows(job.rows)
      job.updatedAt = Date.now()
    }

    sortPlanningRows(job.rows)
    job.summary = summaryFromRows(job.rows)
    try {
      job.pending_pr = await getPendingPRCount()
    } catch {
      job.pending_pr = { total: 0, byAp: {}, byItemAp: {} }
    }
    job.status = 'complete'
    job.processed = job.total
    job.updatedAt = Date.now()
  } catch (err) {
    job.status = 'failed'
    job.error = err.message
    job.updatedAt = Date.now()
  }
}

router.get('/supplier-settings', async (req, res) => {
  const { page, limit, offset } = pageParams(req)
  const search = searchClause(req.query.search, ['s.code', 's.name_1'], 1)
  const enabledOnly = boolQuery(req.query.enabled_only)
  const enabledClause = enabledOnly ? ' AND p.planning_enabled = 1' : ''
  const params = [...search.params]

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ap_supplier s
       LEFT JOIN purchase_planning_supplier_setting p ON p.ap_code = s.code
       WHERE COALESCE(s.code, '') <> ''${search.sql}${enabledClause}`,
      params,
    )

    const dataResult = await posDB.query(
      `SELECT
          s.code AS ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          COALESCE(sd.tax_type, '') AS tax_type,
          p.lead_time_days,
          p.late_buffer_days,
          p.wholesale_buffer_days,
          p.order_cycle_days,
          COALESCE(p.planning_enabled, 0) AS planning_enabled,
          COALESCE(p.remark, '') AS remark,
          p.last_update_date_time
       FROM ap_supplier s
       LEFT JOIN ap_supplier_detail sd ON sd.ap_code = s.code AND COALESCE(sd.tax_type, '') <> ''
       LEFT JOIN purchase_planning_supplier_setting p ON p.ap_code = s.code
       WHERE COALESCE(s.code, '') <> ''${search.sql}${enabledClause}
       ORDER BY s.code
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

router.post('/supplier-settings/save/:apCode', async (req, res) => {
  const apCode = clean(req.params.apCode)
  const body = req.body || {}
  const userCode = clean(req.user?.code)
  if (!apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสเจ้าหนี้' })

  try {
    const result = await posDB.query(
      `INSERT INTO purchase_planning_supplier_setting (
          ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
          planning_enabled, remark, create_code, last_update_code
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
       ON CONFLICT (ap_code) DO UPDATE SET
          lead_time_days = EXCLUDED.lead_time_days,
          late_buffer_days = EXCLUDED.late_buffer_days,
          wholesale_buffer_days = EXCLUDED.wholesale_buffer_days,
          order_cycle_days = EXCLUDED.order_cycle_days,
          planning_enabled = EXCLUDED.planning_enabled,
          remark = EXCLUDED.remark,
          last_update_code = EXCLUDED.last_update_code,
          last_update_date_time = now()
       RETURNING *`,
      [
        apCode,
        toIntOrNull(body.lead_time_days),
        toIntOrNull(body.late_buffer_days),
        toIntOrNull(body.wholesale_buffer_days),
        toIntOrNull(body.order_cycle_days),
        Number(body.planning_enabled) === 0 ? 0 : 1,
        clean(body.remark).slice(0, 255),
        userCode,
      ],
    )

    res.json({ success: true, data: result.rows[0] })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/supplier-settings/sync-from-purchase-history', async (req, res) => {
  const body = req.body || {}
  const dryRun = boolQuery(body.dry_run)
  const fromDate = optionalDate(body.from_date)
  const toDate = optionalDate(body.to_date)
  const remark = clean(body.remark || 'sync from purchase history').slice(0, 255)
  const userCode = clean(req.user?.code)
  const cte = supplierHistoryCte({ fromDate, toDate, startIndex: 1 })

  try {
    if (dryRun) {
      const previewResult = await posDB.query(
        `${cte.sql}
         SELECT ap_code, ap_name, latest_purchase_date, purchase_line_count,
                COUNT(*) OVER()::int AS missing_count
         FROM missing_suppliers
         ORDER BY latest_purchase_date DESC NULLS LAST, ap_code
         LIMIT 20`,
        cte.params,
      )
      return res.json({
        success: true,
        dryRun: true,
        missingCount: Number(previewResult.rows[0]?.missing_count || 0),
        insertedCount: 0,
        preview: previewResult.rows.map(({ missing_count, ...row }) => row),
        fromDate,
        toDate,
      })
    }

    const result = await withPosTransaction((client) => client.query(
      `${cte.sql},
       inserted AS (
         INSERT INTO purchase_planning_supplier_setting (
           ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
           planning_enabled, remark, create_code, last_update_code
         )
         SELECT ap_code, 1, 1, 1, 1, 1, $${cte.params.length + 1}, $${cte.params.length + 2}, $${cte.params.length + 2}
         FROM missing_suppliers
         ORDER BY latest_purchase_date DESC NULLS LAST, ap_code
         ON CONFLICT (ap_code) DO NOTHING
         RETURNING ap_code
       )
       SELECT
         (SELECT COUNT(*)::int FROM missing_suppliers) AS missing_count,
         (SELECT COUNT(*)::int FROM inserted) AS inserted_count`,
      [...cte.params, remark, userCode],
    ))

    res.json({
      success: true,
      dryRun: false,
      missingCount: Number(result.rows[0]?.missing_count || 0),
      insertedCount: Number(result.rows[0]?.inserted_count || 0),
      preview: [],
      fromDate,
      toDate,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── Supplier: Excel export/import ────────────────────────────────────────────
router.get('/supplier-settings/export', async (req, res) => {
  try {
    const dataResult = await posDB.query(
      `SELECT
          s.code AS ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          COALESCE(sd.tax_type, '') AS tax_type,
          p.lead_time_days,
          p.late_buffer_days,
          p.wholesale_buffer_days,
          p.order_cycle_days,
          COALESCE(p.planning_enabled, 0) AS planning_enabled,
          COALESCE(p.remark, '') AS remark
       FROM ap_supplier s
       LEFT JOIN ap_supplier_detail sd ON sd.ap_code = s.code AND COALESCE(sd.tax_type, '') <> ''
       LEFT JOIN purchase_planning_supplier_setting p ON p.ap_code = s.code
       WHERE COALESCE(s.code, '') <> ''
       ORDER BY s.code`,
    )
    const rows = buildExportSheet(dataResult.rows, SUPPLIER_COLUMNS)
    const sheet = XLSX.utils.json_to_sheet(rows, {
      header: SUPPLIER_COLUMNS.map((c) => c.header),
    })
    // ปรับความกว้างคอลัมน์ให้อ่านง่าย
    sheet['!cols'] = SUPPLIER_COLUMNS.map((col) => ({ wch: Math.max(col.header.length * 2, 12) }))
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, sheet, 'เจ้าหนี้')
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' })
    const filename = `supplier-settings-${new Date().toISOString().slice(0, 10)}.xlsx`
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`)
    res.send(buffer)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/supplier-settings/import', importUpload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'กรุณาแนบไฟล์ Excel' })
  const commit = boolQuery(req.body.commit)
  const userCode = clean(req.user?.code)

  try {
    const { rows } = parseImportRows(req.file.buffer, SUPPLIER_COLUMNS)
    if (rows.length === 0) return res.json({ success: true, commit: false, total: 0, updated: 0, errors: [], preview: [] })
    if (rows.length > IMPORT_MAX_ROWS) {
      return res.status(400).json({ error: `จำนวน row เกินกำหนด (สูงสุด ${IMPORT_MAX_ROWS} รายการ)` })
    }

    // validate: ap_code ต้องไม่ว่าง, และต้องมีอยู่จริงใน ap_supplier
    const errors = []
    const validRows = []
    for (const { lineNo, parsed } of rows) {
      const apCode = parsed.ap_code
      if (!apCode) {
        errors.push({ line: lineNo, message: 'ไม่ระบุรหัสเจ้าหนี้' })
        continue
      }
      validRows.push({ lineNo, parsed })
    }

    const codes = [...new Set(validRows.map((r) => r.parsed.ap_code))]
    let existingMap = new Map()
    if (codes.length) {
      const existingResult = await posDB.query(
        `SELECT code FROM ap_supplier WHERE code = ANY($1::text[])`,
        [codes],
      )
      existingMap = new Set(existingResult.rows.map((r) => r.code))
    }
    const checkedRows = []
    for (const { lineNo, parsed } of validRows) {
      if (!existingMap.has(parsed.ap_code)) {
        errors.push({ line: lineNo, ap_code: parsed.ap_code, message: 'ไม่พบรหัสเจ้าหนี้ในระบบ' })
        continue
      }
      checkedRows.push(parsed)
    }
    const rowsToCommit = [...new Map(checkedRows.map((r) => [r.ap_code, r])).values()]

    const preview = checkedRows.slice(0, 10).map((r) => ({
      ap_code: r.ap_code,
      lead_time_days: r.lead_time_days,
      planning_enabled: r.planning_enabled,
      remark: r.remark,
    }))

    if (!commit) {
      return res.json({
        success: true,
        commit: false,
        total: rows.length,
        updated: checkedRows.length,
        errors,
        preview,
      })
    }

    // commit: upsert ทั้งหมดใน transaction
    const result = await withPosTransaction(async (client) => {
      if (!rowsToCommit.length) return { updated: 0 }
      const upsertResult = await client.query(
        `WITH input AS (
           SELECT *
           FROM unnest(
             $1::text[],
             $2::int[],
             $3::int[],
             $4::int[],
             $5::int[],
             $6::int[],
             $7::text[]
           ) AS t(
             ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days,
             order_cycle_days, planning_enabled, remark
           )
         )
         INSERT INTO purchase_planning_supplier_setting (
            ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
            planning_enabled, remark, create_code, last_update_code
         )
         SELECT
            ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
            planning_enabled, remark, $8::text, $8::text
         FROM input
         ON CONFLICT (ap_code) DO UPDATE SET
            lead_time_days = EXCLUDED.lead_time_days,
            late_buffer_days = EXCLUDED.late_buffer_days,
            wholesale_buffer_days = EXCLUDED.wholesale_buffer_days,
            order_cycle_days = EXCLUDED.order_cycle_days,
            planning_enabled = EXCLUDED.planning_enabled,
            remark = EXCLUDED.remark,
            last_update_code = EXCLUDED.last_update_code,
            last_update_date_time = now()`,
        [
          rowsToCommit.map((r) => r.ap_code),
          rowsToCommit.map((r) => r.lead_time_days),
          rowsToCommit.map((r) => r.late_buffer_days),
          rowsToCommit.map((r) => r.wholesale_buffer_days),
          rowsToCommit.map((r) => r.order_cycle_days),
          rowsToCommit.map((r) => (Number(r.planning_enabled) === 0 ? 0 : 1)),
          rowsToCommit.map((r) => r.remark),
          userCode,
        ],
      )
      return { updated: upsertResult.rowCount }
    })

    res.json({
      success: true,
      commit: true,
      total: rows.length,
      updated: result.updated,
      errors,
      preview,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/item-settings', async (req, res) => {
  const { page, limit, offset } = pageParams(req)
  const search = itemSearchClause(req.query.search, 1)
  const params = [...search.params]

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ic_inventory i
       LEFT JOIN purchase_planning_item_setting p ON p.ic_code = i.code
       WHERE COALESCE(i.code, '') <> ''${search.sql}`,
      params,
    )

    const dataResult = await posDB.query(
      `SELECT
          i.code AS ic_code,
          COALESCE(i.name_1, '') AS ic_name,
          COALESCE(i.unit_standard, '') AS unit_code,
          COALESCE(d.is_hold_sale, 0) AS is_hold_sale,
          COALESCE(d.is_hold_purchase, 0) AS is_hold_purchase,
          p.lead_time_days,
          p.late_buffer_days,
          p.wholesale_buffer_days,
          p.order_cycle_days,
          COALESCE(p.planning_enabled, 0) AS planning_enabled,
          COALESCE(p.remark, '') AS remark,
          p.last_update_date_time
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       LEFT JOIN purchase_planning_item_setting p ON p.ic_code = i.code
       WHERE COALESCE(i.code, '') <> ''${search.sql}
       ORDER BY i.code
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

router.post('/item-settings/save/:icCode', async (req, res) => {
  const icCode = clean(req.params.icCode)
  const body = req.body || {}
  const userCode = clean(req.user?.code)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  try {
    const result = await posDB.query(
      `INSERT INTO purchase_planning_item_setting (
          ic_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
          planning_enabled, remark, create_code, last_update_code
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
       ON CONFLICT (ic_code) DO UPDATE SET
          lead_time_days = EXCLUDED.lead_time_days,
          late_buffer_days = EXCLUDED.late_buffer_days,
          wholesale_buffer_days = EXCLUDED.wholesale_buffer_days,
          order_cycle_days = EXCLUDED.order_cycle_days,
          planning_enabled = EXCLUDED.planning_enabled,
          remark = EXCLUDED.remark,
          last_update_code = EXCLUDED.last_update_code,
          last_update_date_time = now()
       RETURNING *`,
      [
        icCode,
        toIntOrNull(body.lead_time_days),
        toIntOrNull(body.late_buffer_days),
        toIntOrNull(body.wholesale_buffer_days),
        toIntOrNull(body.order_cycle_days),
        Number(body.planning_enabled) === 0 ? 0 : 1,
        clean(body.remark).slice(0, 255),
        userCode,
      ],
    )
    res.json({ success: true, data: result.rows[0] })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/item-supplier-settings', async (req, res) => {
  const { page, limit, offset } = pageParams(req)
  const search = searchClause(req.query.search, ['link.ic_code', 'i.name_1', 'link.ap_code', 's.name_1'], 1)
  const enabledOnly = boolQuery(req.query.enabled_only)
  const enabledClause = enabledOnly ? ' AND p.planning_enabled = 1' : ''
  const params = [...search.params]

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM (
         SELECT DISTINCT link.ic_code, link.ap_code
         FROM ap_item_by_supplier link
         LEFT JOIN ic_inventory i ON i.code = link.ic_code
         LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
         JOIN ap_supplier s ON s.code = link.ap_code
         JOIN purchase_planning_supplier_setting supplier_plan
           ON supplier_plan.ap_code = link.ap_code
          AND COALESCE(supplier_plan.planning_enabled, 0) = 1
         LEFT JOIN purchase_planning_item_supplier_setting p
           ON p.ic_code = link.ic_code AND p.ap_code = link.ap_code
         WHERE COALESCE(link.ic_code, '') <> ''
           AND COALESCE(link.ap_code, '') <> ''
           AND ${linkableItemWhere('i', 'd')}
           AND ${activeSupplierWhere('s')}${search.sql}${enabledClause}
       ) x`,
      params,
    )

    const dataResult = await posDB.query(
      `WITH latest_price AS (
         SELECT DISTINCT ON (item_code, cust_code)
           item_code AS ic_code,
           cust_code AS ap_code,
           price_exclude_vat AS price,
           unit_code,
           doc_date,
           doc_time
         FROM ic_trans_detail
         WHERE trans_flag = 310
           AND COALESCE(status, 0) = 0
           AND COALESCE(item_code, '') <> ''
           AND COALESCE(cust_code, '') <> ''
         ORDER BY item_code, cust_code, doc_date DESC, doc_time DESC, doc_no DESC
       )
       SELECT DISTINCT ON (link.ic_code, link.ap_code)
          link.ic_code,
          COALESCE(i.name_1, '') AS ic_name,
          COALESCE(i.unit_standard, '') AS item_unit_code,
          link.ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          lp.price AS last_purchase_price,
          COALESCE(lp.unit_code, '') AS last_purchase_unit_code,
          lp.doc_date AS last_purchase_date,
          p.lead_time_days,
          p.late_buffer_days,
          p.wholesale_buffer_days,
          p.order_cycle_days,
          COALESCE(p.min_order_qty, 0) AS min_order_qty,
          COALESCE(NULLIF(p.pack_size, 0), 1) AS pack_size,
          COALESCE(p.purchase_unit_code, '') AS purchase_unit_code,
          COALESCE(p.planning_enabled, 0) AS planning_enabled,
          COALESCE(p.is_preferred, 0) AS is_preferred,
          COALESCE(p.remark, '') AS remark,
          p.last_update_date_time
       FROM ap_item_by_supplier link
       LEFT JOIN ic_inventory i ON i.code = link.ic_code
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       JOIN ap_supplier s ON s.code = link.ap_code
       JOIN purchase_planning_supplier_setting supplier_plan
         ON supplier_plan.ap_code = link.ap_code
        AND COALESCE(supplier_plan.planning_enabled, 0) = 1
       LEFT JOIN purchase_planning_item_supplier_setting p
         ON p.ic_code = link.ic_code AND p.ap_code = link.ap_code
       LEFT JOIN latest_price lp
         ON lp.ic_code = link.ic_code AND lp.ap_code = link.ap_code
       WHERE COALESCE(link.ic_code, '') <> ''
         AND COALESCE(link.ap_code, '') <> ''
         AND ${linkableItemWhere('i', 'd')}
         AND ${activeSupplierWhere('s')}${search.sql}${enabledClause}
       ORDER BY link.ic_code, link.ap_code
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

router.post('/item-supplier-settings/save', async (req, res) => {
  const body = req.body || {}
  const icCode = clean(body.ic_code)
  const apCode = clean(body.ap_code)
  const userCode = clean(req.user?.code)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })
  if (!apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสเจ้าหนี้' })

  const minOrderQty = Math.max(0, toNumber(body.min_order_qty, 0))
  const packSize = Math.max(0.000001, toNumber(body.pack_size, 1))

  try {
    const result = await posDB.query(
      `INSERT INTO purchase_planning_item_supplier_setting (
          ic_code, ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
          min_order_qty, pack_size, purchase_unit_code, planning_enabled, is_preferred,
          remark, create_code, last_update_code
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13)
       ON CONFLICT (ic_code, ap_code) DO UPDATE SET
          lead_time_days = EXCLUDED.lead_time_days,
          late_buffer_days = EXCLUDED.late_buffer_days,
          wholesale_buffer_days = EXCLUDED.wholesale_buffer_days,
          order_cycle_days = EXCLUDED.order_cycle_days,
          min_order_qty = EXCLUDED.min_order_qty,
          pack_size = EXCLUDED.pack_size,
          purchase_unit_code = EXCLUDED.purchase_unit_code,
          planning_enabled = EXCLUDED.planning_enabled,
          is_preferred = EXCLUDED.is_preferred,
          remark = EXCLUDED.remark,
          last_update_code = EXCLUDED.last_update_code,
          last_update_date_time = now()
       RETURNING *`,
      [
        icCode,
        apCode,
        toIntOrNull(body.lead_time_days),
        toIntOrNull(body.late_buffer_days),
        toIntOrNull(body.wholesale_buffer_days),
        toIntOrNull(body.order_cycle_days),
        minOrderQty,
        packSize,
        clean(body.purchase_unit_code).slice(0, 25),
        Number(body.planning_enabled) === 0 ? 0 : 1,
        Number(body.is_preferred) === 1 ? 1 : 0,
        clean(body.remark).slice(0, 255),
        userCode,
      ],
    )
    res.json({ success: true, data: result.rows[0] })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/item-supplier-settings/sync-from-purchase-history', async (req, res) => {
  const body = req.body || {}
  const dryRun = boolQuery(body.dry_run)
  const fromDate = optionalDate(body.from_date)
  const toDate = optionalDate(body.to_date)
  const remark = clean(body.remark || 'sync from purchase history').slice(0, 255)
  const cte = itemSupplierHistoryCte({ fromDate, toDate, startIndex: 1 })

  try {
    if (dryRun) {
      const previewResult = await posDB.query(
        `${cte.sql}
         SELECT ic_code, ic_name, ap_code, ap_name, latest_purchase_date, purchase_line_count,
                COUNT(*) OVER()::int AS missing_count
         FROM missing_pairs
         ORDER BY latest_purchase_date DESC NULLS LAST, ic_code, ap_code
         LIMIT 20`,
        cte.params,
      )
      return res.json({
        success: true,
        dryRun: true,
        missingCount: Number(previewResult.rows[0]?.missing_count || 0),
        insertedCount: 0,
        preview: previewResult.rows.map(({ missing_count, max_line_number, ...row }) => row),
        fromDate,
        toDate,
      })
    }

    const result = await withPosTransaction((client) => client.query(
      `${cte.sql},
       inserted AS (
         INSERT INTO ap_item_by_supplier (ap_code, ic_code, remark, status, line_number)
         SELECT
           ap_code,
           ic_code,
           $${cte.params.length + 1},
           0,
           max_line_number + ROW_NUMBER() OVER (
             PARTITION BY ic_code
             ORDER BY latest_purchase_date DESC NULLS LAST, ap_code
           )
         FROM missing_pairs
         ORDER BY ic_code, latest_purchase_date DESC NULLS LAST, ap_code
         RETURNING ic_code, ap_code
       )
       SELECT
         (SELECT COUNT(*)::int FROM missing_pairs) AS missing_count,
         (SELECT COUNT(*)::int FROM inserted) AS inserted_count`,
      [...cte.params, remark],
    ))

    res.json({
      success: true,
      dryRun: false,
      missingCount: Number(result.rows[0]?.missing_count || 0),
      insertedCount: Number(result.rows[0]?.inserted_count || 0),
      preview: [],
      fromDate,
      toDate,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── Item-Supplier: Excel export/import ───────────────────────────────────────
router.get('/item-supplier-settings/export', async (req, res) => {
  try {
    const dataResult = await posDB.query(
      `SELECT DISTINCT ON (link.ic_code, link.ap_code)
          link.ic_code,
          COALESCE(i.name_1, '') AS ic_name,
          link.ap_code,
          COALESCE(s.name_1, '') AS ap_name,
          p.lead_time_days,
          p.late_buffer_days,
          p.wholesale_buffer_days,
          p.order_cycle_days,
          COALESCE(p.min_order_qty, 0) AS min_order_qty,
          COALESCE(p.is_preferred, 0) AS is_preferred,
          COALESCE(p.planning_enabled, 0) AS planning_enabled,
          COALESCE(p.remark, '') AS remark
       FROM ap_item_by_supplier link
       LEFT JOIN ic_inventory i ON i.code = link.ic_code
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       JOIN ap_supplier s ON s.code = link.ap_code
       JOIN purchase_planning_supplier_setting supplier_plan
         ON supplier_plan.ap_code = link.ap_code
        AND COALESCE(supplier_plan.planning_enabled, 0) = 1
       LEFT JOIN purchase_planning_item_supplier_setting p
         ON p.ic_code = link.ic_code AND p.ap_code = link.ap_code
       WHERE COALESCE(link.ic_code, '') <> ''
         AND COALESCE(link.ap_code, '') <> ''
         AND ${linkableItemWhere('i', 'd')}
         AND ${activeSupplierWhere('s')}
       ORDER BY link.ic_code, link.ap_code`,
    )
    const rows = buildExportSheet(dataResult.rows, ITEM_SUPPLIER_COLUMNS)
    const sheet = XLSX.utils.json_to_sheet(rows, {
      header: ITEM_SUPPLIER_COLUMNS.map((c) => c.header),
    })
    sheet['!cols'] = ITEM_SUPPLIER_COLUMNS.map((col) => ({ wch: Math.max(col.header.length * 2, 12) }))
    const wb = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(wb, sheet, 'สินค้า+เจ้าหนี้')
    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' })
    const filename = `item-supplier-settings-${new Date().toISOString().slice(0, 10)}.xlsx`
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`)
    res.send(buffer)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/item-supplier-settings/import', importUpload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'กรุณาแนบไฟล์ Excel' })
  const commit = boolQuery(req.body.commit)
  const userCode = clean(req.user?.code)

  try {
    const { rows } = parseImportRows(req.file.buffer, ITEM_SUPPLIER_COLUMNS)
    if (rows.length === 0) return res.json({ success: true, commit: false, total: 0, updated: 0, errors: [], preview: [] })
    if (rows.length > IMPORT_MAX_ROWS) {
      return res.status(400).json({ error: `จำนวน row เกินกำหนด (สูงสุด ${IMPORT_MAX_ROWS} รายการ)` })
    }

    const errors = []
    const validRows = []
    for (const { lineNo, parsed } of rows) {
      if (!parsed.ic_code || !parsed.ap_code) {
        errors.push({ line: lineNo, message: 'ไม่ระบุรหัสสินค้าหรือรหัสเจ้าหนี้' })
        continue
      }
      validRows.push({ lineNo, parsed })
    }

    // validate ว่าคู่ (ic_code, ap_code) มีอยู่จริงใน ap_item_by_supplier
    let existingPairs = new Set()
    if (validRows.length) {
      const pairRows = await posDB.query(
        `WITH input AS (
           SELECT DISTINCT ic_code, ap_code
           FROM unnest($1::text[], $2::text[]) AS t(ic_code, ap_code)
         )
         SELECT link.ic_code, link.ap_code
         FROM input inp
         JOIN ap_item_by_supplier link
           ON link.ic_code = inp.ic_code AND link.ap_code = inp.ap_code
         LEFT JOIN ic_inventory i ON i.code = link.ic_code
         LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
         JOIN ap_supplier s ON s.code = link.ap_code
         JOIN purchase_planning_supplier_setting supplier_plan
           ON supplier_plan.ap_code = link.ap_code
          AND COALESCE(supplier_plan.planning_enabled, 0) = 1
         WHERE ${linkableItemWhere('i', 'd')}
           AND ${activeSupplierWhere('s')}`,
        [
          validRows.map((r) => r.parsed.ic_code),
          validRows.map((r) => r.parsed.ap_code),
        ],
      )
      existingPairs = new Set(pairRows.rows.map((r) => `${r.ic_code}\u0000${r.ap_code}`))
    }

    const checkedRows = []
    for (const { lineNo, parsed } of validRows) {
      const pairKey = `${parsed.ic_code}\u0000${parsed.ap_code}`
      if (!existingPairs.has(pairKey)) {
        errors.push({
          line: lineNo,
          ic_code: parsed.ic_code,
          ap_code: parsed.ap_code,
          message: 'ไม่พบคู่สินค้า+เจ้าหนี้ที่ผูกกันในระบบ',
        })
        continue
      }
      checkedRows.push(parsed)
    }
    const rowsToCommit = [...new Map(checkedRows.map((r) => [`${r.ic_code}\u0000${r.ap_code}`, r])).values()]

    const preview = checkedRows.slice(0, 10).map((r) => ({
      ic_code: r.ic_code,
      ap_code: r.ap_code,
      lead_time_days: r.lead_time_days,
      min_order_qty: r.min_order_qty,
      is_preferred: r.is_preferred,
      planning_enabled: r.planning_enabled,
    }))

    if (!commit) {
      return res.json({
        success: true,
        commit: false,
        total: rows.length,
        updated: checkedRows.length,
        errors,
        preview,
      })
    }

    // commit: upsert ทั้งหมดใน transaction
    const result = await withPosTransaction(async (client) => {
      if (!rowsToCommit.length) return { updated: 0 }
      const upsertResult = await client.query(
        `WITH input AS (
           SELECT *
           FROM unnest(
             $1::text[],
             $2::text[],
             $3::int[],
             $4::int[],
             $5::int[],
             $6::int[],
             $7::numeric[],
             $8::int[],
             $9::int[],
             $10::text[]
           ) AS t(
             ic_code, ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days,
             order_cycle_days, min_order_qty, planning_enabled, is_preferred, remark
           )
         )
         INSERT INTO purchase_planning_item_supplier_setting (
            ic_code, ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
            min_order_qty, planning_enabled, is_preferred, remark, create_code, last_update_code
         )
         SELECT
            ic_code, ap_code, lead_time_days, late_buffer_days, wholesale_buffer_days, order_cycle_days,
            min_order_qty, planning_enabled, is_preferred, remark, $11::text, $11::text
         FROM input
         ON CONFLICT (ic_code, ap_code) DO UPDATE SET
            lead_time_days = EXCLUDED.lead_time_days,
            late_buffer_days = EXCLUDED.late_buffer_days,
            wholesale_buffer_days = EXCLUDED.wholesale_buffer_days,
            order_cycle_days = EXCLUDED.order_cycle_days,
            min_order_qty = EXCLUDED.min_order_qty,
            planning_enabled = EXCLUDED.planning_enabled,
            is_preferred = EXCLUDED.is_preferred,
            remark = EXCLUDED.remark,
            last_update_code = EXCLUDED.last_update_code,
            last_update_date_time = now()`,
        [
          rowsToCommit.map((r) => r.ic_code),
          rowsToCommit.map((r) => r.ap_code),
          rowsToCommit.map((r) => r.lead_time_days),
          rowsToCommit.map((r) => r.late_buffer_days),
          rowsToCommit.map((r) => r.wholesale_buffer_days),
          rowsToCommit.map((r) => r.order_cycle_days),
          rowsToCommit.map((r) => Math.max(0, toNumber(r.min_order_qty, 0))),
          rowsToCommit.map((r) => (Number(r.planning_enabled) === 0 ? 0 : 1)),
          rowsToCommit.map((r) => (Number(r.is_preferred) === 1 ? 1 : 0)),
          rowsToCommit.map((r) => r.remark),
          userCode,
        ],
      )
      return { updated: upsertResult.rowCount }
    })

    res.json({
      success: true,
      commit: true,
      total: rows.length,
      updated: result.updated,
      errors,
      preview,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/report', async (req, res) => {
  const { page, limit, offset } = pageParams(req)
  const options = planningOptions(req)
  const countWhere = planningWhere(req, 1)
  const where = planningWhere(req, 4)
  const startIndex = 4 + where.params.length

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       ${countWhere.sql}`,
      countWhere.params,
    )

    const rowsResult = await posDB.query(
      buildPlanningMetricsSql({ whereSql: where.sql, startIndex, includePagination: true }),
      [options.asOfDate, options.warehouse, options.days, ...where.params, offset, limit],
    )

    res.json({
      data: rowsResult.rows,
      total: countResult.rows[0]?.total || 0,
      page,
      limit,
      options,
      summary: summaryFromRows(rowsResult.rows),
      pending_pr: await getPendingPRCount(),
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/report-lazy', async (req, res) => {
  pruneReportJobs()
  const body = req.body || {}
  const query = takeParams(body, ['search', 'supplier_search', 'ap_search', 'days', 'as_of_date', 'warehouse', 'group_main', 'ap_code', 'alert_only'])
  const batchSize = Math.min(10, Math.max(1, parseInt(body.batch_size, 10) || 5))
  const key = reportJobKey(req.user?.code, query)

  try {
    // ไม่ reuse cache จากหน้าจอ — ทุกครั้งที่กดประมวลผลต้องคำนวณใหม่เสมอ
    // (แก้ปัญหา: เพิ่มสินค้าเข้าแผนแล้วกดประมวลผลซ้ำ แต่สินค้าใหม่ไม่ขึ้น)
    // ใช้ id ที่ unique ทุกครั้ง (key + timestamp + random) เพื่อไม่ให้ชนกับ job เดิม
    const id = `${key}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

    const queryReq = { query }
    const countWhere = planningWhere(queryReq, 1)
    const options = planningOptions(queryReq)
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       ${countWhere.sql}`,
      countWhere.params,
    )

    const job = {
      id,
      query,
      options,
      batchSize,
      total: Number(countResult.rows[0]?.total || 0),
      processed: 0,
      rows: [],
      summary: summaryFromRows([]),
      partialSummary: summaryFromRows([]),
      status: 'queued',
      error: '',
      createdAt: Date.now(),
      updatedAt: Date.now(),
      cancelled: false,
    }
    reportJobs.set(id, job)
    setImmediate(() => runReportJob(job))

    res.json({
      job_id: id,
      status: job.status,
      total: job.total,
      processed: job.processed,
      options,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/report-lazy/:jobId', async (req, res) => {
  pruneReportJobs()
  const job = reportJobs.get(clean(req.params.jobId))
  if (!job) return res.status(404).json({ error: 'ไม่พบงานคำนวณรายงาน กรุณาโหลดใหม่' })

  const offset = Math.max(0, parseInt(req.query.offset, 10) || 0)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 30))
  const stockStatus = clean(req.query.stock_status)
  const sourceRows = job.status === 'complete'
    ? job.rows
    : sortPlanningRows([...job.rows])
  const filteredRows = PLANNING_STOCK_STATUSES.has(stockStatus)
    ? sourceRows.filter((row) => row.stock_status === stockStatus)
    : sourceRows
  const rows = filteredRows.slice(offset, offset + limit)

  job.updatedAt = Date.now()
  res.json({
    job_id: job.id,
    status: job.status,
    error: job.error,
    data: rows,
    total: job.total,
    filtered_total: filteredRows.length,
    processed: job.processed,
    offset,
    limit,
    has_more: job.status === 'complete' ? offset + rows.length < filteredRows.length : true,
    summary: job.status === 'complete' ? job.summary : null,
    partial_summary: job.partialSummary,
    pending_pr: job.pending_pr || { total: 0, byAp: {}, byItemAp: {} },
    options: job.options,
  })
})

// ─────────────────────────────────────────────────────────────────────────────
// POST /purchase-planning/pr/create — สร้างใบเสนอซื้อ (PR, trans_flag=2)
// 1 PR ต่อเจ้าหนี้ จากรายการที่จัดกลุ่มแล้ว
// ─────────────────────────────────────────────────────────────────────────────

// สร้างเลขที่เอกสาร CRMPR{YYYYMMDD}-{N} โดย N = MAX+1 ของ prefix วันนั้น
async function generatePRDocNo(client, docDateStr) {
  const dateStr = docDateStr.replaceAll('-', '') // YYYYMMDD
  const prefix = `CRMPR${dateStr}-`
  const result = await client.query(
    `SELECT COALESCE(MAX(CAST(SUBSTRING(doc_no FROM $1::int) AS int)), 0) + 1 AS next_no
     FROM ic_trans
     WHERE trans_flag = 2 AND doc_no LIKE $2::text`,
    [prefix.length + 1, prefix + '%'],
  )
  const nextNo = Number(result.rows[0]?.next_no || 1)
  return `${prefix}${nextNo}`
}

// แปลงวันที่ ISO (YYYY-MM-DD) เป็นวันที่ไทย d/M/yyyy (พ.ศ.) สำหรับ logs.data1
function toThaiDocDate(dateStr) {
  const d = new Date(dateStr + 'T00:00:00')
  const day = d.getDate()
  const month = d.getMonth() + 1
  const thaiYear = d.getFullYear() + 543
  return `${day}/${month}/${thaiYear}`
}

// สร้าง GUID 32 hex ไม่มี dash
function newPRLogGuid() {
  return crypto.randomUUID().replace(/-/g, '')
}

router.post('/pr/create', async (req, res) => {
  const body = req.body || {}
  const userCode = clean(req.user?.code) || 'CRM'
  const docDate = optionalDate(body.doc_date)
  const docTime = clean(body.doc_time) || new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Bangkok', hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(new Date())
  const branchCode = clean(body.branch_code) || '0000'
  const fallbackRemark = clean(body.remark || '').slice(0, 255) // กรณีเก่าที่ส่ง remark รวม
  const groups = Array.isArray(body.groups) ? body.groups : []

  // validate
  if (!docDate) return res.status(400).json({ error: 'กรุณาระบุวันที่เอกสาร' })
  if (!groups.length) return res.status(400).json({ error: 'กรุณาระบุรายการที่จะสร้าง PR' })

  for (const g of groups) {
    if (!clean(g.ap_code)) return res.status(400).json({ error: 'กรุณาระบุรหัสเจ้าหนี้ของทุกกลุ่ม' })
    if (!Array.isArray(g.items) || !g.items.length) return res.status(400).json({ error: 'แต่ละกลุ่มต้องมีสินค้าอย่างน้อย 1 รายการ' })
    for (const it of g.items) {
      if (!clean(it.item_code)) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })
      if (!(Number(it.qty) > 0)) return res.status(400).json({ error: 'จำนวนต้องมากกว่า 0' })
      if (Number(it.price) < 0) return res.status(400).json({ error: 'ราคาต้องไม่ติดลบ' })
    }
  }

  try {
    const result = await withPosTransaction(async (client) => {
      // อ่าน vat_rate จาก erp_option
      const vatResult = await client.query(`SELECT vat_rate FROM erp_option LIMIT 1`)
      const vatRate = Number(vatResult.rows[0]?.vat_rate || 7)
      const divisor = 100 + vatRate

      const prDocs = []
      for (const g of groups) {
        const apCode = clean(g.ap_code)
        // หมายเหตุแยกตามกลุ่ม/PR — ถ้ากลุ่มไม่ส่งมา ใช้ fallbackRemark (backward compatible)
        const remark = clean(g.remark || fallbackRemark).slice(0, 255)

        // คำนวณยอดต่อ PR
        let totalValue = 0
        for (const it of g.items) {
          totalValue += Number(it.qty) * Number(it.price)
        }
        const totalBeforeVat = Math.round((totalValue * 100 / divisor) * 100) / 100
        const totalVatValue = Math.round((totalValue - totalBeforeVat) * 100) / 100
        const totalAfterVat = totalValue

        // สร้าง doc_no
        const docNo = await generatePRDocNo(client, docDate)

        // INSERT ic_trans (header)
        await client.query(
          `INSERT INTO ic_trans (
            trans_type, trans_flag, doc_date, doc_time, doc_no,
            inquiry_type, vat_type, cust_code, branch_code, vat_rate,
            total_value, total_before_vat, total_vat_value, total_after_vat, total_except_vat,
            total_amount, balance_amount, user_request, approve_status,
            doc_format_code, remark, creator_code, create_datetime
          ) VALUES (
            1, 2, $1::date, $2, $3,
            0, 1, $4, $5, $6,
            $7, $8, $9, $10, $7,
            $10, $10, $11, 0,
            'PR', $12, $11, NOW()
          )`,
          [
            docDate, docTime, docNo,
            apCode, branchCode, vatRate,
            totalValue, totalBeforeVat, totalVatValue, totalAfterVat,
            userCode, remark,
          ],
        )

        // INSERT ic_trans_detail (line items, line_number 10,20,...)
        let lineNo = 10
        for (const it of g.items) {
          const itemCode = clean(it.item_code)
          const itemName = clean(it.item_name).slice(0, 255)
          const unitCode = clean(it.unit_code)
          // บันทึก qty/price ตามหน่วยที่เลือกในบรรทัดเอกสาร
          // เช่น 100 ลัง ต้องเก็บ qty=100, unit_code=ลัง, stand_value=100
          const ratio = Number(it.unit_ratio) || 1
          const lineQty = Math.round(Number(it.qty) * 1000000) / 1000000
          const linePrice = Math.round(Number(it.price) * 1000000) / 1000000
          const sumAmount = Math.round(lineQty * linePrice * 100) / 100
          const standValue = Number(it.unit_stand_value) || ratio || 1
          const divideValue = Number(it.unit_divide_value) || 1
          await client.query(
            `INSERT INTO ic_trans_detail (
              trans_type, trans_flag, doc_date, doc_time, doc_no, cust_code,
              item_code, item_name, unit_code, qty, price, sum_amount,
              line_number, stand_value, divide_value, ratio, calc_flag, vat_type,
              doc_date_calc, doc_time_calc, branch_code, creator_code, create_datetime
            ) VALUES (
              1, 2, $1::date, $2, $3, $4,
              $5, $6, $7, $8, $9, $10,
              $11, $12, $13, $14, 1, 1,
              $1::date, $2, $15, $16, NOW()
            )`,
            [
              docDate, docTime, docNo, apCode,
              itemCode, itemName, unitCode, lineQty, linePrice, sumAmount,
              lineNo, standValue, divideValue, ratio, branchCode, userCode,
            ],
          )
          lineNo += 10
        }

        // INSERT logs (audit)
        const thaiDate = toThaiDocDate(docDate)
        const guid = newPRLogGuid()
        const xmlData = `<?xml version="1.0" encoding="utf-8"?><top>` +
          `<d t=2 f=doc_date>${thaiDate}</d>` +
          `<d t=1 f=doc_time>${docTime}</d>` +
          `<d t=1 f=doc_no>${docNo}</d>` +
          `<d t=1 f=doc_format_code>PR</d>` +
          `<d t=1 f=cust_code>${apCode}</d>` +
          `<d t=4 f=on_hold>False</d>` +
          `<d t=5 f=inquiry_type>0</d>` +
          `<d t=5 f=vat_type>0</d>` +
          `<d t=1 f=user_request>${userCode}</d>` +
          `<d t=1 f=approve_code></d>` +
          `<d t=1 f=remark>${remark}</d>` +
          `</top>`
        await client.query(
          `INSERT INTO logs (
            function_code, data1, user_code, date_time, screen_code, guid,
            doc_date, doc_no, doc_amount, function_type, menu_name, doc_qty
          ) VALUES (1, $1, $2, NOW(), 2, $3, $4::date, $5, $6, 2, 'menu_purchase_requisition', $7)`,
          [xmlData, userCode, guid, docDate, docNo, totalAfterVat, g.items.length],
        )

        prDocs.push({
          ap_code: apCode,
          ap_name: clean(g.ap_name || ''),
          doc_no: docNo,
          total_amount: totalAfterVat,
          item_count: g.items.length,
        })
      }
      return { prDocs }
    })

    res.json({
      success: true,
      pr_count: result.prDocs.length,
      pr_docs: result.prDocs,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/items/:icCode/suppliers', async (req, res) => {
  const icCode = clean(req.params.icCode)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  const options = planningOptions(req)
  const baseReq = { query: { ...req.query, search: '' } }
  const where = planningWhere(baseReq, 4)
  const metricSql = buildPlanningMetricsSql({ whereSql: where.sql, startIndex: 4 + where.params.length, includePagination: false, singleItem: true })

  try {
    const metricResult = await posDB.query(
      metricSql,
      [options.asOfDate, options.warehouse, options.days, ...where.params, icCode],
    )
    if (!metricResult.rows.length) {
      const msg = await resolveItemMissingError(icCode)
      return res.status(404).json({ error: msg })
    }
    const metric = metricResult.rows[0]

    const supplierResult = await posDB.query(
      `WITH latest_purchase AS (
         SELECT DISTINCT ON (td.item_code, td.cust_code)
           td.item_code AS ic_code,
           td.cust_code AS ap_code,
           td.price_exclude_vat AS price,
           td.price_exclude_vat,
           td.unit_code,
           td.doc_no,
           td.doc_date::date AS doc_date,
           td.doc_time
         FROM ic_trans_detail td
         WHERE td.trans_flag = 310
           AND COALESCE(td.status, 0) = 0
           AND td.item_code = $1::text
           AND COALESCE(td.cust_code, '') <> ''
         ORDER BY td.item_code, td.cust_code, td.doc_date DESC, td.doc_time DESC, td.doc_no DESC
       )
       SELECT
         link.ic_code,
         link.ap_code,
         COALESCE(s.name_1, '') AS ap_name,
         COALESCE(sd.tax_type, '') AS tax_type,
         lp.price_exclude_vat AS last_purchase_price,
         lp.price_exclude_vat AS last_purchase_price_exclude_vat,
         COALESCE(lp.unit_code, '') AS last_purchase_unit_code,
         lp.doc_no AS last_purchase_doc_no,
         lp.doc_date AS last_purchase_date,
         COALESCE(r.lead_time_days, 0) AS lead_time_days,
         COALESCE(r.late_buffer_days, 0) AS late_buffer_days,
         COALESCE(r.wholesale_buffer_days, 0) AS wholesale_buffer_days,
         COALESCE(r.order_cycle_days, 0) AS order_cycle_days,
         COALESCE(r.min_order_qty, 0) AS min_order_qty,
         COALESCE(NULLIF(r.pack_size, 0), 1) AS pack_size,
         COALESCE(r.purchase_unit_code, '') AS purchase_unit_code,
         COALESCE(r.is_preferred, 0) AS is_preferred,
         ROW_NUMBER() OVER (
           ORDER BY
             CASE WHEN COALESCE(r.is_preferred, 0) = 1 THEN 0 ELSE 1 END,
             COALESCE(lp.price_exclude_vat, 999999999999),
             lp.doc_date DESC NULLS LAST,
             link.ap_code
         ) AS rank_no
       FROM ap_item_by_supplier link
       LEFT JOIN ap_supplier s ON s.code = link.ap_code
       LEFT JOIN ap_supplier_detail sd ON sd.ap_code = link.ap_code AND COALESCE(sd.tax_type, '') <> ''
       LEFT JOIN purchase_planning_item_supplier_resolved r
         ON r.ic_code = link.ic_code AND r.ap_code = link.ap_code
       LEFT JOIN latest_purchase lp
         ON lp.ic_code = link.ic_code AND lp.ap_code = link.ap_code
       WHERE link.ic_code = $1::text
         AND COALESCE(link.ap_code, '') <> ''
         AND COALESCE(r.planning_enabled, 0) = 1
       ORDER BY rank_no, link.ap_code`,
      [icCode],
    )

    const suppliers = supplierResult.rows.map((supplier) => {
      const lead = Number(supplier.lead_time_days || 0)
      const late = Number(supplier.late_buffer_days || 0)
      const wholesale = Number(supplier.wholesale_buffer_days || 0)
      const cycle = Number(supplier.order_cycle_days || 0)
      const dAvg = Number(metric.d_avg || 0)
      const available = Number(metric.available_qty || 0)
      const rawMinStock = dAvg * (lead + late + wholesale)
      const rawMaxStock = dAvg * (lead + late + wholesale + cycle)
      const minStock = dAvg > 0 ? Math.ceil(Math.max(rawMinStock, MIN_DISPLAY_QTY)) : 0
      const maxStock = dAvg > 0 ? Math.ceil(Math.max(rawMaxStock, rawMinStock, MIN_DISPLAY_QTY)) : 0
      const minOrderQty = Math.max(Number(supplier.min_order_qty || 0), 0)
      const hasEnoughSalesDays = Number(metric.active_stock_days || 0) > 0
      const suggestQty = dAvg > 0 && hasEnoughSalesDays && available <= minStock
        ? Math.ceil(Math.max(maxStock - available, minOrderQty, 0))
        : 0
      return {
        ...supplier,
        min_stock: minStock,
        max_stock: maxStock,
        suggest_qty: suggestQty,
        suggest_amount: suggestQty * Number(supplier.last_purchase_price || 0),
        is_default: Number(supplier.rank_no) === 1 ? 1 : 0,
      }
    })

    res.json({ item: metric, data: suppliers, options })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────────────────────
// GET /purchase-planning/items/:icCode/units — ดึงหน่วยนับทั้งหมดของสินค้า
// ใช้สำหรับแปลงหน่วยสต๊อก/แนะนำซื้อ (ratio = stand_value / divide_value)
// ─────────────────────────────────────────────────────────────────────────────
router.get('/items/:icCode/units', async (req, res) => {
  const icCode = clean(req.params.icCode)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  try {
    const unitResult = await posDB.query(
      `SELECT
         COALESCE(i.unit_standard, '') AS base_unit,
         uu.code AS unit_code,
         COALESCE(NULLIF(uu.stand_value, 0), 1) AS stand_value,
         COALESCE(NULLIF(uu.divide_value, 0), 1) AS divide_value,
         COALESCE(NULLIF(uu.stand_value, 0), 1) / COALESCE(NULLIF(uu.divide_value, 0), 1) AS ratio,
         uu.row_order,
         uu.line_number
       FROM ic_inventory i
       LEFT JOIN ic_unit_use uu ON uu.ic_code = i.code
       WHERE i.code = $1::text
       ORDER BY uu.line_number, uu.row_order`,
      [icCode],
    )

    const baseUnit = unitResult.rows[0]?.base_unit || ''
    // กรณีไม่มี ic_unit_use เลย → ใช้แค่หน่วยมาตรฐาน
    const units = unitResult.rows
      .filter((r) => r.unit_code)
      .map((r) => ({
        unit_code: r.unit_code,
        stand_value: Number(r.stand_value),
        divide_value: Number(r.divide_value),
        ratio: Number(r.ratio),
        is_base: Number(r.ratio) === 1,
      }))
    if (!units.length && baseUnit) {
      units.push({ unit_code: baseUnit, stand_value: 1, divide_value: 1, ratio: 1, is_base: true })
    }

    res.json({ base_unit: baseUnit, units })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/items/:icCode/detail', async (req, res) => {
  const icCode = clean(req.params.icCode)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  const options = planningOptions(req)
  const baseReq = { query: { ...req.query, search: '' } }
  const where = planningWhere(baseReq, 4)
  const metricSql = buildPlanningMetricsSql({ whereSql: where.sql, startIndex: 4 + where.params.length, includePagination: false, singleItem: true })

  try {
    const [metricResult, barcodeResult, purchaseResult, salesTotalResult, topCustomerResult, chartResult, pendingResult, supplierResult, billReceiveResult] = await Promise.all([
      posDB.query(metricSql, [options.asOfDate, options.warehouse, options.days, ...where.params, icCode]),
      posDB.query(
        `SELECT barcode, COALESCE(unit_code, '') AS unit_code
         FROM ic_inventory_barcode
         WHERE ic_code = $1::text
         ORDER BY barcode
         LIMIT 20`,
        [icCode],
      ),
      posDB.query(
        `SELECT
           td.doc_date::date AS doc_date,
           td.doc_time,
           td.doc_no,
           td.cust_code AS ap_code,
           COALESCE(s.name_1, '') AS ap_name,
           COALESCE(sd.tax_type, '') AS tax_type,
           td.barcode,
           td.unit_code,
           td.qty,
           td.price_exclude_vat AS price,
           td.price_exclude_vat,
           td.sum_amount_exclude_vat AS sum_amount,
           td.sum_amount_exclude_vat,
           td.average_cost
         FROM ic_trans_detail td
         LEFT JOIN ap_supplier s ON s.code = td.cust_code
         LEFT JOIN ap_supplier_detail sd ON sd.ap_code = td.cust_code AND COALESCE(sd.tax_type, '') <> ''
         WHERE td.trans_flag = 310
           AND COALESCE(td.status, 0) = 0
           AND td.item_code = $1::text
         ORDER BY td.doc_date DESC, td.doc_time DESC, td.doc_no DESC
         LIMIT 5`,
        [icCode],
      ),
      posDB.query(
        `SELECT
           COALESCE(SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) FILTER (WHERE td.doc_date::date >= $2::date - interval '29 day'), 0) AS qty_30,
           COALESCE(SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) FILTER (WHERE td.doc_date::date >= $2::date - interval '89 day'), 0) AS qty_90,
           COALESCE(SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) FILTER (WHERE td.doc_date::date >= $2::date - interval '179 day'), 0) AS qty_180,
           COALESCE(SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) FILTER (WHERE td.doc_date::date >= $2::date - interval '364 day'), 0) AS qty_365,
           -- มูลค่าขายสุทธิ = ขาย44 - รับคืน48 (sum_amount_exclude_vat) แยกตามช่วง
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '29 day'), 0) AS amount_net_30,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '89 day'), 0) AS amount_net_90,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '179 day'), 0) AS amount_net_180,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_amount_exclude_vat, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '364 day'), 0) AS amount_net_365,
           -- ต้นทุนสุทธิ
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '29 day'), 0) AS cost_net_30,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '89 day'), 0) AS cost_net_90,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '179 day'), 0) AS cost_net_180,
           COALESCE(SUM(CASE WHEN td.trans_flag = 44 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END
                     - CASE WHEN td.trans_flag = 48 THEN COALESCE(td.sum_of_cost, 0) ELSE 0 END)
                     FILTER (WHERE td.doc_date::date >= $2::date - interval '364 day'), 0) AS cost_net_365
         FROM ic_trans_detail td
         JOIN ic_trans t ON t.doc_no = td.doc_no AND t.trans_flag = td.trans_flag
         WHERE td.trans_flag IN (44, 48)
           AND COALESCE(t.last_status, 0) = 0
           AND td.item_code = $1::text
           AND td.doc_date::date BETWEEN $2::date - interval '364 day' AND $2::date`,
        [icCode, options.asOfDate],
      ),
      posDB.query(
        `SELECT
           COALESCE(td.cust_code, '') AS cust_code,
           COALESCE(c.name_1, '') AS cust_name,
           SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty,
           SUM(COALESCE(td.sum_amount_exclude_vat, 0)) AS amount,
           MAX(td.doc_date)::date AS last_sale_date
         FROM ic_trans_detail td
         LEFT JOIN ar_customer c ON c.code = td.cust_code
         WHERE td.trans_flag = 44
           AND COALESCE(td.status, 0) = 0
           AND td.item_code = $1::text
           AND td.doc_date::date BETWEEN $2::date - interval '364 day' AND $2::date
         GROUP BY td.cust_code, c.name_1
         ORDER BY qty DESC NULLS LAST
         LIMIT 10`,
        [icCode, options.asOfDate],
      ),
      posDB.query(
        `WITH days AS (
           SELECT generate_series($2::date - interval '89 day', $2::date, '1 day'::interval)::date AS doc_date
         ),
         move AS (
           SELECT
             td.doc_date::date AS doc_date,
             SUM(CASE WHEN td.trans_flag = 44 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS sale_qty,
             SUM(CASE WHEN td.trans_flag = 12 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS purchase_qty,
             SUM(CASE WHEN td.trans_flag = 310 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS receive_qty,
             SUM(CASE WHEN td.trans_flag = 48 THEN td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1) ELSE 0 END) AS credit_note_qty
           FROM ic_trans_detail td
           JOIN ic_trans t ON t.doc_no = td.doc_no AND t.trans_flag = td.trans_flag
           WHERE td.trans_flag IN (12, 310, 44, 48)
             AND COALESCE(td.status, 0) = 0
             AND COALESCE(t.last_status, 0) = 0
             AND td.item_code = $1::text
             AND td.doc_date::date BETWEEN $2::date - interval '89 day' AND $2::date
           GROUP BY td.doc_date::date
         )
         SELECT
           days.doc_date,
           COALESCE(move.sale_qty, 0) AS sale_qty,
           COALESCE(move.purchase_qty, 0) AS purchase_qty,
           COALESCE(move.receive_qty, 0) AS receive_qty,
           COALESCE(move.credit_note_qty, 0) AS credit_note_qty
         FROM days
         LEFT JOIN move ON move.doc_date = days.doc_date
         ORDER BY days.doc_date`,
        [icCode, options.asOfDate],
      ),
      posDB.query(
        `WITH po_lines AS (
           SELECT
             td.doc_no,
             td.cust_code,
             td.item_code,
             COALESCE(td.item_name, '') AS item_name,
             td.unit_code,
             MIN(td.doc_date)::date AS doc_date,
             MAX(td.doc_time) AS doc_time,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
           FROM ic_trans_detail td
           WHERE td.trans_flag = 6
             AND COALESCE(td.last_status, 0) = 0
             AND td.item_code = $1::text
           GROUP BY td.doc_no, td.cust_code, td.item_code, COALESCE(td.item_name, ''), td.unit_code
         ),
         po_reduce AS (
           SELECT
             td.ref_doc_no AS doc_no,
             td.item_code,
             SUM(td.qty * COALESCE(td.stand_value / NULLIF(td.divide_value, 0), 1)) AS qty
           FROM ic_trans_detail td
           WHERE (
               td.trans_flag IN (12, 310)
               OR (
                 td.trans_flag = 7
                 AND EXISTS (
                   SELECT 1
                   FROM ic_trans t_cancel
                   WHERE t_cancel.doc_no = td.doc_no
                     AND t_cancel.trans_flag = td.trans_flag
                     AND COALESCE(t_cancel.cancel_type, 0) = 2
                 )
               )
             )
             AND COALESCE(td.last_status, 0) = 0
             AND COALESCE(td.ref_doc_no, '') <> ''
             AND td.item_code = $1::text
           GROUP BY td.ref_doc_no, td.item_code
         ),
         pending AS (
           SELECT
             p.*,
             p.qty - COALESCE(r.qty, 0) AS acc_in_balance
           FROM po_lines p
           LEFT JOIN po_reduce r
             ON r.doc_no = p.doc_no
            AND r.item_code = p.item_code
         )
         SELECT
           p.doc_date,
           p.doc_time,
           p.doc_no,
           p.cust_code AS ap_code,
           COALESCE(s.name_1, '') AS ap_name,
           COALESCE(sd.tax_type, '') AS tax_type,
           p.acc_in_balance AS qty,
           p.unit_code,
           GREATEST(($2::date - p.doc_date), 0)::int AS waiting_days
         FROM pending p
         LEFT JOIN ap_supplier s ON s.code = p.cust_code
         LEFT JOIN ap_supplier_detail sd ON sd.ap_code = p.cust_code AND COALESCE(sd.tax_type, '') <> ''
         WHERE p.acc_in_balance <> 0
         ORDER BY p.acc_in_balance, p.doc_date DESC, p.doc_time DESC
         LIMIT 20`,
        [icCode, options.asOfDate],
      ),
      posDB.query(
        `WITH latest_purchase AS (
           SELECT DISTINCT ON (td.item_code, td.cust_code)
             td.item_code AS ic_code,
             td.cust_code AS ap_code,
             td.price_exclude_vat AS price,
             td.price_exclude_vat,
             td.unit_code,
             td.doc_date::date AS doc_date,
             td.doc_time
           FROM ic_trans_detail td
           WHERE td.trans_flag = 310
             AND COALESCE(td.status, 0) = 0
             AND td.item_code = $1::text
             AND COALESCE(td.cust_code, '') <> ''
           ORDER BY td.item_code, td.cust_code, td.doc_date DESC, td.doc_time DESC, td.doc_no DESC
         )
         SELECT
           link.ap_code,
           COALESCE(s.name_1, '') AS ap_name,
           COALESCE(sd.tax_type, '') AS tax_type,
           lp.price_exclude_vat AS last_purchase_price,
           lp.price_exclude_vat AS last_purchase_price_exclude_vat,
           lp.unit_code AS last_purchase_unit_code,
           lp.doc_date AS last_purchase_date,
           COALESCE(r.lead_time_days, 0) AS lead_time_days,
           COALESCE(r.late_buffer_days, 0) AS late_buffer_days,
           COALESCE(r.wholesale_buffer_days, 0) AS wholesale_buffer_days,
           COALESCE(r.order_cycle_days, 0) AS order_cycle_days,
           COALESCE(r.min_order_qty, 0) AS min_order_qty,
           COALESCE(NULLIF(r.pack_size, 0), 1) AS pack_size,
           COALESCE(r.is_preferred, 0) AS is_preferred
         FROM ap_item_by_supplier link
         LEFT JOIN ap_supplier s ON s.code = link.ap_code
         LEFT JOIN ap_supplier_detail sd ON sd.ap_code = link.ap_code AND COALESCE(sd.tax_type, '') <> ''
         LEFT JOIN purchase_planning_item_supplier_resolved r
           ON r.ic_code = link.ic_code AND r.ap_code = link.ap_code
         LEFT JOIN latest_purchase lp
           ON lp.ic_code = link.ic_code AND lp.ap_code = link.ap_code
         WHERE link.ic_code = $1::text
           AND COALESCE(r.planning_enabled, 0) = 1
         ORDER BY CASE WHEN COALESCE(r.is_preferred, 0) = 1 THEN 0 ELSE 1 END,
                  COALESCE(lp.price_exclude_vat, 999999999999),
                  link.ap_code`,
        [icCode],
      ),
      // ทยอยรับ (trans_flag=310, status=0) — 5 ใบล่าสุด
      posDB.query(
        `SELECT
           td.doc_date::date AS doc_date,
           td.doc_time,
           td.doc_no,
           td.cust_code AS ap_code,
           COALESCE(s.name_1, '') AS ap_name,
           COALESCE(sd.tax_type, '') AS tax_type,
           td.qty,
           td.unit_code,
           td.price_exclude_vat
         FROM ic_trans_detail td
         LEFT JOIN ap_supplier s ON s.code = td.cust_code
         LEFT JOIN ap_supplier_detail sd ON sd.ap_code = td.cust_code AND COALESCE(sd.tax_type, '') <> ''
         WHERE td.trans_flag = 310
           AND COALESCE(td.status, 0) = 0
           AND td.item_code = $1::text
         ORDER BY td.doc_date DESC, td.doc_time DESC, td.doc_no DESC
         LIMIT 10`,
        [icCode],
      ),
    ])

    if (!metricResult.rows.length) {
      const msg = await resolveItemMissingError(icCode)
      return res.status(404).json({ error: msg })
    }

    const item = metricResult.rows[0]
    res.json({
      item: {
        ...item,
        image_url: `/api/products/images/primary?item_code=${encodeURIComponent(icCode)}`,
      },
      barcodes: barcodeResult.rows,
      last_purchases: purchaseResult.rows,
      sales_totals: salesTotalResult.rows[0] || {},
      top_customers: topCustomerResult.rows,
      movement_chart: chartResult.rows,
      pending_receive: pendingResult.rows,
      suppliers: supplierResult.rows,
      bill_receive: billReceiveResult.rows,
      options,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /purchase-planning/trigger-alert — manual trigger สำหรับทดสอบ (admin/SUPERADMIN เท่านั้น)
router.post('/trigger-alert', requireRole('admin', 'manager'), async (req, res) => {
  const user = req.user || {}
  const role = (user.role || '').toLowerCase()
  const code = (user.code || '').toUpperCase()
  if (role !== 'admin' && code !== 'SUPERADMIN') {
    return res.status(403).json({ error: 'เฉพาะ admin เท่านั้น' })
  }

  try {
    const { runPurchaseAlert } = require('../services/cronJobs')
    // ใช้วันที่ไทย (Asia/Bangkok) ให้ตรงกับ cron จริง ไม่ใช่ UTC
    const today = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Bangkok', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date())
    const result = await runPurchaseAlert({ todayStr: today, skipDedup: true })
    res.json({ success: true, ...result })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── Item-Supplier Linking ──────────────────────────────────────────────────

// GET /item-supplier-link/items?search=&page=&limit= — รายการสินค้า
router.get('/item-supplier-link/items', async (req, res) => {
  const search = clean(req.query.search)
  const page = Math.max(1, parseInt(req.query.page, 10) || 1)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 20))
  const offset = (page - 1) * limit

  const searchClauseResult = itemSearchClause(search, 1)
  const params = [...searchClauseResult.params]
  const whereSql = searchClauseResult.sql
  const itemWhereSql = linkableItemWhere('i', 'd')

  try {
    const [countResult, dataResult] = await Promise.all([
      posDB.query(
        `SELECT COUNT(*)::int AS total
         FROM ic_inventory i
         LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
         WHERE ${itemWhereSql}${whereSql}`,
        params,
      ),
      posDB.query(
        `SELECT i.code AS ic_code, COALESCE(i.name_1,'') AS ic_name, COALESCE(i.unit_standard,'') AS unit_code
         FROM ic_inventory i
         LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
         WHERE ${itemWhereSql}${whereSql}
         ORDER BY i.code
         OFFSET $${params.length + 1} LIMIT $${params.length + 2}`,
        [...params, offset, limit],
      ),
    ])
    res.json({ data: dataResult.rows, total: countResult.rows[0]?.total || 0, page, limit })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// GET /item-supplier-link/:icCode/linked — เจ้าหนี้ที่ผูกกับสินค้านี้แล้ว
router.get('/item-supplier-link/:icCode/linked', async (req, res) => {
  const icCode = clean(req.params.icCode)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })
  try {
    const result = await posDB.query(
      `SELECT lnk.roworder, lnk.ap_code, COALESCE(s.name_1,'') AS ap_name, lnk.remark, lnk.create_date_time_now
       FROM ap_item_by_supplier lnk
       JOIN ap_supplier s ON s.code = lnk.ap_code
       WHERE lnk.ic_code = $1::text
         AND ${activeSupplierWhere('s')}
         AND EXISTS (
           SELECT 1
           FROM ic_inventory i
           LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
           WHERE i.code = lnk.ic_code
             AND ${linkableItemWhere('i', 'd')}
         )
       ORDER BY s.name_1, lnk.ap_code`,
      [icCode],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// GET /item-supplier-link/:icCode/available?search= — เจ้าหนี้ที่ยังไม่ผูก
router.get('/item-supplier-link/:icCode/available', async (req, res) => {
  const icCode = clean(req.params.icCode)
  const search = clean(req.query.search)
  if (!icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  const params = [icCode]
  let searchSql = ''
  const keywords = search.split(/\s+/).filter(Boolean)
  for (const kw of keywords) {
    params.push(`%${kw}%`)
    const p = `$${params.length}`
    searchSql += ` AND (s.code ILIKE ${p} OR COALESCE(s.name_1,'') ILIKE ${p})`
  }

  try {
    const result = await posDB.query(
      `SELECT s.code AS ap_code, COALESCE(s.name_1,'') AS ap_name
       FROM ap_supplier s
       WHERE ${activeSupplierWhere('s')}
         AND EXISTS (
           SELECT 1
           FROM ic_inventory i
           LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
           WHERE i.code = $1::text
             AND ${linkableItemWhere('i', 'd')}
         )
         AND NOT EXISTS (
           SELECT 1 FROM ap_item_by_supplier lnk
           WHERE lnk.ic_code = $1::text AND lnk.ap_code = s.code
         )${searchSql}
       ORDER BY s.name_1, s.code
       LIMIT 100`,
      params,
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /item-supplier-link/:icCode/link — ผูกเจ้าหนี้กับสินค้า
router.post('/item-supplier-link/:icCode/link', async (req, res) => {
  const icCode = clean(req.params.icCode)
  const apCode = clean(req.body?.ap_code)
  if (!icCode || !apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้าและเจ้าหนี้' })

  try {
    const itemResult = await posDB.query(
      `SELECT 1
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       WHERE i.code = $1::text
         AND ${linkableItemWhere('i', 'd')}
       LIMIT 1`,
      [icCode],
    )
    if (!itemResult.rows.length) return res.status(400).json({ error: 'สินค้านี้ไม่สามารถผูกเจ้าหนี้ได้' })

    const supplierResult = await posDB.query(
      `SELECT 1
       FROM ap_supplier s
       WHERE s.code = $1::text
         AND ${activeSupplierWhere('s')}
       LIMIT 1`,
      [apCode],
    )
    if (!supplierResult.rows.length) return res.status(400).json({ error: 'เจ้าหนี้นี้ไม่สามารถผูกสินค้าได้' })

    const exists = await posDB.query(
      `SELECT 1 FROM ap_item_by_supplier WHERE ic_code = $1::text AND ap_code = $2::text LIMIT 1`,
      [icCode, apCode],
    )
    if (exists.rows.length) return res.status(409).json({ error: 'ผูกไว้แล้ว' })

    await posDB.query(
      `INSERT INTO ap_item_by_supplier (ap_code, ic_code, remark, status, line_number)
       VALUES ($1::text, $2::text, '', 0, 0)`,
      [apCode, icCode],
    )
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// DELETE /item-supplier-link/:icCode/link/:apCode — ยกเลิกผูก
router.delete('/item-supplier-link/:icCode/link/:apCode', async (req, res) => {
  const icCode = clean(req.params.icCode)
  const apCode = clean(req.params.apCode)
  if (!icCode || !apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้าและเจ้าหนี้' })

  try {
    const result = await posDB.query(
      `DELETE FROM ap_item_by_supplier WHERE ic_code = $1::text AND ap_code = $2::text`,
      [icCode, apCode],
    )
    if (result.rowCount === 0) return res.status(404).json({ error: 'ไม่พบรายการที่ผูก' })
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// GET /item-supplier-link/suppliers?search= — รายการเจ้าหนี้ (สำหรับ tab เลือกเจ้าหนี้ก่อน)
router.get('/item-supplier-link/suppliers', async (req, res) => {
  const search = clean(req.query.search)
  const page = Math.max(1, parseInt(req.query.page, 10) || 1)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 20))
  const offset = (page - 1) * limit

  const params = []
  let whereSql = ''
  const keywords = search.split(/\s+/).filter(Boolean)
  for (const kw of keywords) {
    params.push(`%${kw}%`)
    const p = `$${params.length}`
    whereSql += ` AND (s.code ILIKE ${p} OR COALESCE(s.name_1,'') ILIKE ${p})`
  }

  try {
    const supplierWhereSql = activeSupplierWhere('s')
    const [countResult, dataResult] = await Promise.all([
      posDB.query(
        `SELECT COUNT(*)::int AS total FROM ap_supplier s WHERE ${supplierWhereSql}${whereSql}`,
        params,
      ),
      posDB.query(
        `SELECT s.code AS ap_code, COALESCE(s.name_1,'') AS ap_name
         FROM ap_supplier s
         WHERE ${supplierWhereSql}${whereSql}
         ORDER BY s.name_1, s.code
         OFFSET $${params.length + 1} LIMIT $${params.length + 2}`,
        [...params, offset, limit],
      ),
    ])
    res.json({ data: dataResult.rows, total: countResult.rows[0]?.total || 0, page, limit })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// GET /item-supplier-link/by-supplier/:apCode/linked — สินค้าที่ผูกกับเจ้าหนี้นี้แล้ว
router.get('/item-supplier-link/by-supplier/:apCode/linked', async (req, res) => {
  const apCode = clean(req.params.apCode)
  if (!apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสเจ้าหนี้' })
  try {
    const result = await posDB.query(
      `SELECT lnk.roworder, lnk.ic_code, COALESCE(i.name_1,'') AS ic_name,
              COALESCE(i.unit_standard,'') AS unit_code, lnk.create_date_time_now
       FROM ap_item_by_supplier lnk
       JOIN ap_supplier s ON s.code = lnk.ap_code
       LEFT JOIN ic_inventory i ON i.code = lnk.ic_code
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       WHERE lnk.ap_code = $1::text
         AND ${activeSupplierWhere('s')}
         AND ${linkableItemWhere('i', 'd')}
       ORDER BY i.name_1, lnk.ic_code`,
      [apCode],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// GET /item-supplier-link/by-supplier/:apCode/available?search= — สินค้าที่ยังไม่ผูกกับเจ้าหนี้นี้
router.get('/item-supplier-link/by-supplier/:apCode/available', async (req, res) => {
  const apCode = clean(req.params.apCode)
  if (!apCode) return res.status(400).json({ error: 'กรุณาระบุรหัสเจ้าหนี้' })

  const params = [apCode]
  const searchClauseResult = itemSearchClause(clean(req.query.search), 2)
  params.push(...searchClauseResult.params)
  const searchSql = searchClauseResult.sql

  try {
    const result = await posDB.query(
      `SELECT i.code AS ic_code, COALESCE(i.name_1,'') AS ic_name, COALESCE(i.unit_standard,'') AS unit_code
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       WHERE ${linkableItemWhere('i', 'd')}
         AND EXISTS (
           SELECT 1
           FROM ap_supplier s
           WHERE s.code = $1::text
             AND ${activeSupplierWhere('s')}
         )
         AND NOT EXISTS (
           SELECT 1 FROM ap_item_by_supplier lnk
           WHERE lnk.ap_code = $1::text AND lnk.ic_code = i.code
         )${searchSql}
       ORDER BY i.name_1, i.code
       LIMIT 100`,
      params,
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /item-supplier-link/by-supplier/:apCode/link — ผูกสินค้ากับเจ้าหนี้ (reverse direction)
router.post('/item-supplier-link/by-supplier/:apCode/link', async (req, res) => {
  const apCode = clean(req.params.apCode)
  const icCode = clean(req.body?.ic_code)
  if (!apCode || !icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้าและเจ้าหนี้' })

  try {
    const itemResult = await posDB.query(
      `SELECT 1
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       WHERE i.code = $1::text
         AND ${linkableItemWhere('i', 'd')}
       LIMIT 1`,
      [icCode],
    )
    if (!itemResult.rows.length) return res.status(400).json({ error: 'สินค้านี้ไม่สามารถผูกเจ้าหนี้ได้' })

    const supplierResult = await posDB.query(
      `SELECT 1
       FROM ap_supplier s
       WHERE s.code = $1::text
         AND ${activeSupplierWhere('s')}
       LIMIT 1`,
      [apCode],
    )
    if (!supplierResult.rows.length) return res.status(400).json({ error: 'เจ้าหนี้นี้ไม่สามารถผูกสินค้าได้' })

    const exists = await posDB.query(
      `SELECT 1 FROM ap_item_by_supplier WHERE ic_code = $1::text AND ap_code = $2::text LIMIT 1`,
      [icCode, apCode],
    )
    if (exists.rows.length) return res.status(409).json({ error: 'ผูกไว้แล้ว' })

    await posDB.query(
      `INSERT INTO ap_item_by_supplier (ap_code, ic_code, remark, status, line_number)
       VALUES ($1::text, $2::text, '', 0, 0)`,
      [apCode, icCode],
    )
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// DELETE /item-supplier-link/by-supplier/:apCode/link/:icCode — ยกเลิกผูก (reverse direction)
router.delete('/item-supplier-link/by-supplier/:apCode/link/:icCode', async (req, res) => {
  const apCode = clean(req.params.apCode)
  const icCode = clean(req.params.icCode)
  if (!apCode || !icCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้าและเจ้าหนี้' })

  try {
    const result = await posDB.query(
      `DELETE FROM ap_item_by_supplier WHERE ic_code = $1::text AND ap_code = $2::text`,
      [icCode, apCode],
    )
    if (result.rowCount === 0) return res.status(404).json({ error: 'ไม่พบรายการที่ผูก' })
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.buildPlanningMetricsSql = buildPlanningMetricsSql
router.runReportJob = runReportJob

module.exports = router
