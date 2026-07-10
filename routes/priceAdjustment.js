const express = require('express')
const { posDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

const router = express.Router()

router.use(authMiddleware, requireRole('admin'))

const PRICE_FIELDS = Array.from({ length: 10 }, (_, i) => `price_${i}`)
const UNIT_MARGIN_FIELDS = [1, 2, 3, 4]
const PURCHASE_FLAGS = [12, 310]
let formulaHistorySchemaReady = false
let priceMarginSchemaReady = false

function clean(value) {
  return String(value || '').trim()
}

function searchTokenPatterns(token) {
  const raw = clean(token).toLowerCase()
  if (!raw) return []

  const variants = new Set([raw])
  const gramMatch = raw.match(/^(\d+(?:\.\d+)?)\s*g$/i)
  if (gramMatch) {
    variants.add(`${gramMatch[1]}g`)
    variants.add(`${gramMatch[1]} g`)
    variants.add(`${gramMatch[1]}กรัม`)
    variants.add(`${gramMatch[1]} กรัม`)
  }

  return [...variants].map((value) => `%${value}%`)
}

function isAnchorSearchToken(token) {
  const raw = clean(token).toLowerCase()
  if (!raw) return false
  if (/^\d+(?:\.\d+)?$/.test(raw)) return false
  if (/^\d+(?:\.\d+)?\s*(g|kg|ml|l)$/.test(raw)) return false
  return true
}

function intValue(value, fallback = 0) {
  const n = Number(value)
  return Number.isInteger(n) ? n : fallback
}

function numericValue(value) {
  const raw = String(value ?? '').replace(/,/g, '').trim()
  if (!raw) return 0
  const n = Number(raw)
  if (!Number.isFinite(n) || n < 0) return NaN
  return Math.ceil(n)
}

function toPriceText(value) {
  const n = numericValue(value)
  if (!Number.isFinite(n)) return null
  return String(n)
}

function pricesChanged(oldPrices, newPrices, oldPriceAvailable) {
  if (!oldPriceAvailable) return newPrices.some((price) => Number(price || 0) > 0)
  return PRICE_FIELDS.some((_, index) => Number(oldPrices[index] || 0) !== Number(newPrices[index] || 0))
}

function percentValue(value) {
  const raw = String(value ?? '').replace(/,/g, '').trim()
  if (!raw) return 0
  const n = Number(raw)
  return Number.isFinite(n) ? Math.round(n * 10000) / 10000 : NaN
}

function priceSelect(alias = 'f', prefix = '') {
  return PRICE_FIELDS.map((field) => `COALESCE(${alias}.${field}::text, '') AS ${prefix}${field}`).join(',\n         ')
}

function priceObject(row, prefix = '') {
  return Object.fromEntries(PRICE_FIELDS.map((field) => [field, clean(row[`${prefix}${field}`])]))
}

function otherPriceFilter(alias = 'op') {
  return `
  COALESCE(${alias}.status, 1) = 1
  AND COALESCE(${alias}.to_date, 'infinity'::date) >= CURRENT_DATE
  AND (
    COALESCE(${alias}.price_type, 1) <> 1
    OR COALESCE(${alias}.cust_code, '') <> ''
    OR COALESCE(${alias}.cust_group_1, '') <> ''
    OR COALESCE(${alias}.cust_group_2, '') <> ''
    OR COALESCE(${alias}.from_qty, 0) <> 0
    OR COALESCE(${alias}.to_qty, 0) <> 0
  )
`
}

function otherPriceSummarySelect(itemCodeExpr = 'u.item_code') {
  return `LEFT JOIN LATERAL (
         SELECT
           COUNT(*)::int AS other_price_count,
           COUNT(*) FILTER (
             WHERE COALESCE(op.price_type, 1) = 3 OR COALESCE(op.cust_code, '') <> ''
           )::int AS customer_price_count,
           COUNT(*) FILTER (
             WHERE COALESCE(op.price_type, 1) = 2
               OR COALESCE(op.cust_group_1, '') <> ''
               OR COALESCE(op.cust_group_2, '') <> ''
           )::int AS group_price_count,
           COUNT(*) FILTER (
             WHERE COALESCE(op.from_qty, 0) <> 0 OR COALESCE(op.to_qty, 0) <> 0
           )::int AS qty_price_count
         FROM ic_inventory_price op
         WHERE op.ic_code = ${itemCodeExpr}
           AND ${otherPriceFilter('op')}
       ) other_prices ON true`
}

async function ensureFormulaHistorySchema(client) {
  if (formulaHistorySchemaReady) return

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.pc_formular_doc_temp (
      roworder serial,
      doc_no character varying(255) NOT NULL,
      doc_date date DEFAULT now(),
      creator_code character varying(255),
      remark character varying(255),
      create_datetime timestamp without time zone DEFAULT now(),
      CONSTRAINT pc_formular_doc_temp_pk PRIMARY KEY (doc_no)
    );

    CREATE TABLE IF NOT EXISTS public.pc_formular_doc_detail_temp (
      roworder serial,
      doc_no character varying(25) DEFAULT ''::character varying,
      doc_date date,
      creator_code character varying(25) DEFAULT ''::character varying,
      ic_code character varying(25) NOT NULL DEFAULT ''::character varying,
      unit_code character varying(25) DEFAULT ''::character varying,
      sale_type smallint DEFAULT 0,
      tax_type smallint DEFAULT 0,
      source_doc_no character varying(255) DEFAULT ''::character varying,
      source_trans_flag smallint DEFAULT 0,
      formula_category_code character varying(25) DEFAULT ''::character varying,
      formula_category_name character varying(255) DEFAULT ''::character varying,
      old_price_available boolean DEFAULT false,
      old_price_0 numeric DEFAULT 0.0,
      old_price_1 numeric DEFAULT 0.0,
      old_price_2 numeric DEFAULT 0.0,
      old_price_3 numeric DEFAULT 0.0,
      old_price_4 numeric DEFAULT 0.0,
      old_price_5 numeric DEFAULT 0.0,
      old_price_6 numeric DEFAULT 0.0,
      old_price_7 numeric DEFAULT 0.0,
      old_price_8 numeric DEFAULT 0.0,
      old_price_9 numeric DEFAULT 0.0,
      price_0 numeric DEFAULT 0.0,
      price_1 numeric DEFAULT 0.0,
      price_2 numeric DEFAULT 0.0,
      price_3 numeric DEFAULT 0.0,
      price_4 numeric DEFAULT 0.0,
      price_5 numeric DEFAULT 0.0,
      price_6 numeric DEFAULT 0.0,
      price_7 numeric DEFAULT 0.0,
      price_8 numeric DEFAULT 0.0,
      price_9 numeric DEFAULT 0.0,
      create_date_time_now timestamp without time zone NOT NULL DEFAULT now(),
      command character varying(255),
      CONSTRAINT pc_formular_doc_detail_temp_pkey PRIMARY KEY (roworder)
    );

    CREATE TABLE IF NOT EXISTS public.pc_formular_doc_detail_temp_log (
      roworder serial,
      doc_no character varying(25) DEFAULT ''::character varying,
      doc_date date,
      creator_code character varying(25) DEFAULT ''::character varying,
      ic_code character varying(25) NOT NULL DEFAULT ''::character varying,
      unit_code character varying(25) DEFAULT ''::character varying,
      sale_type smallint DEFAULT 0,
      tax_type smallint DEFAULT 0,
      source_doc_no character varying(255) DEFAULT ''::character varying,
      source_trans_flag smallint DEFAULT 0,
      formula_category_code character varying(25) DEFAULT ''::character varying,
      formula_category_name character varying(255) DEFAULT ''::character varying,
      old_price_available boolean DEFAULT false,
      old_price_0 numeric DEFAULT 0.0,
      old_price_1 numeric DEFAULT 0.0,
      old_price_2 numeric DEFAULT 0.0,
      old_price_3 numeric DEFAULT 0.0,
      old_price_4 numeric DEFAULT 0.0,
      old_price_5 numeric DEFAULT 0.0,
      old_price_6 numeric DEFAULT 0.0,
      old_price_7 numeric DEFAULT 0.0,
      old_price_8 numeric DEFAULT 0.0,
      old_price_9 numeric DEFAULT 0.0,
      price_0 numeric DEFAULT 0.0,
      price_1 numeric DEFAULT 0.0,
      price_2 numeric DEFAULT 0.0,
      price_3 numeric DEFAULT 0.0,
      price_4 numeric DEFAULT 0.0,
      price_5 numeric DEFAULT 0.0,
      price_6 numeric DEFAULT 0.0,
      price_7 numeric DEFAULT 0.0,
      price_8 numeric DEFAULT 0.0,
      price_9 numeric DEFAULT 0.0,
      create_date_time_now timestamp without time zone NOT NULL DEFAULT now(),
      command character varying(255),
      CONSTRAINT pc_formular_doc_detail_temp_log_pkey PRIMARY KEY (roworder)
    );
  `)

  await client.query(`
    ALTER TABLE public.ic_inventory_price_formula
      ADD COLUMN IF NOT EXISTS doc_no character varying(255);
    ALTER TABLE public.ic_inventory_price_formula
      ADD COLUMN IF NOT EXISTS creator_code character varying(255);
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS old_price_available boolean DEFAULT false;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS old_price_available boolean DEFAULT false;
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS source_doc_no character varying(255) DEFAULT ''::character varying;
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS source_trans_flag smallint DEFAULT 0;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS source_doc_no character varying(255) DEFAULT ''::character varying;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS source_trans_flag smallint DEFAULT 0;
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS formula_category_code character varying(25) DEFAULT ''::character varying;
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS formula_category_name character varying(255) DEFAULT ''::character varying;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS formula_category_code character varying(25) DEFAULT ''::character varying;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS formula_category_name character varying(255) DEFAULT ''::character varying;
    ${PRICE_FIELDS.map((field) => `
    ALTER TABLE public.pc_formular_doc_detail_temp
      ADD COLUMN IF NOT EXISTS old_${field} numeric DEFAULT 0.0;
    ALTER TABLE public.pc_formular_doc_detail_temp_log
      ADD COLUMN IF NOT EXISTS old_${field} numeric DEFAULT 0.0;`).join('')}

    CREATE INDEX IF NOT EXISTS idx_pc_formula_log_source_doc_item_unit
      ON public.pc_formular_doc_detail_temp_log (source_doc_no, source_trans_flag, ic_code, unit_code)
      WHERE COALESCE(source_doc_no, '') <> '';
  `)

  formulaHistorySchemaReady = true
}

async function ensurePriceMarginSchema(client) {
  if (priceMarginSchemaReady) return

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.crm_price_margin_category (
      category_code character varying(25) PRIMARY KEY,
      category_name character varying(255) NOT NULL DEFAULT '',
      price_0_margin numeric NOT NULL DEFAULT 0,
      price_1_margin numeric NOT NULL DEFAULT 0,
      price_2_margin numeric NOT NULL DEFAULT 0,
      price_3_margin numeric NOT NULL DEFAULT 0,
      price_4_margin numeric NOT NULL DEFAULT 0,
      price_5_margin numeric NOT NULL DEFAULT 0,
      price_6_margin numeric NOT NULL DEFAULT 0,
      price_7_margin numeric NOT NULL DEFAULT 0,
      price_8_margin numeric NOT NULL DEFAULT 0,
      price_9_margin numeric NOT NULL DEFAULT 0,
      unit_1_margin numeric NOT NULL DEFAULT 0,
      unit_2_margin numeric NOT NULL DEFAULT 0,
      unit_3_margin numeric NOT NULL DEFAULT 0,
      unit_4_margin numeric NOT NULL DEFAULT 0,
      create_datetime timestamp without time zone NOT NULL DEFAULT now(),
      last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
      create_code character varying(50) NOT NULL DEFAULT '',
      last_update_code character varying(50) NOT NULL DEFAULT ''
    );
  `)

  priceMarginSchemaReady = true
}

function marginSelect(alias = 'm') {
  return [
    ...PRICE_FIELDS.map((field) => `COALESCE(${alias}.${field}_margin, 0) AS ${field}_margin`),
    ...UNIT_MARGIN_FIELDS.map((field) => `COALESCE(${alias}.unit_${field}_margin, 0) AS unit_${field}_margin`),
  ].join(',\n         ')
}

router.get('/margin-master', async (req, res) => {
  const rawCodes = clean(req.query.category_codes)
  const categoryCodes = rawCodes
    ? rawCodes.split(',').map(clean).filter(Boolean).slice(0, 500)
    : []

  const client = await posDB.connect()
  try {
    await ensurePriceMarginSchema(client)

    const params = []
    const where = ['COALESCE(c.code, \'\') <> \'\'']
    if (categoryCodes.length) {
      params.push(categoryCodes)
      where.push(`c.code = ANY($${params.length}::text[])`)
    }

    const result = await client.query(
      `SELECT
         c.code AS category_code,
         COALESCE(c.name_1, '') AS category_name,
         ${marginSelect('m')}
       FROM ic_category c
       LEFT JOIN crm_price_margin_category m ON m.category_code = c.code
       WHERE ${where.join(' AND ')}
       ORDER BY c.code`,
      params,
    )

    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

router.put('/margin-master', async (req, res) => {
  const items = Array.isArray(req.body.items) ? req.body.items : []
  if (!items.length) return res.status(400).json({ error: 'ไม่มีรายการ master margin สำหรับบันทึก' })
  if (items.length > 2000) return res.status(400).json({ error: 'บันทึก master margin ได้ไม่เกิน 2,000 หมวดต่อครั้ง' })

  const normalized = []
  for (const item of items) {
    const categoryCode = clean(item.category_code)
    if (!categoryCode) return res.status(400).json({ error: 'พบรายการ master margin ที่ไม่มีรหัสหมวดสินค้า' })

    const priceMargins = PRICE_FIELDS.map((field) => percentValue(item[`${field}_margin`]))
    const unitMargins = UNIT_MARGIN_FIELDS.map((field) => percentValue(item[`unit_${field}_margin`]))
    if ([...priceMargins, ...unitMargins].some((n) => !Number.isFinite(n))) {
      return res.status(400).json({ error: `margin ของหมวด ${categoryCode} ต้องเป็นตัวเลข` })
    }

    normalized.push({
      categoryCode,
      categoryName: clean(item.category_name).slice(0, 255),
      priceMargins,
      unitMargins,
    })
  }

  const client = await posDB.connect()
  try {
    await client.query('BEGIN')
    await ensurePriceMarginSchema(client)

    for (const item of normalized) {
      await client.query(
        `INSERT INTO crm_price_margin_category (
           category_code,
           category_name,
           ${PRICE_FIELDS.map((field) => `${field}_margin`).join(', ')},
           ${UNIT_MARGIN_FIELDS.map((field) => `unit_${field}_margin`).join(', ')},
           create_code,
           last_update_code
         )
         VALUES (
           $1, $2,
           ${PRICE_FIELDS.map((_, index) => `$${index + 3}`).join(', ')},
           ${UNIT_MARGIN_FIELDS.map((_, index) => `$${index + 13}`).join(', ')},
           $17, $17
         )
         ON CONFLICT (category_code) DO UPDATE SET
           category_name = EXCLUDED.category_name,
           ${PRICE_FIELDS.map((field) => `${field}_margin = EXCLUDED.${field}_margin`).join(', ')},
           ${UNIT_MARGIN_FIELDS.map((field) => `unit_${field}_margin = EXCLUDED.unit_${field}_margin`).join(', ')},
           last_update_date_time = now(),
           last_update_code = EXCLUDED.last_update_code`,
        [
          item.categoryCode,
          item.categoryName,
          ...item.priceMargins,
          ...item.unitMargins,
          clean(req.user?.code),
        ],
      )
    }

    await client.query('COMMIT')
    res.json({ success: true, saved_count: normalized.length })
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

router.get('/documents', async (req, res) => {
  const fromDate = clean(req.query.from_date)
  const toDate = clean(req.query.to_date)
  const docNo = clean(req.query.doc_no)
  const docNoFrom = clean(req.query.doc_no_from)
  const docNoTo = clean(req.query.doc_no_to)
  const limit = Math.min(500, Math.max(1, intValue(req.query.limit, 200)))

  const params = []
  const where = [
    't.trans_flag IN (12, 310)',
    'COALESCE(t.last_status, 0) = 0',
  ]

  if (fromDate) {
    params.push(fromDate)
    where.push(`t.doc_date >= $${params.length}::date`)
  }
  if (toDate) {
    params.push(toDate)
    where.push(`t.doc_date <= $${params.length}::date`)
  }
  if (docNo) {
    params.push(`%${docNo}%`)
    where.push(`t.doc_no ILIKE $${params.length}`)
  }
  if (docNoFrom) {
    params.push(docNoFrom)
    where.push(`t.doc_no >= $${params.length}`)
  }
  if (docNoTo) {
    params.push(docNoTo)
    where.push(`t.doc_no <= $${params.length}`)
  }
  params.push(limit)

  try {
    await ensureFormulaHistorySchema(posDB)
    const result = await posDB.query(
      `WITH filtered_docs AS (
         SELECT t.*
         FROM ic_trans t
         WHERE ${where.join(' AND ')}
         ORDER BY t.doc_date DESC, t.doc_time DESC, t.doc_no DESC
         LIMIT $${params.length}
       ),
       line_stats AS (
         SELECT
           d.doc_no,
           d.trans_flag,
           COUNT(*) AS line_count,
           COUNT(DISTINCT d.item_code) AS item_count,
           COUNT(DISTINCT COALESCE(d.item_code, '') || E'\\x1F' || COALESCE(d.unit_code, '')) AS item_unit_count,
           MIN(COALESCE(d.vat_type, 0)) AS vat_type,
           COUNT(DISTINCT COALESCE(d.vat_type, 0)) AS vat_type_count
         FROM ic_trans_detail d
         JOIN filtered_docs t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
         WHERE COALESCE(d.status, 0) = 0
           AND COALESCE(d.last_status, 0) = 0
         GROUP BY d.doc_no, d.trans_flag
       ),
       doc_items AS (
         SELECT DISTINCT
           d.doc_no,
           d.trans_flag,
           COALESCE(d.item_code, '') AS item_code,
           COALESCE(d.unit_code, '') AS unit_code
         FROM ic_trans_detail d
         JOIN filtered_docs t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
         WHERE COALESCE(d.status, 0) = 0
           AND COALESCE(d.last_status, 0) = 0
           AND COALESCE(d.item_code, '') <> ''
           AND COALESCE(d.unit_code, '') <> ''
       ),
       adjust_stats AS (
         SELECT
           di.doc_no,
           di.trans_flag,
           COUNT(*)::int AS adjusted_item_unit_count
         FROM doc_items di
         WHERE EXISTS (
           SELECT 1
           FROM pc_formular_doc_detail_temp_log h
           WHERE COALESCE(h.source_doc_no, '') <> ''
             AND h.source_doc_no = di.doc_no
             AND h.source_trans_flag = di.trans_flag
             AND h.ic_code = di.item_code
             AND h.unit_code = di.unit_code
           LIMIT 1
         )
         GROUP BY di.doc_no, di.trans_flag
       )
       SELECT
         t.doc_no,
         TO_CHAR(t.doc_date::date, 'YYYY-MM-DD') AS doc_date,
         t.trans_flag,
         CASE t.trans_flag WHEN 12 THEN 'ซื้อ' WHEN 310 THEN 'รับสินค้า' ELSE t.trans_flag::text END AS trans_name,
         COALESCE(t.cust_code, '') AS supplier_code,
         COALESCE(ap.name_1, '') AS supplier_name,
         COALESCE(t.total_amount, 0) AS total_amount,
         COALESCE(t.creator_code, '') AS creator_code,
         COALESCE(t.remark, '') AS remark,
         COALESCE(line_stats.line_count, 0)::int AS line_count,
         COALESCE(line_stats.item_count, 0)::int AS item_count,
         COALESCE(line_stats.item_unit_count, 0)::int AS item_unit_count,
         COALESCE(adjust_stats.adjusted_item_unit_count, 0)::int AS adjusted_item_unit_count,
         CASE
           WHEN COALESCE(line_stats.item_unit_count, 0) = 0 THEN 'none'
           WHEN COALESCE(adjust_stats.adjusted_item_unit_count, 0) >= COALESCE(line_stats.item_unit_count, 0) THEN 'full'
           WHEN COALESCE(adjust_stats.adjusted_item_unit_count, 0) > 0 THEN 'partial'
           ELSE 'none'
         END AS price_adjust_status,
         COALESCE(line_stats.vat_type, 0)::int AS vat_type,
         COALESCE(line_stats.vat_type_count, 0)::int AS vat_type_count
       FROM filtered_docs t
       LEFT JOIN ap_supplier ap ON ap.code = t.cust_code
       LEFT JOIN line_stats ON line_stats.doc_no = t.doc_no AND line_stats.trans_flag = t.trans_flag
       LEFT JOIN adjust_stats ON adjust_stats.doc_no = t.doc_no AND adjust_stats.trans_flag = t.trans_flag
       ORDER BY t.doc_date DESC, t.doc_time DESC, t.doc_no DESC`,
      params,
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/items-from-documents', async (req, res) => {
  const rawDocs = Array.isArray(req.body.documents) ? req.body.documents : []

  const docs = rawDocs
    .map((doc) => ({
      doc_no: clean(doc.doc_no),
      trans_flag: intValue(doc.trans_flag, 0),
    }))
    .filter((doc) => doc.doc_no && PURCHASE_FLAGS.includes(doc.trans_flag))

  if (!docs.length) return res.status(400).json({ error: 'กรุณาเลือกเอกสารอย่างน้อย 1 ใบ' })
  if (docs.length > 200) return res.status(400).json({ error: 'เลือกเอกสารได้ไม่เกิน 200 ใบต่อครั้ง' })

  const params = []
  const values = docs.map((doc) => {
    params.push(doc.doc_no, doc.trans_flag)
    return `($${params.length - 1}::text, $${params.length}::smallint)`
  }).join(', ')

  try {
    const result = await posDB.query(
      `WITH selected(doc_no, trans_flag) AS (
         VALUES ${values}
       ),
       vat_option AS (
         SELECT COALESCE((SELECT vat_rate FROM erp_option LIMIT 1), 0)::numeric AS vat_rate
       ),
       detail_rows AS (
         SELECT
           d.roworder,
           d.doc_no,
           d.trans_flag,
           d.doc_date::date AS doc_date,
           d.item_code,
           COALESCE(d.item_name, i.name_1, '') AS item_name,
           d.unit_code AS source_unit_code,
           COALESCE(du.ratio, d.ratio, 1) AS source_unit_ratio,
           COALESCE(i.item_category, '') AS category_code,
           COALESCE(cat.name_1, '') AS category_name,
           COALESCE(i.group_main, '') AS group_main,
           COALESCE(d.vat_type, 0) AS vat_type,
           COALESCE(d.tax_type, 0) AS item_tax_type,
           COALESCE(d.is_permium, 0) AS is_permium,
           CASE
             WHEN COALESCE(d.vat_type, 0) = 0 AND COALESCE(d.tax_type, 0) = 0
               THEN ROUND(COALESCE(d.price, 0) * (1 + (v.vat_rate / 100)), 6)
             ELSE COALESCE(d.price, 0)
           END AS purchase_price,
           CASE
             WHEN COALESCE(d.tax_type, 0) = 1 THEN COALESCE(d.price, 0)
             WHEN COALESCE(d.vat_type, 0) = 0 THEN COALESCE(d.price, 0)
             WHEN COALESCE(d.vat_type, 0) = 1 THEN ROUND(COALESCE(d.price, 0) / NULLIF((1 + (v.vat_rate / 100)), 0), 6)
             ELSE COALESCE(d.price, 0)
           END AS purchase_price_before_vat,
           CASE
             WHEN COALESCE(d.tax_type, 0) = 1 THEN 0
             WHEN COALESCE(d.vat_type, 0) = 0 THEN ROUND(COALESCE(d.price, 0) * (v.vat_rate / 100), 6)
             WHEN COALESCE(d.vat_type, 0) = 1 THEN ROUND(COALESCE(d.price, 0) - (COALESCE(d.price, 0) / NULLIF((1 + (v.vat_rate / 100)), 0)), 6)
             ELSE 0
           END AS purchase_price_vat_amount,
           COALESCE(d.price_exclude_vat, 0) AS price_exclude_vat,
           CASE
             WHEN COALESCE(d.vat_type, 0) = 0 AND COALESCE(d.tax_type, 0) = 0
               THEN ROUND(COALESCE(d.price, 0) * (1 + (v.vat_rate / 100)), 6)
             ELSE COALESCE(d.price, 0)
           END AS price_include_vat,
           COALESCE(d.qty, 0) AS qty
         FROM selected s
         CROSS JOIN vat_option v
         JOIN ic_trans t ON t.doc_no = s.doc_no AND t.trans_flag = s.trans_flag
         JOIN ic_trans_detail d ON d.doc_no = t.doc_no AND d.trans_flag = t.trans_flag
         LEFT JOIN ic_inventory i ON i.code = d.item_code
         LEFT JOIN ic_category cat ON cat.code = i.item_category
         LEFT JOIN ic_unit_use du ON du.ic_code = d.item_code AND du.code = d.unit_code
         WHERE t.trans_flag IN (12, 310)
           AND COALESCE(t.last_status, 0) = 0
           AND COALESCE(d.status, 0) = 0
           AND COALESCE(d.last_status, 0) = 0
           AND COALESCE(d.item_code, '') <> ''
           AND COALESCE(d.unit_code, '') <> ''
       ),
       stock_costs AS (
         SELECT
           sb.ic_code AS item_code,
           MAX(COALESCE(sb.average_cost_end, 0)) AS base_average_cost
         FROM (SELECT DISTINCT item_code FROM detail_rows) items
         CROSS JOIN LATERAL sml_ic_function_stock_balance_warehouse_location(
           CURRENT_DATE,
           items.item_code::varchar,
           'MMA01'::varchar,
           ''::varchar
         ) sb
         GROUP BY sb.ic_code
       ),
       unit_rows AS (
         SELECT
           u.ic_code AS item_code,
           u.code AS unit_code,
           COALESCE(u.row_order, 0) AS unit_row_order,
           COALESCE(u.ratio, 1) AS unit_ratio,
           COALESCE(u.stand_value, 1) AS stand_value,
           COALESCE(u.divide_value, 1) AS divide_value
         FROM ic_unit_use u
         JOIN (SELECT DISTINCT item_code FROM detail_rows) i ON i.item_code = u.ic_code
         WHERE COALESCE(u.code, '') <> ''
         UNION
         SELECT DISTINCT
           item_code,
           source_unit_code AS unit_code,
           0 AS unit_row_order,
           COALESCE(source_unit_ratio, 1) AS unit_ratio,
           1 AS stand_value,
           1 AS divide_value
         FROM detail_rows d
         WHERE NOT EXISTS (
           SELECT 1
           FROM ic_unit_use u
           WHERE u.ic_code = d.item_code
             AND u.code = d.source_unit_code
         )
       ),
       ranked_by_unit AS (
         SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY item_code, source_unit_code
             ORDER BY purchase_price DESC, doc_date DESC, doc_no DESC, roworder DESC
           ) AS rn
         FROM detail_rows
       ),
       ranked_by_item AS (
         SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY item_code
             ORDER BY purchase_price DESC, doc_date DESC, doc_no DESC, roworder DESC
           ) AS rn
         FROM detail_rows
       ),
       sources AS (
         SELECT
           item_code,
           COUNT(*)::int AS source_line_count,
           COUNT(DISTINCT doc_no)::int AS source_doc_count,
           STRING_AGG(
             DISTINCT doc_no || ' (' ||
               CASE COALESCE(vat_type, 0)
                 WHEN 0 THEN 'แยกนอก'
                 WHEN 1 THEN 'รวมใน'
                 WHEN 2 THEN 'ภาษีศูนย์'
                 WHEN 3 THEN 'ไม่กระทบภาษี'
                 ELSE 'ไม่ทราบ'
               END || ')',
             ', '
           ) AS source_docs
         FROM detail_rows
         GROUP BY item_code
       )
       SELECT
         u.item_code,
         COALESCE(ru.item_name, ri.item_name, '') AS item_name,
         u.unit_code,
         COALESCE(ru.category_code, ri.category_code, '') AS category_code,
         COALESCE(ru.category_name, ri.category_name, '') AS category_name,
         COALESCE(ru.group_main, ri.group_main, '') AS group_main,
         COALESCE(barcode_info.barcode, '') AS barcode,
         COALESCE(barcode_info.description, '') AS barcode_description,
         0::int AS vat_type,
         COALESCE(ru.vat_type, ri.vat_type, 0) AS source_vat_type,
         COALESCE(ru.item_tax_type, ri.item_tax_type, 0) AS item_tax_type,
         COALESCE(ru.is_permium, ri.is_permium, 0) AS is_permium,
         u.unit_row_order,
         u.unit_ratio,
         u.stand_value,
         u.divide_value,
         CASE
           WHEN ru.item_code IS NOT NULL THEN ru.purchase_price
           ELSE ROUND(COALESCE(ri.purchase_price, 0) * COALESCE(u.unit_ratio, 1) / NULLIF(COALESCE(ri.source_unit_ratio, 1), 0), 6)
         END AS purchase_price,
         CASE
           WHEN ru.item_code IS NOT NULL THEN ru.purchase_price_before_vat
           ELSE ROUND(COALESCE(ri.purchase_price_before_vat, 0) * COALESCE(u.unit_ratio, 1) / NULLIF(COALESCE(ri.source_unit_ratio, 1), 0), 6)
         END AS purchase_price_before_vat,
         CASE
           WHEN ru.item_code IS NOT NULL THEN ru.purchase_price_vat_amount
           ELSE ROUND(COALESCE(ri.purchase_price_vat_amount, 0) * COALESCE(u.unit_ratio, 1) / NULLIF(COALESCE(ri.source_unit_ratio, 1), 0), 6)
         END AS purchase_price_vat_amount,
         ROUND(
           COALESCE(sc.base_average_cost, 0)
           * COALESCE(u.unit_ratio, 1)
           * CASE
               WHEN COALESCE(ru.item_tax_type, ri.item_tax_type, 0) <> 1
                 THEN (1 + (v.vat_rate / 100))
               ELSE 1
             END,
           6
         ) AS average_cost,
         ROUND(
           COALESCE(sc.base_average_cost, 0)
           * COALESCE(u.unit_ratio, 1),
           6
         ) AS average_cost_before_vat,
         ROUND(
           COALESCE(sc.base_average_cost, 0)
           * COALESCE(u.unit_ratio, 1)
           * CASE
               WHEN COALESCE(ru.item_tax_type, ri.item_tax_type, 0) <> 1
                 THEN (v.vat_rate / 100)
               ELSE 0
             END,
           6
         ) AS average_cost_vat_amount,
         CASE
           WHEN ru.item_code IS NOT NULL THEN ru.price_exclude_vat
           ELSE ROUND(COALESCE(ri.price_exclude_vat, 0) * COALESCE(u.unit_ratio, 1) / NULLIF(COALESCE(ri.source_unit_ratio, 1), 0), 6)
         END AS price_exclude_vat,
         CASE
           WHEN ru.item_code IS NOT NULL THEN ru.price_include_vat
           ELSE ROUND(COALESCE(ri.price_include_vat, 0) * COALESCE(u.unit_ratio, 1) / NULLIF(COALESCE(ri.source_unit_ratio, 1), 0), 6)
         END AS price_include_vat,
         COALESCE(ru.qty, ri.qty, 0) AS qty,
         COALESCE(ru.doc_no, ri.doc_no, '') AS source_doc_no,
         COALESCE(ru.source_unit_code, ri.source_unit_code, '') AS source_unit_code,
         COALESCE(ru.source_unit_ratio, ri.source_unit_ratio, 1) AS source_unit_ratio,
         COALESCE(ru.trans_flag, ri.trans_flag, 0) AS source_trans_flag,
         TO_CHAR(COALESCE(ru.doc_date, ri.doc_date), 'YYYY-MM-DD') AS source_doc_date,
         COALESCE(other_prices.other_price_count, 0) AS other_price_count,
         COALESCE(other_prices.customer_price_count, 0) AS customer_price_count,
         COALESCE(other_prices.group_price_count, 0) AS group_price_count,
         COALESCE(other_prices.qty_price_count, 0) AS qty_price_count,
         s.source_docs,
         s.source_doc_count,
         s.source_line_count,
         COALESCE(TO_CHAR(f.create_date_time_now, 'YYYY-MM-DD HH24:MI:SS'), '') AS latest_price_update_at,
         ${priceSelect('f', 'old_')}
       FROM unit_rows u
       CROSS JOIN vat_option v
       JOIN sources s ON s.item_code = u.item_code
       LEFT JOIN ranked_by_unit ru ON ru.item_code = u.item_code AND ru.source_unit_code = u.unit_code AND ru.rn = 1
       LEFT JOIN ranked_by_item ri ON ri.item_code = u.item_code AND ri.rn = 1
       LEFT JOIN stock_costs sc ON sc.item_code = u.item_code
       LEFT JOIN ic_inventory_price_formula f
         ON f.ic_code = u.item_code
       AND f.unit_code = u.unit_code
       AND f.sale_type = 0::smallint
       AND f.tax_type = 0::smallint
       LEFT JOIN LATERAL (
         SELECT ib.barcode, COALESCE(ib.description, '') AS description
         FROM ic_inventory_barcode ib
         WHERE ib.ic_code = u.item_code
           AND ib.unit_code = u.unit_code
           AND COALESCE(ib.barcode, '') <> ''
         ORDER BY COALESCE(ib.roworder, 0), ib.barcode
         LIMIT 1
       ) barcode_info ON true
       ${otherPriceSummarySelect('u.item_code')}
       ORDER BY u.item_code, u.unit_row_order, u.unit_code`,
      params,
    )

    const data = result.rows.map((row) => {
      const oldPrices = {}
      const newPrices = {}
      for (const field of PRICE_FIELDS) {
        const oldValue = clean(row[`old_${field}`])
        oldPrices[field] = oldValue
        newPrices[field] = oldValue || String(Math.ceil(Number(row.purchase_price || 0)))
      }
      return {
        ...row,
        old_prices: oldPrices,
        new_prices: newPrices,
      }
    })

    res.json({ data, count: data.length })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/formula-products', async (req, res) => {
  const query = clean(req.query.q)
  const categoryCode = clean(req.query.category_code)
  const tokens = query.split(/\s+/).map(clean).filter(Boolean).slice(0, 8)

  const params = []
  const where = [
    `COALESCE(f.ic_code, '') <> ''`,
    `COALESCE(f.unit_code, '') <> ''`,
    `COALESCE(f.sale_type, 0) = 0`,
  ]
  const tokenMatches = []
  const anchorMatches = []
  for (const token of tokens) {
    const patterns = searchTokenPatterns(token)
    if (!patterns.length) continue
    params.push(patterns)
    const match = `(
      lower(COALESCE(f.ic_code, '')) LIKE ANY($${params.length}::text[])
      OR lower(COALESCE(i.name_1, '')) LIKE ANY($${params.length}::text[])
    )`
    tokenMatches.push(match)
    if (isAnchorSearchToken(token)) anchorMatches.push(match)
  }
  if (anchorMatches.length) where.push(`(${anchorMatches.join(' OR ')})`)
  else if (tokenMatches.length) where.push(`(${tokenMatches.join(' OR ')})`)
  if (categoryCode) {
    params.push(categoryCode)
    where.push(`COALESCE(i.item_category, '') = $${params.length}`)
  }
  const matchScore = tokenMatches.length
    ? tokenMatches.map((match) => `CASE WHEN ${match} THEN 1 ELSE 0 END`).join(' + ')
    : '0'

  try {
    const result = await posDB.query(
      `SELECT
         f.ic_code AS item_code,
         COALESCE(i.name_1, '') AS item_name,
         COALESCE(i.item_category, '') AS category_code,
         COALESCE(cat.name_1, '') AS category_name,
         COALESCE(i.group_main, '') AS group_main,
         COUNT(*)::int AS formula_count,
         COUNT(DISTINCT f.unit_code)::int AS unit_count,
         STRING_AGG(DISTINCT f.unit_code, ', ' ORDER BY f.unit_code) AS unit_codes,
         (${matchScore})::int AS match_score
       FROM ic_inventory_price_formula f
       LEFT JOIN ic_inventory i ON i.code = f.ic_code
       LEFT JOIN ic_category cat ON cat.code = i.item_category
       WHERE ${where.join(' AND ')}
       GROUP BY f.ic_code, i.name_1, i.item_category, cat.name_1, i.group_main
       ORDER BY match_score DESC, f.ic_code limit 3000`,
      params,
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/items-from-products', async (req, res) => {
  const rawProducts = Array.isArray(req.body.products) ? req.body.products : []
  const productCodes = [...new Set(rawProducts
    .map((item) => clean(item.item_code || item.code || item))
    .filter(Boolean))]

  if (!productCodes.length) return res.status(400).json({ error: 'กรุณาเลือกสินค้าอย่างน้อย 1 รายการ' })

  const params = [productCodes]
  try {
    const result = await posDB.query(
      `WITH selected AS (
         SELECT unnest($1::text[]) AS item_code
       ),
       vat_option AS (
         SELECT COALESCE((SELECT vat_rate FROM erp_option LIMIT 1), 0)::numeric AS vat_rate
       ),
       stock_costs AS (
         SELECT
           sb.ic_code AS item_code,
           MAX(COALESCE(sb.average_cost_end, 0)) AS base_average_cost
         FROM selected s
         CROSS JOIN LATERAL sml_ic_function_stock_balance_warehouse_location(
           now()::date,
           s.item_code::varchar,
           'MMA01'::varchar,
           ''::varchar
         ) sb
         GROUP BY sb.ic_code
       )
       SELECT
         f.ic_code AS item_code,
         COALESCE(i.name_1, '') AS item_name,
         f.unit_code,
         COALESCE(i.item_category, '') AS category_code,
         COALESCE(cat.name_1, '') AS category_name,
         COALESCE(i.group_main, '') AS group_main,
         COALESCE(barcode_info.barcode, '') AS barcode,
         COALESCE(barcode_info.description, '') AS barcode_description,
         0::int AS vat_type,
         COALESCE(i.tax_type, 0) AS item_tax_type,
         COALESCE(detail_info.is_premium, 0) AS is_permium,
         COALESCE(u.row_order, 0) AS unit_row_order,
         COALESCE(u.ratio, 1) AS unit_ratio,
         COALESCE(u.stand_value, 1) AS stand_value,
         COALESCE(u.divide_value, 1) AS divide_value,
         0::numeric AS purchase_price,
         0::numeric AS purchase_price_before_vat,
         0::numeric AS purchase_price_vat_amount,
         ROUND(
           COALESCE(sc.base_average_cost, 0)
           * COALESCE(u.ratio, 1)
           * CASE
               WHEN COALESCE(i.tax_type, 0) <> 1 THEN (1 + (v.vat_rate / 100))
               ELSE 1
             END,
           6
         ) AS average_cost,
         ROUND(COALESCE(sc.base_average_cost, 0) * COALESCE(u.ratio, 1), 6) AS average_cost_before_vat,
         ROUND(
           COALESCE(sc.base_average_cost, 0)
           * COALESCE(u.ratio, 1)
           * CASE
               WHEN COALESCE(i.tax_type, 0) <> 1 THEN (v.vat_rate / 100)
               ELSE 0
             END,
           6
         ) AS average_cost_vat_amount,
         0::numeric AS price_exclude_vat,
         0::numeric AS price_include_vat,
         0::numeric AS qty,
         '' AS source_doc_no,
         f.unit_code AS source_unit_code,
         COALESCE(u.ratio, 1) AS source_unit_ratio,
         0 AS source_trans_flag,
         NULL::date AS source_doc_date,
         'ราคาตามสูตร' AS source_docs,
         0::int AS source_doc_count,
         COUNT(*) OVER (PARTITION BY f.ic_code)::int AS source_line_count,
         COALESCE(other_prices.other_price_count, 0) AS other_price_count,
         COALESCE(other_prices.customer_price_count, 0) AS customer_price_count,
         COALESCE(other_prices.group_price_count, 0) AS group_price_count,
         COALESCE(other_prices.qty_price_count, 0) AS qty_price_count,
         COALESCE(TO_CHAR(f.create_date_time_now, 'YYYY-MM-DD HH24:MI:SS'), '') AS latest_price_update_at,
         ${priceSelect('f', 'old_')}
       FROM selected s
       JOIN ic_inventory_price_formula f ON f.ic_code = s.item_code
       CROSS JOIN vat_option v
       LEFT JOIN ic_inventory i ON i.code = f.ic_code
       LEFT JOIN ic_category cat ON cat.code = i.item_category
       LEFT JOIN ic_unit_use u ON u.ic_code = f.ic_code AND u.code = f.unit_code
       LEFT JOIN stock_costs sc ON sc.item_code = f.ic_code
       LEFT JOIN LATERAL (
         SELECT COALESCE(d.is_premium, 0) AS is_premium
         FROM ic_inventory_detail d
         WHERE d.ic_code = f.ic_code
         LIMIT 1
       ) detail_info ON true
       LEFT JOIN LATERAL (
         SELECT ib.barcode, COALESCE(ib.description, '') AS description
         FROM ic_inventory_barcode ib
         WHERE ib.ic_code = f.ic_code
           AND ib.unit_code = f.unit_code
           AND COALESCE(ib.barcode, '') <> ''
         ORDER BY COALESCE(ib.roworder, 0), ib.barcode
         LIMIT 1
       ) barcode_info ON true
       ${otherPriceSummarySelect('f.ic_code')}
       WHERE COALESCE(f.sale_type, 0) = 0
         AND COALESCE(f.tax_type, 0) = 0
         AND COALESCE(f.ic_code, '') <> ''
         AND COALESCE(f.unit_code, '') <> ''
       ORDER BY f.ic_code, COALESCE(u.row_order, 0), f.unit_code`,
      params,
    )

    const data = result.rows.map((row) => {
      const oldPrices = {}
      const newPrices = {}
      for (const field of PRICE_FIELDS) {
        const oldValue = clean(row[`old_${field}`])
        oldPrices[field] = oldValue
        newPrices[field] = oldValue || '0'
      }
      return {
        ...row,
        old_prices: oldPrices,
        new_prices: newPrices,
      }
    })

    res.json({ data, count: data.length })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/other-prices', async (req, res) => {
  const itemCode = clean(req.query.item_code)
  const unitCode = clean(req.query.unit_code)
  if (!itemCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  const params = [itemCode]
  const unitOrder = unitCode ? 'CASE WHEN ip.unit_code = $2 THEN 0 ELSE 1 END,' : ''
  if (unitCode) params.push(unitCode)

  try {
    const result = await posDB.query(
      `SELECT
         ip.roworder,
         ip.ic_code AS item_code,
         COALESCE(i.name_1, '') AS item_name,
         ip.unit_code,
         COALESCE(ip.price_mode, 0) AS price_mode,
         COALESCE(ip.price_type, 0) AS price_type,
         COALESCE(ip.sale_type, 0) AS sale_type,
         COALESCE(ip.transport_type, 0) AS transport_type,
         COALESCE(ip.from_qty, 0) AS from_qty,
         COALESCE(ip.to_qty, 0) AS to_qty,
         TO_CHAR(ip.from_date::date, 'YYYY-MM-DD') AS from_date,
         TO_CHAR(ip.to_date::date, 'YYYY-MM-DD') AS to_date,
         COALESCE(ip.sale_price1, 0) AS sale_price1,
         COALESCE(ip.sale_price2, 0) AS sale_price2,
         COALESCE(ip.cust_code, '') AS cust_code,
         COALESCE(ar.name_1, '') AS cust_name,
         COALESCE(ip.cust_group_1, '') AS cust_group_1,
         COALESCE(ip.cust_group_2, '') AS cust_group_2,
         COALESCE(ip.status, 0) AS status,
         COALESCE(ip.creator_code, '') AS creator_code,
         COALESCE(ip.doc_no, '') AS doc_no,
         COALESCE(TO_CHAR(ip.create_date_time_now, 'YYYY-MM-DD'), '') AS doc_date,
         COALESCE(TO_CHAR(ip.create_date_time_now, 'HH24:MI'), '') AS doc_time
       FROM ic_inventory_price ip
       LEFT JOIN ic_inventory i ON i.code = ip.ic_code
       LEFT JOIN ar_customer ar ON ar.code = ip.cust_code
       WHERE ip.ic_code = $1
         AND ${otherPriceFilter('ip')}
       ORDER BY ${unitOrder} ip.price_type, ip.unit_code, ip.from_qty, ip.to_qty, ip.create_date_time_now DESC
       LIMIT 500`,
      params,
    )
    res.json({ data: result.rows, count: result.rowCount })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/history', async (req, res) => {
  const limit = Math.min(Math.max(intValue(req.query.limit, 50), 1), 200)
  const client = await posDB.connect()

  try {
    await ensureFormulaHistorySchema(client)
    const result = await client.query(
      `SELECT
         h.doc_no,
         TO_CHAR(h.doc_date::date, 'YYYY-MM-DD') AS doc_date,
         h.creator_code,
         h.remark,
         h.create_datetime,
         COUNT(d.roworder)::int AS line_count,
         COUNT(*) FILTER (WHERE d.command = 'Insert')::int AS insert_count,
         COUNT(*) FILTER (WHERE d.command = 'Update')::int AS update_count
       FROM pc_formular_doc_temp h
       LEFT JOIN pc_formular_doc_detail_temp_log d ON d.doc_no = h.doc_no
       GROUP BY h.doc_no, h.doc_date, h.creator_code, h.remark, h.create_datetime
       ORDER BY h.create_datetime DESC, h.doc_no DESC
       LIMIT $1`,
      [limit],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

router.get('/history/:docNo/details', async (req, res) => {
  const docNo = clean(req.params.docNo)
  if (!docNo) return res.status(400).json({ error: 'กรุณาระบุเลขที่เอกสาร' })

  const client = await posDB.connect()
  try {
    await ensureFormulaHistorySchema(client)
    const result = await client.query(
      `SELECT
         d.roworder,
         d.doc_no,
         TO_CHAR(d.doc_date::date, 'YYYY-MM-DD') AS doc_date,
         d.creator_code,
         d.ic_code,
         COALESCE(i.name_1, '') AS item_name,
         d.unit_code,
         COALESCE(barcode_info.barcode, '') AS barcode,
         COALESCE(barcode_info.description, '') AS barcode_description,
         COALESCE(u.ratio, 1) AS unit_ratio,
         d.sale_type,
         d.tax_type,
         COALESCE(d.formula_category_code, '') AS formula_category_code,
         COALESCE(d.formula_category_name, '') AS formula_category_name,
         d.command,
         COALESCE(d.old_price_available, false) AS old_price_available,
         d.create_date_time_now,
         ${PRICE_FIELDS.map((field) => `COALESCE(d.old_${field}, 0) AS old_${field}`).join(',\n         ')},
         ${PRICE_FIELDS.map((field) => `COALESCE(d.${field}, 0) AS ${field}`).join(',\n         ')}
       FROM pc_formular_doc_detail_temp_log d
       LEFT JOIN ic_inventory i ON i.code = d.ic_code
       LEFT JOIN ic_unit_use u ON u.ic_code = d.ic_code AND u.code = d.unit_code
       LEFT JOIN LATERAL (
         SELECT ib.barcode, COALESCE(ib.description, '') AS description
         FROM ic_inventory_barcode ib
         WHERE ib.ic_code = d.ic_code
           AND ib.unit_code = d.unit_code
           AND COALESCE(ib.barcode, '') <> ''
         ORDER BY COALESCE(ib.roworder, 0), ib.barcode
         LIMIT 1
       ) barcode_info ON true
       WHERE d.doc_no = $1
       ORDER BY d.roworder
       LIMIT 2000`,
      [docNo],
    )
    const data = result.rows.map((row) => ({
      ...row,
      old_prices: priceObject(row, 'old_'),
      new_prices: priceObject(row),
    }))
    res.json({ data, count: data.length })
  } catch (err) {
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

router.post('/save', async (req, res) => {
  const saleType = 0
  const items = Array.isArray(req.body.items) ? req.body.items : []
  const remark = clean(req.body.remark).slice(0, 255)

  if (!items.length) return res.status(400).json({ error: 'ไม่มีรายการสำหรับบันทึก' })
  if (items.length > 1000) return res.status(400).json({ error: 'บันทึกได้ไม่เกิน 1,000 รายการต่อครั้ง' })

  const normalizedItems = []
  for (const item of items) {
    const icCode = clean(item.item_code)
    const unitCode = clean(item.unit_code)
    const taxType = 0
    const sourceDocNo = clean(item.source_doc_no).slice(0, 255)
    const sourceTransFlag = intValue(item.source_trans_flag, 0)
    const formulaCategoryCode = clean(item.formula_category_code || item.selected_category_code).slice(0, 25)
    const formulaCategoryName = clean(item.formula_category_name || item.selected_category_name).slice(0, 255)
    if (!icCode || !unitCode) return res.status(400).json({ error: 'พบรายการที่ไม่มีรหัสสินค้าหรือหน่วยนับ' })

    const prices = PRICE_FIELDS.map((field) => numericValue(item[field]))
    if (prices.some((price) => !Number.isFinite(price))) {
      return res.status(400).json({ error: `ราคาของสินค้า ${icCode} ต้องเป็นตัวเลขและไม่ติดลบ` })
    }

    normalizedItems.push({ icCode, unitCode, taxType, sourceDocNo, sourceTransFlag, formulaCategoryCode, formulaCategoryName, prices })
  }

  const client = await posDB.connect()
  let docNo = ''
  let docDate = ''
  const creatorCode = clean(req.user?.code)

  try {
    await client.query('BEGIN')
    await ensureFormulaHistorySchema(client)
    const clock = await client.query(
      `SELECT
         TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') AS doc_date,
         TO_CHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSMS') AS stamp`,
    )
    const suffix = String(Math.floor(Math.random() * 100)).padStart(2, '0')
    docDate = clock.rows[0].doc_date
    docNo = `CRMPR${clock.rows[0].stamp}${suffix}`.slice(0, 25)

    await client.query(
      `INSERT INTO pc_formular_doc_temp (doc_no, doc_date, creator_code, remark)
       VALUES ($1, $2::date, $3, $4)`,
      [docNo, docDate, creatorCode, remark],
    )

    let insertCount = 0
    let updateCount = 0

    for (const item of normalizedItems) {
      const exists = await client.query(
        `SELECT ${PRICE_FIELDS.map((field) => `COALESCE(${field}::text, '0') AS ${field}`).join(', ')}
         FROM ic_inventory_price_formula
         WHERE COALESCE(ic_code, '') = COALESCE($1, '')
           AND COALESCE(unit_code, '') = COALESCE($2, '')
           AND sale_type = $3
           AND tax_type = $4
         LIMIT 1`,
        [item.icCode, item.unitCode, saleType, item.taxType],
      )

      const command = exists.rowCount ? 'Update' : 'Insert'
      const oldPrices = PRICE_FIELDS.map((field) => numericValue(exists.rows[0]?.[field]))
      if (!pricesChanged(oldPrices, item.prices, exists.rowCount > 0)) continue

      if (exists.rowCount) {
        await client.query(
          `UPDATE ic_inventory_price_formula
           SET creator_code = $1,
               create_date_time_now = now(),
               doc_no = $2,
               ${PRICE_FIELDS.map((field, index) => `${field} = $${index + 3}`).join(', ')}
           WHERE COALESCE(ic_code, '') = COALESCE($13, '')
             AND COALESCE(unit_code, '') = COALESCE($14, '')
             AND sale_type = $15
             AND tax_type = $16`,
          [creatorCode, docNo, ...item.prices.map(toPriceText), item.icCode, item.unitCode, saleType, item.taxType],
        )
        updateCount += 1
      } else {
        await client.query(
          `INSERT INTO ic_inventory_price_formula (
             ic_code, unit_code, sale_type, tax_type,
             ${PRICE_FIELDS.join(', ')}, creator_code, doc_no
           )
           VALUES ($1, $2, $3, $4, ${PRICE_FIELDS.map((_, index) => `$${index + 5}`).join(', ')}, $15, $16)`,
          [item.icCode, item.unitCode, saleType, item.taxType, ...item.prices.map(toPriceText), creatorCode, docNo],
        )
        insertCount += 1
      }

      const detailParams = [
        docNo,
        docDate,
        creatorCode,
        item.icCode,
        item.unitCode,
        saleType,
        item.taxType,
        item.sourceDocNo,
        item.sourceTransFlag,
        item.formulaCategoryCode,
        item.formulaCategoryName,
        exists.rowCount > 0,
        ...oldPrices,
        ...item.prices,
        command,
      ]
      const detailValues = detailParams.map((_, index) => `$${index + 1}`).join(', ')

      await client.query(
        `INSERT INTO pc_formular_doc_detail_temp (
           doc_no, doc_date, creator_code, ic_code, unit_code, sale_type, tax_type,
           source_doc_no, source_trans_flag,
           formula_category_code, formula_category_name,
           old_price_available,
           ${PRICE_FIELDS.map((field) => `old_${field}`).join(', ')},
           ${PRICE_FIELDS.join(', ')}, command
         )
         VALUES (${detailValues})`,
        detailParams,
      )
      await client.query(
        `INSERT INTO pc_formular_doc_detail_temp_log (
           doc_no, doc_date, creator_code, ic_code, unit_code, sale_type, tax_type,
           source_doc_no, source_trans_flag,
           formula_category_code, formula_category_name,
           old_price_available,
           ${PRICE_FIELDS.map((field) => `old_${field}`).join(', ')},
           ${PRICE_FIELDS.join(', ')}, command
         )
         VALUES (${detailValues})`,
        detailParams,
      )
    }

    if (insertCount + updateCount === 0) {
      await client.query('ROLLBACK')
      return res.status(400).json({ error: 'ไม่มีรายการที่มีการเปลี่ยนแปลงราคา' })
    }

    await client.query('COMMIT')
    res.json({
      success: true,
      doc_no: docNo,
      saved_count: insertCount + updateCount,
      insert_count: insertCount,
      update_count: updateCount,
    })
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

module.exports = router
