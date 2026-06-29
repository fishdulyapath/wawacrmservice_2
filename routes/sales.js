const express = require('express')
const router  = express.Router()
const { posDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

router.use(authMiddleware)

// ─────────────────────────────────────────────────────────────
// GET /api/sales/summary
// KPI ภาพรวมยอดขาย + top 10 ลูกค้า + top 10 พนักงาน
// query: date_from, date_to, sale_code, cust_code
// ─────────────────────────────────────────────────────────────
router.get('/summary', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code, cust_code } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from)  { params.push(date_from);  conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);    conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code)  { params.push(sale_code);  conds.push(`t.sale_code = $${params.length}`) }
    if (cust_code)  { params.push(cust_code);  conds.push(`t.cust_code = $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')

    const [kpiRes, topCustRes, topSaleRes] = await Promise.all([
      posDB.query(`
        SELECT
          COUNT(*)                                    AS total_orders,
          ROUND(COALESCE(SUM(t.total_amount),0)::numeric, 2)    AS total_amount,
          ROUND(COALESCE(AVG(t.total_amount),0)::numeric, 2)    AS avg_order_value,
          ROUND(COALESCE(SUM(t.total_discount),0)::numeric, 2)  AS total_discount
        FROM ic_trans t
        ${where}
      `, params),

      posDB.query(`
        SELECT
          t.cust_code,
          c.name_1                                            AS cust_name,
          COUNT(*)                                            AS total_orders,
          ROUND(SUM(t.total_amount)::numeric, 2)             AS total_amount
        FROM ic_trans t
        LEFT JOIN ar_customer c ON c.code = t.cust_code
        ${where}
        GROUP BY t.cust_code, c.name_1
        ORDER BY total_amount DESC
        LIMIT 10
      `, params),

      posDB.query(`
        SELECT
          t.sale_code,
          e.name_1                                            AS sale_name,
          COUNT(*)                                            AS total_orders,
          ROUND(SUM(t.total_amount)::numeric, 2)             AS total_amount
        FROM ic_trans t
        LEFT JOIN erp_user e ON e.code = t.sale_code
        ${where}
        GROUP BY t.sale_code, e.name_1
        ORDER BY total_amount DESC
        LIMIT 10
      `, params),
    ])

    const kpi = kpiRes.rows[0]
    res.json({
      total_orders:    parseInt(kpi.total_orders)   || 0,
      total_amount:    parseFloat(kpi.total_amount) || 0,
      avg_order_value: parseFloat(kpi.avg_order_value) || 0,
      total_discount:  parseFloat(kpi.total_discount)  || 0,
      top_customers: topCustRes.rows.map(r => ({
        ...r,
        total_orders: parseInt(r.total_orders),
        total_amount: parseFloat(r.total_amount),
      })),
      top_salespeople: topSaleRes.rows.map(r => ({
        ...r,
        total_orders: parseInt(r.total_orders),
        total_amount: parseFloat(r.total_amount),
      })),
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/trend
// แนวโน้มยอดขายรายวัน/รายสัปดาห์/รายเดือน
// query: date_from, date_to, period (day|week|month), sale_code, cust_code
// ─────────────────────────────────────────────────────────────
router.get('/trend', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, period = 'day', sale_code, cust_code } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from)  { params.push(date_from);  conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);    conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code)  { params.push(sale_code);  conds.push(`t.sale_code = $${params.length}`) }
    if (cust_code)  { params.push(cust_code);  conds.push(`t.cust_code = $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')
    const trunc = period === 'month' ? 'month' : period === 'week' ? 'week' : 'day'

    const result = await posDB.query(`
      SELECT
        DATE_TRUNC('${trunc}', t.doc_date::date)::date   AS period,
        COUNT(*)                                          AS total_orders,
        ROUND(SUM(t.total_amount)::numeric, 2)           AS total_amount
      FROM ic_trans t
      ${where}
      GROUP BY 1
      ORDER BY 1 ASC
    `, params)

    res.json(result.rows.map(r => ({
      period:       r.period,
      total_orders: parseInt(r.total_orders),
      total_amount: parseFloat(r.total_amount),
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/by-customer
// ยอดขายแยกตามลูกค้า
// query: date_from, date_to, sale_code, q
// ─────────────────────────────────────────────────────────────
router.get('/by-customer', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code, q } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from)  { params.push(date_from);  conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);    conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code)  { params.push(sale_code);  conds.push(`t.sale_code = $${params.length}`) }
    if (q) {
      params.push(`%${q}%`)
      conds.push(`(t.cust_code ILIKE $${params.length} OR c.name_1 ILIKE $${params.length})`)
    }

    const where = 'WHERE ' + conds.join(' AND ')

    const result = await posDB.query(`
      SELECT
        t.cust_code,
        c.name_1                                      AS cust_name,
        COUNT(*)                                      AS total_orders,
        ROUND(SUM(t.total_amount)::numeric, 2)        AS total_amount,
        ROUND(AVG(t.total_amount)::numeric, 2)        AS avg_amount
      FROM ic_trans t
      LEFT JOIN ar_customer c ON c.code = t.cust_code
      ${where}
      GROUP BY t.cust_code, c.name_1
      ORDER BY total_amount DESC
    `, params)

    res.json(result.rows.map(r => ({
      ...r,
      total_orders: parseInt(r.total_orders),
      total_amount: parseFloat(r.total_amount),
      avg_amount:   parseFloat(r.avg_amount),
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/by-salesperson
// ยอดขายแยกตามพนักงาน
// query: date_from, date_to, cust_code, q
// ─────────────────────────────────────────────────────────────
router.get('/by-salesperson', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, cust_code, q } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']
    const userConds = [`e.code <> ''`]

    if (date_from)  { params.push(date_from);  conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);    conds.push(`t.doc_date <= $${params.length}::date`) }
    if (cust_code)  { params.push(cust_code);  conds.push(`t.cust_code = $${params.length}`) }
    if (q) {
      params.push(`%${q}%`)
      userConds.push(`(e.code ILIKE $${params.length} OR e.name_1 ILIKE $${params.length})`)
    }

    const where = 'WHERE ' + conds.join(' AND ')
    const userWhere = 'WHERE ' + userConds.join(' AND ')

    const result = await posDB.query(`
      SELECT
        e.code                                           AS sale_code,
        e.name_1                                         AS sale_name,
        COALESCE(s.total_orders, 0)                      AS total_orders,
        COALESCE(s.total_amount, 0)                      AS total_amount,
        COALESCE(s.avg_amount, 0)                        AS avg_amount
      FROM erp_user e
      LEFT JOIN (
        SELECT
          t.sale_code,
          COUNT(*)                                      AS total_orders,
          ROUND(SUM(t.total_amount)::numeric, 2)        AS total_amount,
          ROUND(AVG(t.total_amount)::numeric, 2)        AS avg_amount
        FROM ic_trans t
        ${where}
        GROUP BY t.sale_code
      ) s ON s.sale_code = e.code
      ${userWhere}
      ORDER BY total_amount DESC, e.name_1 ASC
    `, params)

    res.json(result.rows.map(r => ({
      ...r,
      total_orders: parseInt(r.total_orders),
      total_amount: parseFloat(r.total_amount) || 0,
      avg_amount:   parseFloat(r.avg_amount) || 0,
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/transactions
// รายการขาย พร้อม pagination และ filter แบบ ILIKE
// query: date_from, date_to, doc_no, sale_code, cust_code (ILIKE), page, limit
// ─────────────────────────────────────────────────────────────
router.get('/transactions', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, doc_no, sale_code, cust_code, page = 1, limit = 20 } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from)  { params.push(date_from);          conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);            conds.push(`t.doc_date <= $${params.length}::date`) }
    if (doc_no)     { params.push(`%${doc_no}%`);      conds.push(`t.doc_no ILIKE $${params.length}`) }
    if (sale_code)  { params.push(`%${sale_code}%`);   conds.push(`t.sale_code ILIKE $${params.length}`) }
    if (cust_code)  { params.push(`%${cust_code}%`);   conds.push(`t.cust_code ILIKE $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')

    const countRes = await posDB.query(
      `SELECT COUNT(*) FROM ic_trans t ${where}`,
      params
    )
    const total = parseInt(countRes.rows[0].count)

    params.push(parseInt(limit), offset)
    const dataRes = await posDB.query(`
      SELECT
        t.doc_no, t.doc_date, t.doc_time,
        t.cust_code, c.name_1                              AS cust_name,
        t.sale_code, e.name_1                              AS sale_name,
        ROUND(t.total_amount::numeric, 2)                  AS total_amount,
        ROUND(t.total_discount::numeric, 2)                AS total_discount,
        t.vat_type, t.remark
      FROM ic_trans t
      LEFT JOIN ar_customer c ON c.code = t.cust_code
      LEFT JOIN erp_user    e ON e.code = t.sale_code
      ${where}
      ORDER BY t.doc_date DESC, t.doc_time DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    res.json({
      data: dataRes.rows.map(r => ({
        ...r,
        total_amount:   parseFloat(r.total_amount),
        total_discount: parseFloat(r.total_discount),
      })),
      pagination: {
        total,
        page:  parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit)),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/transactions/:doc_no
// รายละเอียดเอกสาร + รายการสินค้า
// ─────────────────────────────────────────────────────────────
router.get('/transactions/:doc_no', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { doc_no } = req.params

    const [headerRes, linesRes] = await Promise.all([
      posDB.query(`
        SELECT
          t.doc_no, t.doc_date, t.doc_time,
          t.cust_code, c.name_1                          AS cust_name,
          t.sale_code, e.name_1                          AS sale_name,
          ROUND(t.total_amount::numeric, 2)              AS total_amount,
          ROUND(t.total_discount::numeric, 2)            AS total_discount,
          ROUND(t.total_vat_value::numeric, 2)           AS total_vat_value,
          t.vat_type, t.remark
        FROM ic_trans t
        LEFT JOIN ar_customer c ON c.code = t.cust_code
        LEFT JOIN erp_user    e ON e.code = t.sale_code
        WHERE t.doc_no = $1 AND t.trans_flag = 44
      `, [doc_no]),

      posDB.query(`
        SELECT
          d.item_code, d.item_name, d.barcode,
          d.unit_code, u.name_1                          AS unit_name,
          ROUND(d.qty::numeric, 4)                       AS qty,
          ROUND(d.price::numeric, 4)                     AS price,
          d.discount,
          ROUND(d.sum_amount::numeric, 2)                AS sum_amount
        FROM ic_trans_detail d
        LEFT JOIN ic_unit u ON u.code = d.unit_code
        WHERE d.doc_no = $1 AND d.trans_flag = 44
        ORDER BY d.roworder
      `, [doc_no]),
    ])

    if (!headerRes.rows.length) {
      return res.status(404).json({ error: 'ไม่พบเอกสาร' })
    }

    const header = headerRes.rows[0]
    res.json({
      header: {
        ...header,
        total_amount:   parseFloat(header.total_amount),
        total_discount: parseFloat(header.total_discount),
        total_vat_value: parseFloat(header.total_vat_value),
      },
      lines: linesRes.rows.map(r => ({
        ...r,
        qty:        parseFloat(r.qty),
        price:      parseFloat(r.price),
        sum_amount: parseFloat(r.sum_amount),
      })),
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/customer/:cust_code
// ประวัติการซื้อของลูกค้า (paginated)
// query: date_from, date_to, doc_no (ILIKE), sale_code (ILIKE), page, limit
// ─────────────────────────────────────────────────────────────
router.get('/customer/:cust_code', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { cust_code } = req.params
    const { date_from, date_to, doc_no, sale_code, quote_no, page = 1, limit = 10 } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)

    const params = [cust_code]
    const conds  = ['t.trans_flag = 44', 't.last_status = 0', `t.cust_code = $1`]

    if (date_from)  { params.push(date_from);         conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);           conds.push(`t.doc_date <= $${params.length}::date`) }
    if (doc_no)     { params.push(`%${doc_no}%`);     conds.push(`t.doc_no ILIKE $${params.length}`) }
    if (sale_code)  { params.push(`%${sale_code}%`);  conds.push(`t.sale_code ILIKE $${params.length}`) }
    if (quote_no)   { params.push(`%${quote_no}%`);   conds.push(`t.doc_ref ILIKE $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')

    const countRes = await posDB.query(
      `SELECT COUNT(*) FROM ic_trans t ${where}`,
      params
    )
    const total = parseInt(countRes.rows[0].count)

    params.push(parseInt(limit), offset)
    const dataRes = await posDB.query(`
      SELECT
        t.doc_no, t.doc_date, t.doc_time,
        t.sale_code, e.name_1                            AS sale_name,
        ROUND(t.total_amount::numeric, 2)                AS total_amount,
        ROUND(t.total_discount::numeric, 2)              AS total_discount,
        t.vat_type, t.remark,
        CASE WHEN ref.trans_flag = 36 THEN t.doc_ref ELSE NULL END AS quote_no,
        qt.qt_doc
      FROM ic_trans t
      LEFT JOIN erp_user e ON e.code = t.sale_code
      LEFT JOIN ic_trans ref ON ref.doc_no = t.doc_ref AND ref.trans_flag = 36
      LEFT JOIN LATERAL (
        SELECT STRING_AGG(DISTINCT NULLIF(TRIM(d.billing_no), ''), ', ' ORDER BY NULLIF(TRIM(d.billing_no), '')) AS qt_doc
        FROM ap_ar_trans_detail d
        WHERE d.doc_no = t.doc_ref
          AND d.trans_flag = 36
      ) qt ON TRUE
      ${where}
      ORDER BY t.doc_date DESC, t.doc_time DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    res.json({
      data: dataRes.rows.map(r => ({
        ...r,
        total_amount:   parseFloat(r.total_amount),
        total_discount: parseFloat(r.total_discount),
        quote_no: r.quote_no || null,
        qt_doc: r.qt_doc || null,
      })),
      pagination: {
        total,
        page:  parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit)),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/top-products
// TOP N สินค้าขายดี จาก ic_trans_detail
// query: date_from, date_to, sale_code, cust_code, limit (default 10)
// ─────────────────────────────────────────────────────────────
router.get('/top-products', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code, cust_code, limit: lim = 10 } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from) { params.push(date_from); conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code) { params.push(sale_code); conds.push(`t.sale_code = $${params.length}`) }
    if (cust_code) { params.push(cust_code); conds.push(`t.cust_code = $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')
    params.push(parseInt(lim))

    const result = await posDB.query(`
      SELECT
        d.item_code,
        d.item_name,
        ROUND(SUM(d.qty)::numeric, 2)          AS total_qty,
        ROUND(SUM(d.sum_amount)::numeric, 2)   AS total_amount,
        COUNT(DISTINCT d.doc_no)               AS doc_count
      FROM ic_trans_detail d
      JOIN ic_trans t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
      ${where}
      GROUP BY d.item_code, d.item_name
      ORDER BY total_amount DESC
      LIMIT $${params.length}
    `, params)

    res.json(result.rows.map(r => ({
      ...r,
      total_qty:    parseFloat(r.total_qty),
      total_amount: parseFloat(r.total_amount),
      doc_count:    parseInt(r.doc_count),
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/by-product
// ยอดขายแยกตามสินค้า จาก ic_trans_detail พร้อมค้นหาและโหลดเพิ่ม
// query: date_from, date_to, sale_code, q, page, limit
// ─────────────────────────────────────────────────────────────
router.get('/by-product', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code, q, page = 1, limit = 30 } = req.query
    const safeLimit = Math.min(Math.max(parseInt(limit) || 30, 1), 100)
    const safePage = Math.max(parseInt(page) || 1, 1)
    const offset = (safePage - 1) * safeLimit
    const params = []
    const conds = ['d.trans_flag = 44', 'd.status = 0', 't.last_status = 0']

    if (date_from) { params.push(date_from); conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code) { params.push(sale_code); conds.push(`t.sale_code = $${params.length}`) }
    if (q) {
      params.push(`%${q}%`)
      conds.push(`(d.item_code ILIKE $${params.length} OR d.item_name ILIKE $${params.length})`)
    }

    const where = 'WHERE ' + conds.join(' AND ')
    const countRes = await posDB.query(`
      SELECT COUNT(*) AS count
      FROM (
        SELECT d.item_code, d.item_name
        FROM ic_trans_detail d
        JOIN ic_trans t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
        ${where}
        GROUP BY d.item_code, d.item_name, d.unit_code
      ) x
    `, params)
    const total = parseInt(countRes.rows[0].count) || 0

    params.push(safeLimit, offset)
    const dataRes = await posDB.query(`
      SELECT
        d.item_code,
        d.item_name,
        d.unit_code,
        COUNT(*)                                 AS line_count,
        COUNT(DISTINCT d.doc_no)                AS doc_count,
        ROUND(SUM(d.qty)::numeric, 2)           AS total_qty,
        ROUND(SUM(d.sum_amount)::numeric, 2)    AS total_amount,
        ROUND(AVG(NULLIF(d.price, 0))::numeric, 2) AS avg_price
      FROM ic_trans_detail d
      JOIN ic_trans t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
      ${where}
      GROUP BY d.item_code, d.item_name, d.unit_code
      ORDER BY total_amount DESC, total_qty DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    res.json({
      data: dataRes.rows.map(r => ({
        ...r,
        line_count:   parseInt(r.line_count),
        doc_count:    parseInt(r.doc_count),
        total_qty:    parseFloat(r.total_qty),
        total_amount: parseFloat(r.total_amount),
        avg_price:    parseFloat(r.avg_price) || 0,
      })),
      pagination: {
        total,
        page: safePage,
        limit: safeLimit,
        pages: Math.ceil(total / safeLimit),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/by-category
// ยอดขายแยกตามหมวดสินค้า จาก ic_inventory.item_category
// query: date_from, date_to, sale_code, q, page, limit
// ─────────────────────────────────────────────────────────────
router.get('/by-category', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code, q, page = 1, limit = 30 } = req.query
    const safeLimit = Math.min(Math.max(parseInt(limit) || 30, 1), 100)
    const safePage = Math.max(parseInt(page) || 1, 1)
    const offset = (safePage - 1) * safeLimit
    const params = []
    const conds = ['d.trans_flag = 44', 'd.status = 0', 't.last_status = 0']

    if (date_from) { params.push(date_from); conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`t.doc_date <= $${params.length}::date`) }
    if (sale_code) { params.push(sale_code); conds.push(`t.sale_code = $${params.length}`) }
    if (q) {
      params.push(`%${q}%`)
      conds.push(`(i.item_category ILIKE $${params.length} OR c.name_1 ILIKE $${params.length})`)
    }

    const where = 'WHERE ' + conds.join(' AND ')
    const countRes = await posDB.query(`
      SELECT COUNT(*) AS count
      FROM (
        SELECT COALESCE(NULLIF(i.item_category, ''), '-') AS category_code
        FROM ic_trans_detail d
        JOIN ic_trans t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
        LEFT JOIN ic_inventory i ON i.code = d.item_code
        LEFT JOIN ic_category c ON c.code = i.item_category
        ${where}
        GROUP BY COALESCE(NULLIF(i.item_category, ''), '-'), COALESCE(NULLIF(c.name_1, ''), 'ไม่ระบุหมวด')
      ) x
    `, params)
    const total = parseInt(countRes.rows[0].count) || 0

    params.push(safeLimit, offset)
    const dataRes = await posDB.query(`
      SELECT
        COALESCE(NULLIF(i.item_category, ''), '-')       AS category_code,
        COALESCE(NULLIF(c.name_1, ''), 'ไม่ระบุหมวด')   AS category_name,
        COUNT(DISTINCT d.item_code)                     AS item_count,
        COUNT(*)                                        AS line_count,
        COUNT(DISTINCT d.doc_no)                        AS doc_count,
        ROUND(SUM(d.qty)::numeric, 2)                   AS total_qty,
        ROUND(SUM(d.sum_amount)::numeric, 2)            AS total_amount
      FROM ic_trans_detail d
      JOIN ic_trans t ON t.doc_no = d.doc_no AND t.trans_flag = d.trans_flag
      LEFT JOIN ic_inventory i ON i.code = d.item_code
      LEFT JOIN ic_category c ON c.code = i.item_category
      ${where}
      GROUP BY COALESCE(NULLIF(i.item_category, ''), '-'), COALESCE(NULLIF(c.name_1, ''), 'ไม่ระบุหมวด')
      ORDER BY total_amount DESC, total_qty DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    res.json({
      data: dataRes.rows.map(r => ({
        ...r,
        item_count:   parseInt(r.item_count),
        line_count:   parseInt(r.line_count),
        doc_count:    parseInt(r.doc_count),
        total_qty:    parseFloat(r.total_qty),
        total_amount: parseFloat(r.total_amount),
      })),
      pagination: {
        total,
        page: safePage,
        limit: safeLimit,
        pages: Math.ceil(total / safeLimit),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/top-salespeople
// TOP N ทีมขาย
// query: date_from, date_to, cust_code, limit (default 10)
// ─────────────────────────────────────────────────────────────
router.get('/top-salespeople', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, cust_code, limit: lim = 10 } = req.query
    const params = []
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']

    if (date_from) { params.push(date_from); conds.push(`t.doc_date >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`t.doc_date <= $${params.length}::date`) }
    if (cust_code) { params.push(cust_code); conds.push(`t.cust_code = $${params.length}`) }

    const where = 'WHERE ' + conds.join(' AND ')
    params.push(parseInt(lim))

    const result = await posDB.query(`
      SELECT
        t.sale_code,
        e.name_1                                      AS sale_name,
        COUNT(*)                                      AS total_orders,
        ROUND(SUM(t.total_amount)::numeric, 2)        AS total_amount,
        ROUND(AVG(t.total_amount)::numeric, 2)        AS avg_amount
      FROM ic_trans t
      LEFT JOIN erp_user e ON e.code = t.sale_code
      ${where}
      GROUP BY t.sale_code, e.name_1
      ORDER BY total_amount DESC
      LIMIT $${params.length}
    `, params)

    res.json(result.rows.map(r => ({
      ...r,
      total_orders: parseInt(r.total_orders),
      total_amount: parseFloat(r.total_amount),
      avg_amount:   parseFloat(r.avg_amount),
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/salespeople
// รายชื่อพนักงานขายจาก POS DB (erp_user)
// ─────────────────────────────────────────────────────────────
router.get('/salespeople', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const result = await posDB.query(`
      SELECT code, name_1
      FROM erp_user
      WHERE code != ''
      ORDER BY name_1
    `)
    res.json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/sales/map
// ยอดขายรายลูกค้าพร้อมพิกัดจาก ar_customer.website ("lat,lng")
// Params: date_from, date_to, sale_code
// ─────────────────────────────────────────────────────────────
router.get('/map', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const { date_from, date_to, sale_code } = req.query
    const conds  = ['t.trans_flag = 44', 't.last_status = 0']
    const params = []

    if (date_from) { params.push(date_from); conds.push(`t.doc_date >= $${params.length}`) }
    if (date_to)   { params.push(date_to);   conds.push(`t.doc_date <= $${params.length}`) }
    if (sale_code) { params.push(sale_code); conds.push(`t.sale_code = $${params.length}`) }

    const where = conds.join(' AND ')

    const result = await posDB.query(`
      WITH customer_geo AS (
        SELECT
          code,
          name_1,
          province,
          amper,
          split_part(trim(website), ',', 1)::numeric AS latitude,
          split_part(trim(website), ',', 2)::numeric AS longitude
        FROM ar_customer
        WHERE trim(COALESCE(website, '')) ~ '^-?[0-9]+([.][0-9]+)?,-?[0-9]+([.][0-9]+)?$'
      )
      SELECT
        t.cust_code,
        c.name_1                               AS cust_name,
        c.province                             AS province_code,
        COALESCE(ep.name_1, c.province, '')    AS province,
        c.amper                                AS amper_code,
        COALESCE(ea.name_1, c.amper, '')       AS amper,
        c.latitude::float                      AS lat,
        c.longitude::float                     AS lng,
        COUNT(DISTINCT t.doc_no)               AS total_orders,
        ROUND(SUM(t.total_amount)::numeric, 2) AS total_amount
      FROM ic_trans t
      JOIN customer_geo c   ON c.code     = t.cust_code
      LEFT JOIN erp_province ep ON ep.code    = c.province
      LEFT JOIN erp_amper    ea ON ea.code    = c.amper
      WHERE ${where}
        AND c.latitude BETWEEN -90 AND 90
        AND c.longitude BETWEEN -180 AND 180
        AND c.latitude <> 0
        AND c.longitude <> 0
      GROUP BY t.cust_code, c.name_1, c.province, ep.name_1, c.amper, ea.name_1, c.latitude, c.longitude
      ORDER BY total_amount DESC
    `, params)

    // district summary (อำเภอ + จังหวัด)
    const districtMap = {}
    for (const r of result.rows) {
      const province = r.province || 'ไม่ระบุจังหวัด'
      const district = r.amper || 'ไม่ระบุอำเภอ'
      const key = `${province}::${district}`
      if (!districtMap[key]) {
        districtMap[key] = { province, district, total_amount: 0, total_orders: 0, cust_count: 0 }
      }
      districtMap[key].total_amount += parseFloat(r.total_amount || 0)
      districtMap[key].total_orders += parseInt(r.total_orders || 0)
      districtMap[key].cust_count += 1
    }
    const districts = Object.values(districtMap)
      .sort((a, b) => b.total_amount - a.total_amount)
      .map(p => ({ ...p, total_amount: Math.round(p.total_amount * 100) / 100 }))

    res.json({ markers: result.rows, districts, provinces: districts })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
