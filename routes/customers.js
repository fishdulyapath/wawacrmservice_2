const express = require('express')
const router = express.Router()
const multer = require('multer')
const sharp  = require('sharp')
const fs     = require('fs')
const nodePath = require('path')
const { posDB, crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')
const { logAudit } = require('../middleware/audit')
const { ensureCustomerFollowupPolicy, normalizeFollowupInterval } = require('../services/followupPolicy')

const uploadMem = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } })

// ทุก endpoint ต้อง Login ก่อน
router.use(authMiddleware)

// Migration: เพิ่ม column shop_image ถ้ายังไม่มี
crmDB.query(`ALTER TABLE crm_customer_profile ADD COLUMN IF NOT EXISTS shop_image TEXT`).catch(() => {})

const canManageFollowup = u => u?.code?.toUpperCase() === 'SUPERADMIN' || ['admin','manager','supervisor'].includes(u?.role)
let customerStoreLinkReady = false
const CALL_RETRY_RESULTS = ['no_answer', 'busy', 'left_voicemail']

function normalizeCustomerOwners(crm = {}) {
  const source = Array.isArray(crm.owners)
    ? crm.owners
    : (crm.owner_user_id ? [{ user_id: crm.owner_user_id, is_primary: true }] : [])

  const seen = new Set()
  const owners = []
  for (const item of source) {
    const rawId = typeof item === 'object' ? item.user_id : item
    const userId = Number(rawId)
    if (!userId || seen.has(userId)) continue
    seen.add(userId)
    owners.push({
      user_id: userId,
      is_primary: typeof item === 'object' ? !!item.is_primary : false,
    })
  }

  if (owners.length && !owners.some(o => o.is_primary)) owners[0].is_primary = true
  if (owners.filter(o => o.is_primary).length > 1) {
    let primaryUsed = false
    owners.forEach(o => {
      if (o.is_primary && !primaryUsed) primaryUsed = true
      else o.is_primary = false
    })
  }

  return owners
}

async function replaceCustomerOwners(client, arCode, owners, assignedBy) {
  await client.query(`DELETE FROM crm_customer_owner WHERE ar_code = $1`, [arCode])
  for (const owner of owners) {
    await client.query(`
      INSERT INTO crm_customer_owner (ar_code, user_id, is_primary, assigned_by)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (ar_code, user_id) DO UPDATE SET
        is_primary = EXCLUDED.is_primary,
        assigned_by = EXCLUDED.assigned_by,
        assigned_at = NOW()
    `, [arCode, owner.user_id, owner.is_primary, assignedBy || null])
  }
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

function fleetStoreStatsSql(extraWhere = '') {
  return `
    SELECT
      ls.store_id,
      COUNT(DISTINCT ls.list_id)::int AS visits,
      COALESCE(SUM(ret.return_total), 0) AS return_total,
      COALESCE(SUM(prob.problem_count), 0)::int AS problem_count,
      COALESCE(SUM(prob.store_closed_count), 0)::int AS store_closed_count,
      MAX(COALESCE(co.date_time_check_out, ci.date_time_check_in, ls.created_at)) AS latest_visit_at
    FROM fleet_list_stores ls
    LEFT JOIN fleet_check_ins ci ON ci.list_id = ls.list_id AND ci.deleted_at IS NULL
    LEFT JOIN fleet_check_outs co ON co.list_id = ls.list_id AND co.deleted_at IS NULL
    LEFT JOIN LATERAL (
      SELECT COALESCE(SUM(total), 0) AS return_total
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
    WHERE ls.deleted_at IS NULL
      AND ls.store_id IS NOT NULL
      ${extraWhere}
    GROUP BY ls.store_id
  `
}

async function loadFleetCustomerCodes(fleetStatus) {
  const whereByStatus = {
    has_fleet:     'visits > 0',
    has_returns:   'return_total > 0',
    has_problems:  'problem_count > 0',
    store_closed:  'store_closed_count > 0',
  }
  const where = whereByStatus[fleetStatus] || whereByStatus.has_fleet

  try {
    await ensureCustomerStoreLinkTable()
    const result = await crmDB.query(`
      WITH store_stats AS (${fleetStoreStatsSql()}),
      customer_stats AS (
        SELECT
          COALESCE(l.ar_code, ss.store_id) AS ar_code,
          COALESCE(SUM(ss.visits), 0)::int AS visits,
          COALESCE(SUM(ss.return_total), 0) AS return_total,
          COALESCE(SUM(ss.problem_count), 0)::int AS problem_count,
          COALESCE(SUM(ss.store_closed_count), 0)::int AS store_closed_count
        FROM store_stats ss
        LEFT JOIN crm_customer_store_link l ON l.store_id = ss.store_id AND l.deleted_at IS NULL
        GROUP BY COALESCE(l.ar_code, ss.store_id)
      )
      SELECT ar_code
      FROM customer_stats
      WHERE ${where}
    `)
    return result.rows.map(r => r.ar_code).filter(Boolean)
  } catch (err) {
    console.warn('[customers/fleet filter]', err.message)
    return []
  }
}

async function loadFleetSummaryByArCodes(codes) {
  if (!codes.length) return {}

  try {
    await ensureCustomerStoreLinkTable()
    const result = await crmDB.query(`
      WITH customer_store AS (
        SELECT DISTINCT ar_code, store_id
        FROM (
          SELECT ar_code, store_id
          FROM crm_customer_store_link
          WHERE ar_code = ANY($1)
            AND deleted_at IS NULL
          UNION ALL
          SELECT code AS ar_code, code AS store_id
          FROM UNNEST($1::text[]) AS x(code)
        ) x
      ),
      store_stats AS (${fleetStoreStatsSql('AND ls.store_id IN (SELECT store_id FROM customer_store)')})
      SELECT
        cs.ar_code,
        COALESCE(SUM(ss.visits), 0)::int AS visits,
        COALESCE(SUM(ss.return_total), 0) AS return_total,
        COALESCE(SUM(ss.problem_count), 0)::int AS problem_count,
        COALESCE(SUM(ss.store_closed_count), 0)::int AS store_closed_count,
        MAX(ss.latest_visit_at) AS latest_visit_at
      FROM customer_store cs
      JOIN store_stats ss ON ss.store_id = cs.store_id
      GROUP BY cs.ar_code
    `, [codes])

    return Object.fromEntries(result.rows.map(r => [r.ar_code, r]))
  } catch (err) {
    console.warn('[customers/fleet summary]', err.message)
    return {}
  }
}

// ─────────────────────────────────────────────
// GET /api/customers  — List ลูกค้าทั้งหมด
// ─────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const { search = '', page = 1, limit = 20, status, owner, crm_only,
            followup_enabled, fleet_status, sort_by = 'code', sort_dir = 'asc' } = req.query

    const SORT_WHITELIST = {
      code:               'c.code',
      name_1:             'c.name_1',
      province:           'ep.name_1',
      last_purchase_date: 'last_purchase_date',
    }
    const sortCol     = SORT_WHITELIST[sort_by] || 'c.code'
    const sortDirSafe = sort_dir?.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
    const offset = (parseInt(page) - 1) * parseInt(limit)
    let nextFollowupOrderCodes = null

    // ── 1. สร้าง ar_code whitelist จาก filter ──────────────
    // filter by owner: CRM owner ทุกคน + POS sale_code อ้างอิงเดิม
    let ownerArCodes = null
    if (owner) {
      const [crmOwnerRes, salePosRes] = await Promise.all([
        crmDB.query(
          `SELECT o.ar_code FROM crm_customer_owner o
           JOIN crm_users u ON u.id = o.user_id
           WHERE u.code = $1`, [owner]
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

    // filter by followup_enabled
    let followupArCodes = null
    if (followup_enabled === 'true') {
      const fRes = await crmDB.query(
        `SELECT ar_code FROM crm_customer_profile WHERE followup_enabled = TRUE`
      )
      followupArCodes = fRes.rows.map(r => r.ar_code)
      if (followupArCodes.length === 0) {
        return res.json({ data: [], total: 0, page: parseInt(page), limit: parseInt(limit) })
      }
    }

    // ใช้ในหน้า ActivityForm: ค้นหาได้เฉพาะลูกค้าที่ถูกบันทึกเข้า CRM แล้ว
    let crmOnlyArCodes = null
    if (String(crm_only).toLowerCase() === 'true') {
      const crmOnlyRes = await crmDB.query(`SELECT ar_code FROM crm_customer_profile`)
      crmOnlyArCodes = crmOnlyRes.rows.map(r => r.ar_code)
      if (crmOnlyArCodes.length === 0) {
        return res.json({ data: [], total: 0, page: parseInt(page), limit: parseInt(limit) })
      }
    }

    // ── 2. Build WHERE สำหรับ POS query ───────────────────
    let fleetIncludeArCodes = null
    let fleetExcludeArCodes = null
    if (fleet_status) {
      if (fleet_status === 'no_fleet') {
        fleetExcludeArCodes = await loadFleetCustomerCodes('has_fleet')
      } else {
        fleetIncludeArCodes = await loadFleetCustomerCodes(fleet_status)
        if (fleetIncludeArCodes.length === 0) {
          return res.json({ data: [], total: 0, page: parseInt(page), limit: parseInt(limit) })
        }
      }
    }

    if (sort_by === 'next_followup') {
      const orderResult = await crmDB.query(`
        SELECT ar_code
        FROM crm_customer_profile
        WHERE next_followup IS NOT NULL
        ORDER BY next_followup ${sortDirSafe} NULLS LAST, ar_code ASC
      `)
      nextFollowupOrderCodes = orderResult.rows.map(r => r.ar_code)
    }

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
    if (followupArCodes) {
      params.push(followupArCodes)
      where.push(`c.code = ANY($${params.length})`)
    }
    if (crmOnlyArCodes) {
      params.push(crmOnlyArCodes)
      where.push(`c.code = ANY($${params.length})`)
    }
    if (fleetIncludeArCodes) {
      params.push(fleetIncludeArCodes)
      where.push(`c.code = ANY($${params.length})`)
    }
    if (fleetExcludeArCodes?.length) {
      params.push(fleetExcludeArCodes)
      where.push(`NOT (c.code = ANY($${params.length}))`)
    }

    // ── 3. COUNT ──────────────────────────────────────────
    const countResult = await posDB.query(
      `SELECT COUNT(*) FROM ar_customer c WHERE ${where.join(' AND ')}`,
      params
    )
    const total = parseInt(countResult.rows[0].count)

    // ── 4. PAGE query ─────────────────────────────────────
    let orderSql = `${sortCol} ${sortDirSafe} NULLS LAST, c.code ASC`
    if (sort_by === 'next_followup') {
      params.push(nextFollowupOrderCodes || [])
      const orderParam = params.length
      orderSql = `
        CASE WHEN array_position($${orderParam}::text[], c.code::text) IS NULL THEN 1 ELSE 0 END ASC,
        array_position($${orderParam}::text[], c.code::text) ASC,
        c.code ASC
      `
    }

    params.push(parseInt(limit))
    params.push(offset)

    const posResult = await posDB.query(`
      SELECT
        c.code, c.name_1, c.country, c.address, c.province,
        c.amper, c.tambon, c.zip_code, c.website, c.remark,
        d.sale_code, u.name_1 AS sale_name,
        ep.name_1 AS province_name,
        ea.name_1 AS amper_name,
        (
          SELECT MAX(t.doc_date)
          FROM ic_trans t
          WHERE t.cust_code = c.code
            AND t.trans_flag = 44
            AND t.last_status = 0
        ) AS last_purchase_date
      FROM ar_customer c
      LEFT JOIN ar_customer_detail d ON d.ar_code = c.code
      LEFT JOIN erp_user u           ON u.code = d.sale_code
      LEFT JOIN erp_province ep      ON ep.code = c.province
      LEFT JOIN erp_amper    ea      ON ea.code = c.amper AND ea.province = c.province
      WHERE ${where.join(' AND ')}
      ORDER BY ${orderSql}
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

      const ownersResult = await crmDB.query(`
        SELECT o.ar_code, o.user_id, o.is_primary, u.code, u.name
        FROM crm_customer_owner o
        JOIN crm_users u ON u.id = o.user_id
        WHERE o.ar_code = ANY($1)
        ORDER BY o.ar_code, o.is_primary DESC, o.assigned_at ASC
      `, [codes])
      ownersResult.rows.forEach(o => {
        if (!crmMap[o.ar_code]) return
        if (!crmMap[o.ar_code].owners) crmMap[o.ar_code].owners = []
        crmMap[o.ar_code].owners.push(o)
      })
    }

    const fleetMap = await loadFleetSummaryByArCodes(codes)

    const data = posResult.rows.map(c => {
      const crm = crmMap[c.code] ? { ...crmMap[c.code] } : (fleetMap[c.code] ? {} : null)
      if (crm && fleetMap[c.code]) crm.fleet = fleetMap[c.code]
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
        p.status AS crm_status,
        p.next_followup::text AS next_followup,
        p.followup_pause_until::text AS followup_pause_until,
        p.next_visit_followup::text AS next_visit_followup,
        p.visit_followup_pause_until::text AS visit_followup_pause_until,
        o.user_id AS owner_user_id,
        u.code    AS owner_code,
        u.name    AS owner_name
      FROM crm_customer_profile p
      LEFT JOIN crm_customer_owner o ON o.ar_code = p.ar_code AND o.is_primary = TRUE
      LEFT JOIN crm_users u          ON u.id = o.user_id
      WHERE p.ar_code = $1
    `, [code])

    let crm = crmResult.rows[0] || null

    if (crm) {
      const ownersResult = await crmDB.query(`
        SELECT o.user_id, o.is_primary, u.code, u.name
        FROM crm_customer_owner o
        JOIN crm_users u ON u.id = o.user_id
        WHERE o.ar_code = $1
        ORDER BY o.is_primary DESC, o.assigned_at ASC
      `, [code])
      crm.owners = ownersResult.rows
    }

    const followupSummaryRes = await crmDB.query(`
      SELECT
        (SELECT COUNT(*)::int
         FROM crm_activities a
         WHERE a.ar_code = $1
           AND a.followup_type IN ('scheduled','no_answer_retry')
           AND a.status NOT IN ('done','cancelled','deleted')
           AND (
             a.requires_owner_assignment = TRUE
             OR EXISTS (
               SELECT 1 FROM crm_activity_owners ao
               WHERE ao.activity_id = a.id
                 AND ao.removed_at IS NULL
                 AND ao.status NOT IN ('done','cancelled')
             )
           )) AS open_followup_count,
        (SELECT COUNT(*)::int
         FROM crm_activities a
         WHERE a.ar_code = $1
           AND a.activity_type = 'call'
           AND a.call_result = ANY($2)
           AND DATE(COALESCE(a.call_result_at, a.cdr_end_stamp, a.cdr_start_stamp, a.created_at) AT TIME ZONE 'Asia/Bangkok') = (NOW() AT TIME ZONE 'Asia/Bangkok')::date) AS no_answer_attempts_today,
        (SELECT json_build_object(
           'id', a.id,
           'subject', a.subject,
           'followup_type', a.followup_type,
           'due_date', a.due_date,
           'start_datetime', a.start_datetime,
           'retry_due_at', a.retry_due_at,
           'attempt_no', a.attempt_no,
           'requires_owner_assignment', a.requires_owner_assignment
         )
         FROM crm_activities a
         WHERE a.ar_code = $1
           AND a.followup_type IN ('scheduled','no_answer_retry')
           AND a.status NOT IN ('done','cancelled','deleted')
         ORDER BY COALESCE(a.retry_due_at, a.start_datetime, a.due_date::timestamptz, a.created_at) ASC
         LIMIT 1) AS next_open_followup,
        (SELECT json_build_object(
           'enabled', s.enabled,
           'auto_create_enabled', s.auto_create_enabled,
           'default_call_interval_days', s.default_call_interval_days,
           'no_answer_max_attempts_per_day', s.no_answer_max_attempts_per_day,
           'no_answer_retry_minutes', s.no_answer_retry_minutes,
           'business_start_time', to_char(s.business_start_time, 'HH24:MI'),
           'business_end_time', to_char(s.business_end_time, 'HH24:MI')
         )
         FROM crm_followup_settings s WHERE s.id = 1) AS policy
    `, [code, CALL_RETRY_RESULTS])

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
      followup_summary: followupSummaryRes.rows[0] || null,
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────
// PATCH /api/customers/:code/followup — override follow-up รายลูกค้า
// ─────────────────────────────────────────────
router.patch('/:code/followup', async (req, res) => {
  if (!canManageFollowup(req.user)) {
    return res.status(403).json({ error: 'ไม่มีสิทธิ์ปรับการติดตามลูกค้า' })
  }

  const { code } = req.params
  const {
    followup_enabled,
    followup_pause_until,
    followup_pause_reason,
    next_followup,
    followup_interval_days,
    // visit follow-up
    visit_followup_enabled,
    visit_followup_pause_until,
    visit_followup_pause_reason,
    next_visit_followup,
    visit_followup_interval_days,
  } = req.body

  try {
    await ensureCustomerFollowupPolicy()
    const hasFollowupEnabled = Object.prototype.hasOwnProperty.call(req.body, 'followup_enabled')
    const hasPauseUntil = Object.prototype.hasOwnProperty.call(req.body, 'followup_pause_until')
    const hasPauseReason = Object.prototype.hasOwnProperty.call(req.body, 'followup_pause_reason')
    let hasNextFollowup = Object.prototype.hasOwnProperty.call(req.body, 'next_followup')
    const hasFollowupInterval = Object.prototype.hasOwnProperty.call(req.body, 'followup_interval_days')
    const normalizedInterval = hasFollowupInterval
      ? normalizeFollowupInterval(followup_interval_days)
      : null

    // visit
    const hasVisitFollowupEnabled = Object.prototype.hasOwnProperty.call(req.body, 'visit_followup_enabled')
    const hasVisitPauseUntil = Object.prototype.hasOwnProperty.call(req.body, 'visit_followup_pause_until')
    const hasVisitPauseReason = Object.prototype.hasOwnProperty.call(req.body, 'visit_followup_pause_reason')
    let hasNextVisitFollowup = Object.prototype.hasOwnProperty.call(req.body, 'next_visit_followup')
    const hasVisitInterval = Object.prototype.hasOwnProperty.call(req.body, 'visit_followup_interval_days')
    const normalizedVisitInterval = hasVisitInterval
      ? normalizeFollowupInterval(visit_followup_interval_days)
      : null

    // ถ้ากำลังเปิด followup_enabled = true และ next_followup ไม่ได้ถูก set ใน request นี้
    // ให้ตรวจค่าปัจจุบันใน DB — ถ้าว่างหรือเลยวันนี้ ให้ default เป็น today+1 (Asia/Bangkok)
    let resolvedNextFollowup = next_followup || null
    let resolvedNextVisitFollowup = next_visit_followup || null
    if (hasFollowupEnabled && followup_enabled === true && !hasNextFollowup) {
      const cur = await crmDB.query(
        `SELECT next_followup, next_visit_followup FROM crm_customer_profile WHERE ar_code = $1`, [code]
      )
      const existingDate = cur.rows[0]?.next_followup
      // คำนวณวันที่ใน Asia/Bangkok โดยใช้ Intl เพื่อหลีกเลี่ยง UTC offset
      const bkkNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Bangkok' }))
      const todayStr = `${bkkNow.getFullYear()}-${String(bkkNow.getMonth() + 1).padStart(2, '0')}-${String(bkkNow.getDate()).padStart(2, '0')}`
      const tomorrowBkk = new Date(bkkNow)
      tomorrowBkk.setDate(tomorrowBkk.getDate() + 1)
      const tomorrowStr = `${tomorrowBkk.getFullYear()}-${String(tomorrowBkk.getMonth() + 1).padStart(2, '0')}-${String(tomorrowBkk.getDate()).padStart(2, '0')}`
      // existingDate จาก DB เป็น Date object หรือ string YYYY-MM-DD
      const existingStr = existingDate ? existingDate.toISOString?.().slice(0, 10) ?? String(existingDate).slice(0, 10) : null
      if (!existingStr || existingStr <= todayStr) {
        resolvedNextFollowup = tomorrowStr
        hasNextFollowup = true
      }
    }
    if (hasVisitFollowupEnabled && visit_followup_enabled === true && !hasNextVisitFollowup) {
      const cur = await crmDB.query(
        `SELECT next_visit_followup FROM crm_customer_profile WHERE ar_code = $1`, [code]
      )
      const existingDate = cur.rows[0]?.next_visit_followup
      const bkkNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Bangkok' }))
      const todayStr = `${bkkNow.getFullYear()}-${String(bkkNow.getMonth() + 1).padStart(2, '0')}-${String(bkkNow.getDate()).padStart(2, '0')}`
      const tomorrowBkk = new Date(bkkNow)
      tomorrowBkk.setDate(tomorrowBkk.getDate() + 1)
      const tomorrowStr = `${tomorrowBkk.getFullYear()}-${String(tomorrowBkk.getMonth() + 1).padStart(2, '0')}-${String(tomorrowBkk.getDate()).padStart(2, '0')}`
      const existingStr = existingDate ? existingDate.toISOString?.().slice(0, 10) ?? String(existingDate).slice(0, 10) : null
      if (!existingStr || existingStr <= todayStr) {
        resolvedNextVisitFollowup = tomorrowStr
        hasNextVisitFollowup = true
      }
    }

    const result = await crmDB.query(`
      UPDATE crm_customer_profile SET
        followup_enabled = CASE WHEN $2::boolean THEN $3::boolean ELSE followup_enabled END,
        followup_pause_until = CASE WHEN $4::boolean THEN $5::date ELSE followup_pause_until END,
        followup_pause_reason = CASE WHEN $6::boolean THEN $7::text ELSE followup_pause_reason END,
        next_followup = CASE WHEN $8::boolean THEN $9::date ELSE next_followup END,
        followup_interval_days = CASE WHEN $10::boolean THEN $11::integer ELSE followup_interval_days END,
        followup_interval_updated_by = CASE WHEN $10::boolean THEN $12::integer ELSE followup_interval_updated_by END,
        followup_interval_updated_at = CASE WHEN $10::boolean THEN NOW() ELSE followup_interval_updated_at END,
        visit_followup_enabled = CASE WHEN $13::boolean THEN $14::boolean ELSE visit_followup_enabled END,
        visit_followup_pause_until = CASE WHEN $15::boolean THEN $16::date ELSE visit_followup_pause_until END,
        visit_followup_pause_reason = CASE WHEN $17::boolean THEN $18::text ELSE visit_followup_pause_reason END,
        next_visit_followup = CASE WHEN $19::boolean THEN $20::date ELSE next_visit_followup END,
        visit_followup_interval_days = CASE WHEN $21::boolean THEN $22::integer ELSE visit_followup_interval_days END,
        visit_followup_interval_updated_by = CASE WHEN $21::boolean THEN $12::integer ELSE visit_followup_interval_updated_by END,
        visit_followup_interval_updated_at = CASE WHEN $21::boolean THEN NOW() ELSE visit_followup_interval_updated_at END,
        updated_at = NOW()
      WHERE ar_code = $1
      RETURNING ar_code,
                followup_enabled,
                followup_pause_until::text AS followup_pause_until,
                followup_pause_reason,
                next_followup::text AS next_followup,
                followup_interval_days, followup_interval_updated_by, followup_interval_updated_at,
                visit_followup_enabled,
                visit_followup_pause_until::text AS visit_followup_pause_until,
                visit_followup_pause_reason,
                next_visit_followup::text AS next_visit_followup,
                visit_followup_interval_days, visit_followup_interval_updated_by, visit_followup_interval_updated_at
    `, [
      code,
      hasFollowupEnabled,
      typeof followup_enabled === 'boolean' ? followup_enabled : null,
      hasPauseUntil,
      followup_pause_until || null,
      hasPauseReason,
      followup_pause_reason || null,
      hasNextFollowup,
      resolvedNextFollowup,
      hasFollowupInterval,
      normalizedInterval,
      req.user.id,
      hasVisitFollowupEnabled,
      typeof visit_followup_enabled === 'boolean' ? visit_followup_enabled : null,
      hasVisitPauseUntil,
      visit_followup_pause_until || null,
      hasVisitPauseReason,
      visit_followup_pause_reason || null,
      hasNextVisitFollowup,
      resolvedNextVisitFollowup,
      hasVisitInterval,
      normalizedVisitInterval,
    ])

    if (!result.rows.length) {
      return res.status(404).json({ error: 'ไม่พบข้อมูล CRM ของลูกค้านี้' })
    }
    res.json(result.rows[0])
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
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
      INSERT INTO crm_customer_profile (ar_code, customer_type, status, priority, source, crm_remark, followup_enabled)
      VALUES ($1,$2,$3,$4,$5,$6,FALSE)
      ON CONFLICT (ar_code) DO NOTHING
    `, [
      code,
      crm.customer_type || 'B2C',
      crm.status || 'active',
      crm.priority || 'normal',
      crm.source || null,
      crm.crm_remark || null
    ])

    // Assign CRM owners (1 primary + co-owners)
    await replaceCustomerOwners(crmClient, code, normalizeCustomerOwners(crm), req.user?.id)

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
// POST /api/customers/:code/shop-image  — อัปโหลดรูปร้านค้า
// ─────────────────────────────────────────────
router.post('/:code/shop-image', uploadMem.single('image'), async (req, res) => {
  const { code } = req.params
  if (!req.file) return res.status(400).json({ error: 'ไม่พบไฟล์รูป' })
  try {
    // ลบรูปเก่าก่อน
    const existing = await crmDB.query(
      `SELECT shop_image FROM crm_customer_profile WHERE ar_code = $1`, [code]
    )
    const oldRel = existing.rows[0]?.shop_image
    if (oldRel) {
      const absOld = nodePath.join(__dirname, '../uploads', oldRel)
      if (fs.existsSync(absOld)) fs.unlinkSync(absOld)
    }

    const dir = nodePath.join(__dirname, '../uploads/customers', code)
    fs.mkdirSync(dir, { recursive: true })

    const filename = `${Date.now()}_${Math.random().toString(36).slice(2)}.webp`
    const relPath  = `customers/${code}/${filename}`
    const absPath  = nodePath.join(dir, filename)

    await sharp(req.file.buffer)
      .rotate()
      .resize(1920, 1920, { fit: 'inside', withoutEnlargement: true })
      .webp({ quality: 85 })
      .toFile(absPath)

    await crmDB.query(`
      INSERT INTO crm_customer_profile (ar_code, shop_image)
      VALUES ($1, $2)
      ON CONFLICT (ar_code) DO UPDATE SET shop_image = EXCLUDED.shop_image, updated_at = NOW()
    `, [code, relPath])

    res.json({ success: true, shop_image: relPath })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// DELETE /api/customers/:code/shop-image  — ลบรูปร้านค้า
// ─────────────────────────────────────────────
router.delete('/:code/shop-image', async (req, res) => {
  const { code } = req.params
  try {
    const existing = await crmDB.query(
      `SELECT shop_image FROM crm_customer_profile WHERE ar_code = $1`, [code]
    )
    const oldRel = existing.rows[0]?.shop_image
    if (oldRel) {
      const absOld = nodePath.join(__dirname, '../uploads', oldRel)
      if (fs.existsSync(absOld)) fs.unlinkSync(absOld)
    }
    await crmDB.query(
      `UPDATE crm_customer_profile SET shop_image = NULL, updated_at = NOW() WHERE ar_code = $1`, [code]
    )
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

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
      INSERT INTO crm_customer_profile (ar_code, customer_type, status, priority, source, crm_remark, next_followup, followup_enabled)
      VALUES ($1,$2,$3,$4,$5,$6,$7,FALSE)
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

    // Update CRM owners (clear + replace เพื่อให้ลบ/เพิ่ม co-owner ได้ตรง UI)
    await replaceCustomerOwners(crmClient, code, normalizeCustomerOwners(crm), req.user?.id)

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

// ─────────────────────────────────────────────
// GET /api/customers/:code/credit-detail  — หนี้คงค้าง
// ─────────────────────────────────────────────
router.get('/:code/credit-detail', async (req, res) => {
  const custCode = (req.params.code || '').trim()
  if (!custCode) return res.json({ success: false })

  const client = await posDB.connect()
  try {
    // Query 1: head summary
    const query1 = `
      SELECT code, name_1,
        COALESCE((SELECT credit_money FROM ar_customer_detail WHERE ar_customer_detail.ar_code = ar_customer.code), 0) AS credit_money,
        (SELECT COALESCE(SUM(amount), 0) FROM (
          SELECT roworder, 1 AS calc_type, doc_no, cust_code, total_amount AS amount FROM ic_trans
            WHERE last_status = 0 AND ((trans_flag = 418 OR trans_flag = 44 OR trans_flag = 250) AND inquiry_type IN (0, 2))
            AND ar_customer.code = ic_trans.cust_code
          UNION ALL SELECT roworder, 1 AS calc_type, doc_no, cust_code, total_amount AS amount FROM ic_trans
            WHERE last_status = 0 AND (trans_flag IN (93, 99))
            AND ar_customer.code = ic_trans.cust_code
          UNION ALL SELECT roworder, 2 AS calc_type, doc_no, cust_code, total_amount AS amount FROM ic_trans
            WHERE last_status = 0 AND (trans_flag IN (46, 95, 101))
            AND ar_customer.code = ic_trans.cust_code
          UNION ALL SELECT roworder, 3 AS calc_type, doc_no, cust_code, -1 * total_amount AS amount FROM ic_trans
            WHERE last_status = 0 AND (trans_flag = 48 AND inquiry_type IN (0, 2, 4))
            AND ar_customer.code = ic_trans.cust_code
          UNION ALL SELECT roworder, 3 AS calc_type, doc_no, cust_code, -1 * total_amount AS amount FROM ic_trans
            WHERE last_status = 0 AND (trans_flag IN (97, 103, 252))
            AND ar_customer.code = ic_trans.cust_code
          UNION ALL SELECT roworder, 4 AS calc_type, doc_no, cust_code, -1 * total_net_value AS amount FROM ap_ar_trans
            WHERE last_status = 0 AND trans_flag = 239
            AND ar_customer.code = ap_ar_trans.cust_code
        ) AS temp6) AS balance_end
      FROM ar_customer
      WHERE code = $1`

    const result1 = await client.query(query1, [custCode])
    const dataHead = { credit_money: '0', balance_end: '0' }
    if (result1.rows.length > 0) {
      const row = result1.rows[0]
      dataHead.credit_money = Number(row.credit_money).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      dataHead.balance_end  = Number(row.balance_end).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }

    // Query 2: transaction detail (data_1)
    const query2 = `
      SELECT * FROM (
        SELECT cust_code AS ar_code, doc_no, doc_date, due_date, amount, doc_type,
          used_status AS status, balance_amount AS ar_balance, remark
        FROM (
          SELECT cust_code, doc_date,
            CASE WHEN (trans_flag IN (93, 95, 97)) THEN due_date WHEN (trans_flag = 418) THEN doc_date ELSE credit_date END AS due_date,
            doc_no, trans_flag AS doc_type, used_status, tax_doc_no, tax_doc_date, credit_day,
            COALESCE(total_amount, 0) AS amount,
            COALESCE(total_amount, 0) - (
              SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0) + COALESCE(lost_profit_exchange_amount, 0)), 0)
              FROM ap_ar_trans_detail
              WHERE COALESCE(last_status, 0) = 0 AND trans_flag IN (239)
                AND ic_trans.doc_no = ap_ar_trans_detail.billing_no
                AND ic_trans.trans_flag = ap_ar_trans_detail.bill_type
                AND doc_date <= DATE('2035-11-11')
            ) AS balance_amount,
            branch_code, remark
          FROM ic_trans
          WHERE COALESCE(last_status, 0) = 0 AND trans_flag = 44 AND (inquiry_type = 0 OR inquiry_type = 2)
            AND doc_date <= DATE('2035-11-11') AND cust_code = $1

          UNION ALL
          SELECT cust_code, doc_date,
            CASE WHEN (trans_flag IN (93, 95, 97)) THEN due_date WHEN (trans_flag = 418) THEN doc_date ELSE credit_date END AS due_date,
            doc_no, trans_flag AS doc_type, used_status, tax_doc_no, tax_doc_date, credit_day,
            COALESCE(total_amount, 0) AS amount,
            COALESCE(total_amount, 0) - (
              SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)
              FROM ap_ar_trans_detail
              WHERE COALESCE(last_status, 0) = 0 AND trans_flag IN (239)
                AND ic_trans.doc_no = ap_ar_trans_detail.billing_no
                AND ic_trans.trans_flag = ap_ar_trans_detail.bill_type
                AND doc_date <= DATE('2035-11-11')
            ) AS balance_amount,
            branch_code, remark
          FROM ic_trans
          WHERE COALESCE(last_status, 0) = 0
            AND (trans_flag = 46 OR trans_flag = 93 OR trans_flag = 99 OR trans_flag = 95
              OR trans_flag = 101 OR trans_flag = 418 OR ((trans_flag = 250 OR trans_flag = 254) AND (inquiry_type IN (0, 2))))
            AND doc_date <= DATE('2035-11-11') AND cust_code = $1

          UNION ALL
          SELECT cust_code, doc_date,
            CASE WHEN (trans_flag IN (93, 95, 97)) THEN due_date WHEN (trans_flag = 418) THEN doc_date ELSE credit_date END AS due_date,
            doc_no, trans_flag AS doc_type, used_status, tax_doc_no, tax_doc_date, credit_day,
            -1 * COALESCE(total_amount, 0) AS amount,
            -1 * (COALESCE(total_amount, 0) + (
              SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0)), 0)
              FROM ap_ar_trans_detail
              WHERE COALESCE(last_status, 0) = 0 AND trans_flag IN (239)
                AND ic_trans.doc_no = ap_ar_trans_detail.billing_no
                AND ic_trans.trans_flag = ap_ar_trans_detail.bill_type
                AND doc_date <= DATE('2035-11-11')
            )) AS balance_amount,
            branch_code, remark
          FROM ic_trans
          WHERE COALESCE(last_status, 0) = 0
            AND ((trans_flag = 48 AND inquiry_type IN (0, 2, 4)) OR trans_flag = 97 OR (trans_flag = 252 AND inquiry_type IN (0, 2)) OR trans_flag = 103)
            AND doc_date <= DATE('2035-11-11') AND cust_code = $1

          UNION ALL
          SELECT cust_code, doc_date, due_date, doc_no, trans_flag AS doc_type, used_status, tax_doc_no, tax_doc_date, credit_day,
            COALESCE(total_amount, 0) AS amount,
            COALESCE(total_amount, 0) - (
              SELECT COALESCE(SUM(COALESCE(sum_pay_money, 0) + COALESCE(lost_profit_exchange_amount, 0)), 0)
              FROM ap_ar_trans_detail
              WHERE COALESCE(last_status, 0) = 0 AND trans_flag IN (239)
                AND ic_trans.doc_no = ap_ar_trans_detail.billing_no
                AND ic_trans.trans_flag = ap_ar_trans_detail.bill_type
            ) AS balance_amount,
            branch_code, remark
          FROM as_trans AS ic_trans
          WHERE trans_flag = 1802 AND inquiry_type IN (0, 2) AND doc_date <= DATE('2035-11-11')
        ) AS xdoc
        WHERE balance_amount <> 0 AND cust_code = $1
      ) AS outer_query`

    const result2 = await client.query(query2, [custCode])
    let sumStatus = 0
    const data1 = result2.rows.map(row => {
      sumStatus += Number(row.amount)
      return {
        doc_date:   row.doc_date,
        doc_no:     row.doc_no,
        due_date:   row.due_date,
        doc_type:   row.doc_type,
        amount:     row.amount,
        ar_balance: row.ar_balance,
        remark:     row.remark
      }
    })

    // Query 3: cheque list (data_2)
    const query3 = `
      SELECT chq_number, chq_get_date, doc_ref, chq_due_date, amount,
        (CASE
          WHEN (status = 0) THEN 'เช็คในมือ'
          WHEN (status = 1) THEN 'เช็คนำฝาก'
          WHEN (status = 2) THEN 'เช็คผ่าน'
          WHEN (status = 3) THEN 'เช็ครับคืน'
          WHEN (status = 4) THEN 'เช็คยกเลิก'
          WHEN (status = 5) THEN 'เช็คขายลด'
          WHEN (status = 6) THEN 'เช็คคืนนำเข้าใหม่'
          WHEN (status = 7) THEN 'เช็คเปลี่ยน'
          ELSE '' END) AS status
      FROM cb_chq_list
      WHERE chq_type = 1 AND status != 2 AND status != 8 AND status != 7 AND ap_ar_code = $1`

    const result3 = await client.query(query3, [custCode])
    let sumCheque = 0
    const data2 = result3.rows.map(row => {
      sumCheque += Number(row.amount)
      return {
        chq_number:   row.chq_number,
        chq_get_date: row.chq_get_date,
        doc_ref:      row.doc_ref,
        chq_due_date: row.chq_due_date,
        amount:       row.amount,
        status:       row.status
      }
    })

    // Query 4: SR/SS documents (data_3)
    const query4 = `
      SELECT * FROM (
        SELECT doc_no, doc_date, trans_flag,
          COALESCE(remark, '') AS remark,
          (total_amount - COALESCE((
            SELECT SUM(sum_amount) FROM ic_trans_detail AS x
            WHERE x.trans_flag IN (44, 39, 36) AND x.last_status = 0 AND x.ref_doc_no = ic_trans.doc_no
          ), 0)) AS total_amount
        FROM ic_trans
        WHERE trans_flag = 34 AND last_status = 0 AND inquiry_type IN (0, 2)
          AND doc_success = 0 AND approve_status IN (0, 1) AND ic_trans.cust_code = $1

        UNION ALL
        SELECT doc_no, doc_date, trans_flag,
          COALESCE(remark, '') AS remark,
          (total_amount - COALESCE((
            SELECT SUM(sum_amount) FROM ic_trans_detail AS x
            WHERE x.trans_flag IN (44, 37) AND x.last_status = 0 AND x.ref_doc_no = ic_trans.doc_no
          ), 0)) AS total_amount
        FROM ic_trans
        WHERE trans_flag = 36 AND last_status = 0 AND inquiry_type IN (0, 2)
          AND doc_success = 0 AND approve_status IN (0, 1) AND ic_trans.cust_code = $1
      ) AS temp1
      WHERE temp1.total_amount > 0
      ORDER BY doc_date, doc_no`

    const result4 = await client.query(query4, [custCode])
    let sumSr = 0, sumSS = 0
    const data3 = result4.rows.map(row => {
      if (String(row.trans_flag) === '36') sumSr += Number(row.total_amount)
      else if (String(row.trans_flag) === '34') sumSS += Number(row.total_amount)
      return {
        doc_no:       row.doc_no,
        doc_date:     row.doc_date,
        trans_flag:   row.trans_flag,
        total_amount: row.total_amount,
        remark:       row.remark
      }
    })

    const fmt = v => Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    dataHead.sum_cheque = fmt(sumCheque)
    dataHead.sum_sr     = fmt(sumSr)
    dataHead.sum_ss     = fmt(sumSS)
    dataHead.sumsrss    = fmt(sumSr + sumSS)
    dataHead.sum_status = fmt(sumStatus)

    res.json({ success: true, data_head: dataHead, data_1: data1, data_2: data2, data_3: data3 })
  } catch (err) {
    console.error('credit-detail error:', err)
    res.status(500).json({ success: false, error: err.message })
  } finally {
    client.release()
  }
})

module.exports = router
