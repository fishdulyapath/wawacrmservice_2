const express = require('express')
const router  = express.Router()
const { crmDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

router.use(authMiddleware)

// ─────────────────────────────────────────────────────────────
// GET /api/reports/summary
// ภาพรวมกิจกรรมแยก status / type / priority
// query: date_from, date_to, owner_id
// NOTE: status ใช้ derived status (open ถ้ายังมี owner เปิดอยู่, done ถ้าทุกคน done)
// ─────────────────────────────────────────────────────────────
router.get('/summary', requireRole('admin', 'manager', 'supervisor'), async (req, res) => {
  try {
    const { date_from, date_to, owner_id } = req.query
    const params = []
    const conds  = []

    if (date_from) { params.push(date_from); conds.push(`a.created_at >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`a.created_at <  ($${params.length}::date + INTERVAL '1 day')`) }
    if (owner_id)  {
      params.push(parseInt(owner_id))
      conds.push(`EXISTS (
        SELECT 1 FROM crm_activity_owners ao2
        WHERE ao2.activity_id = a.id AND ao2.user_id = $${params.length} AND ao2.removed_at IS NULL
      )`)
    }

    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : ''

    // derived_status: open ถ้ายังมี owner status='open', done ถ้าทุกคน done, cancelled ถ้าทุกคน cancelled
    const derivedStatus = `
      CASE
        WHEN EXISTS (
          SELECT 1 FROM crm_activity_owners ao
          WHERE ao.activity_id = a.id AND ao.status = 'open' AND ao.removed_at IS NULL
        ) THEN 'open'
        WHEN NOT EXISTS (
          SELECT 1 FROM crm_activity_owners ao
          WHERE ao.activity_id = a.id AND ao.status != 'cancelled' AND ao.removed_at IS NULL
        ) THEN 'cancelled'
        ELSE 'done'
      END
    `

    const [byStatus, byType, byPriority, callResults, total] = await Promise.all([
      crmDB.query(`
        SELECT (${derivedStatus}) AS status, COUNT(*) AS count
        FROM crm_activities a ${where}
        GROUP BY 1 ORDER BY count DESC`, params),

      crmDB.query(`
        SELECT activity_type, COUNT(*) AS count
        FROM crm_activities a ${where}
        GROUP BY activity_type ORDER BY count DESC`, params),

      crmDB.query(`
        SELECT priority, COUNT(*) AS count
        FROM crm_activities a ${where}
        GROUP BY priority ORDER BY count DESC`, params),

      crmDB.query(`
        SELECT call_result, COUNT(*) AS count
        FROM crm_activities a
        WHERE a.activity_type = 'call' AND a.call_result IS NOT NULL
        ${conds.length ? 'AND ' + conds.join(' AND ') : ''}
        GROUP BY call_result ORDER BY count DESC`, params),

      crmDB.query(`SELECT COUNT(*) AS count FROM crm_activities a ${where}`, params),
    ])

    res.json({
      total: parseInt(total.rows[0].count),
      by_status:   byStatus.rows.map(r => ({ ...r, count: parseInt(r.count) })),
      by_type:     byType.rows.map(r => ({ ...r, count: parseInt(r.count) })),
      by_priority: byPriority.rows.map(r => ({ ...r, count: parseInt(r.count) })),
      call_results: callResults.rows.map(r => ({ ...r, count: parseInt(r.count) })),
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/reports/by-owner
// สรุปงานแยกตามพนักงาน (multi-owner: นับทุก activity ที่ user เป็น owner)
// KPI: total, open, done, cancelled, calls, meetings, tasks, avg_call_sec, done_rate
// ─────────────────────────────────────────────────────────────
router.get('/by-owner', requireRole('admin', 'manager', 'supervisor'), async (req, res) => {
  try {
    const { date_from, date_to } = req.query
    const params = []
    const conds  = []

    if (date_from) { params.push(date_from); conds.push(`a.created_at >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`a.created_at <  ($${params.length}::date + INTERVAL '1 day')`) }

    const where = conds.length ? 'AND ' + conds.join(' AND ') : ''

    // Join via crm_activity_owners for multi-owner support
    // ao.status = สถานะของ user นั้นๆ ต่อ activity นั้น (per-user KPI)
    const result = await crmDB.query(`
      SELECT
        u.id, u.name, u.code, u.role,
        COUNT(DISTINCT a.id)                                                    AS total,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'open')                  AS open,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'done')                  AS done,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'cancelled')             AS cancelled,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'call')            AS calls,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'meeting')         AS meetings,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'task')            AS tasks,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.is_primary = TRUE)                AS primary_count,
        ROUND(AVG(a.duration_sec) FILTER (
          WHERE a.activity_type = 'call' AND a.duration_sec > 0
        ))                                                                       AS avg_call_sec
      FROM crm_users u
      LEFT JOIN crm_activity_owners ao
        ON ao.user_id = u.id AND ao.removed_at IS NULL
      LEFT JOIN crm_activities a
        ON a.id = ao.activity_id ${where}
      WHERE u.is_active = TRUE
      GROUP BY u.id, u.name, u.code, u.role
      ORDER BY total DESC
    `, params)

    res.json(result.rows.map(r => {
      const total    = parseInt(r.total) || 0
      const done     = parseInt(r.done)  || 0
      return {
        ...r,
        total,
        open:          parseInt(r.open)          || 0,
        done,
        cancelled:     parseInt(r.cancelled)     || 0,
        calls:         parseInt(r.calls)         || 0,
        meetings:      parseInt(r.meetings)      || 0,
        tasks:         parseInt(r.tasks)         || 0,
        primary_count: parseInt(r.primary_count) || 0,
        avg_call_sec:  r.avg_call_sec ? parseInt(r.avg_call_sec) : null,
        done_rate:     total > 0 ? Math.round((done / total) * 100) : 0,
      }
    }))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/reports/trend
// แนวโน้มกิจกรรมรายวัน/รายสัปดาห์ 30 วันย้อนหลัง
// query: period = day | week | month
// ─────────────────────────────────────────────────────────────
router.get('/trend', requireRole('admin', 'manager', 'supervisor'), async (req, res) => {
  try {
    const { period = 'day', date_from, date_to, owner_id } = req.query
    const params = []
    const conds  = []

    if (date_from) { params.push(date_from); conds.push(`a.created_at >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`a.created_at <  ($${params.length}::date + INTERVAL '1 day')`) }
    if (owner_id)  {
      params.push(parseInt(owner_id))
      conds.push(`EXISTS (
        SELECT 1 FROM crm_activity_owners ao2
        WHERE ao2.activity_id = a.id AND ao2.user_id = $${params.length} AND ao2.removed_at IS NULL
      )`)
    }

    const trunc = period === 'week' ? 'week' : period === 'month' ? 'month' : 'day'
    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : ''

    // derived done: all active owners are done
    const result = await crmDB.query(`
      SELECT
        DATE_TRUNC('${trunc}', a.created_at)::date AS period,
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE a.activity_type = 'task')    AS tasks,
        COUNT(*) FILTER (WHERE a.activity_type = 'call')    AS calls,
        COUNT(*) FILTER (WHERE a.activity_type = 'meeting') AS meetings,
        COUNT(*) FILTER (WHERE NOT EXISTS (
          SELECT 1 FROM crm_activity_owners ao
          WHERE ao.activity_id = a.id AND ao.status = 'open' AND ao.removed_at IS NULL
        ) AND EXISTS (
          SELECT 1 FROM crm_activity_owners ao
          WHERE ao.activity_id = a.id AND ao.status = 'done' AND ao.removed_at IS NULL
        )) AS done
      FROM crm_activities a
      ${where}
      GROUP BY 1
      ORDER BY 1 ASC
    `, params)

    res.json(result.rows.map(r => ({
      period: r.period,
      total: parseInt(r.total), tasks: parseInt(r.tasks),
      calls: parseInt(r.calls), meetings: parseInt(r.meetings),
      done: parseInt(r.done),
    })))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/reports/audit
// ประวัติการแก้ไข (audit log) พร้อม pagination
// ─────────────────────────────────────────────────────────────
router.get('/audit', requireRole('admin', 'manager', 'supervisor'), async (req, res) => {
  try {
    const { page = 1, limit = 30, table_name, action, user_code, date_from, date_to } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)
    const params = []
    const conds  = []

    if (table_name) { params.push(table_name); conds.push(`l.table_name = $${params.length}`) }
    if (action)     { params.push(action);      conds.push(`l.action = $${params.length}`) }
    if (user_code)  { params.push(`%${user_code}%`); conds.push(`l.user_code ILIKE $${params.length}`) }
    if (date_from)  { params.push(date_from);   conds.push(`l.created_at >= $${params.length}::date`) }
    if (date_to)    { params.push(date_to);     conds.push(`l.created_at <  ($${params.length}::date + INTERVAL '1 day')`) }

    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : ''

    const countRes = await crmDB.query(`SELECT COUNT(*) FROM crm_audit_log l ${where}`, params)
    const total    = parseInt(countRes.rows[0].count)

    params.push(parseInt(limit), offset)
    const dataRes = await crmDB.query(`
      SELECT l.id, l.table_name, l.record_id, l.ar_code, l.action,
             l.changed_fields, l.user_code, l.ip_address, l.created_at,
             u.name AS user_name
      FROM crm_audit_log l
      LEFT JOIN crm_users u ON u.id = l.user_id
      ${where}
      ORDER BY l.created_at DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    res.json({
      data: dataRes.rows,
      pagination: {
        total, page: parseInt(page), limit: parseInt(limit),
        pages: Math.ceil(total / parseInt(limit)),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/reports/kpi
// KPI รายบุคคล: done_rate, avg_close_days, overdue_rate, call_count
// query: date_from, date_to, user_id
// ─────────────────────────────────────────────────────────────
router.get('/kpi', requireRole('admin', 'manager', 'supervisor'), async (req, res) => {
  try {
    const { date_from, date_to, user_id } = req.query
    const params = []
    const conds  = []

    if (date_from) { params.push(date_from); conds.push(`a.created_at >= $${params.length}::date`) }
    if (date_to)   { params.push(date_to);   conds.push(`a.created_at <  ($${params.length}::date + INTERVAL '1 day')`) }
    if (user_id)   { params.push(parseInt(user_id)); conds.push(`ao.user_id = $${params.length}`) }

    const where = conds.length ? 'AND ' + conds.join(' AND ') : ''

    const result = await crmDB.query(`
      SELECT
        u.id, u.name, u.code, u.role,
        COUNT(DISTINCT a.id)                                                   AS total,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'done')                 AS done,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'open')                 AS open,
        COUNT(DISTINCT a.id) FILTER (WHERE ao.status = 'cancelled')            AS cancelled,
        COUNT(DISTINCT a.id) FILTER (WHERE
          a.due_date < CURRENT_DATE AND ao.status = 'open'
        )                                                                       AS overdue,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'call')           AS calls,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'meeting')        AS meetings,
        COUNT(DISTINCT a.id) FILTER (WHERE a.activity_type = 'task')           AS tasks,
        ROUND(AVG(a.duration_sec) FILTER (
          WHERE a.activity_type = 'call' AND a.duration_sec > 0
        ))                                                                       AS avg_call_sec,
        ROUND(AVG(
          EXTRACT(EPOCH FROM (ao.assigned_at - a.created_at)) / 86400.0
        ) FILTER (WHERE ao.status = 'done'))                                    AS avg_assign_to_done_days,
        COUNT(DISTINCT a.ar_code) FILTER (WHERE a.ar_code IS NOT NULL)         AS unique_customers
      FROM crm_users u
      JOIN crm_activity_owners ao
        ON ao.user_id = u.id AND ao.removed_at IS NULL
      JOIN crm_activities a
        ON a.id = ao.activity_id ${where}
      WHERE u.is_active = TRUE
      GROUP BY u.id, u.name, u.code, u.role
      ORDER BY done DESC, total DESC
    `, params)

    res.json(result.rows.map(r => {
      const total    = parseInt(r.total)    || 0
      const done     = parseInt(r.done)     || 0
      const overdue  = parseInt(r.overdue)  || 0
      const open     = parseInt(r.open)     || 0
      return {
        id:            r.id,
        name:          r.name,
        code:          r.code,
        role:          r.role,
        total,
        done,
        open,
        cancelled:     parseInt(r.cancelled) || 0,
        overdue,
        calls:         parseInt(r.calls)     || 0,
        meetings:      parseInt(r.meetings)  || 0,
        tasks:         parseInt(r.tasks)     || 0,
        unique_customers: parseInt(r.unique_customers) || 0,
        avg_call_sec:  r.avg_call_sec ? parseInt(r.avg_call_sec) : null,
        // KPI ratios
        done_rate:          total > 0 ? Math.round((done / total) * 100)   : 0,
        overdue_rate:       open  > 0 ? Math.round((overdue / open) * 100) : 0,
        avg_assign_to_done_days: r.avg_assign_to_done_days
          ? parseFloat(r.avg_assign_to_done_days)
          : null,
      }
    }))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
