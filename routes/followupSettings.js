const express = require('express')
const router = express.Router()
const { crmDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

router.use(authMiddleware)

const DEFAULT_SETTINGS = {
  id: 1,
  enabled: false,
  auto_create_enabled: true,
  default_call_interval_days: 30,
  auto_create_time: '08:00',
  assignment_mode: 'primary',
  no_owner_action: 'queue',
  no_answer_max_attempts_per_day: 3,
  no_answer_retry_minutes: 30,
  business_start_time: '08:30',
  business_end_time: '17:30',
}

function toBool(value, fallback) {
  if (typeof value === 'boolean') return value
  if (value === 'true') return true
  if (value === 'false') return false
  return fallback
}

function toInt(value, fallback, min, max) {
  const n = Number(value)
  if (!Number.isInteger(n)) return fallback
  return Math.min(Math.max(n, min), max)
}

function normalizeTime(value, fallback = '08:00') {
  if (typeof value !== 'string') return fallback
  return /^([01]\d|2[0-3]):[0-5]\d$/.test(value) ? value : null
}

function normalizeSettings(row = {}) {
  return {
    id: row.id || DEFAULT_SETTINGS.id,
    enabled: row.enabled ?? DEFAULT_SETTINGS.enabled,
    auto_create_enabled: row.auto_create_enabled ?? DEFAULT_SETTINGS.auto_create_enabled,
    default_call_interval_days: row.default_call_interval_days || DEFAULT_SETTINGS.default_call_interval_days,
    auto_create_time: String(row.auto_create_time || DEFAULT_SETTINGS.auto_create_time).slice(0, 5),
    assignment_mode: row.assignment_mode || DEFAULT_SETTINGS.assignment_mode,
    no_owner_action: row.no_owner_action || DEFAULT_SETTINGS.no_owner_action,
    no_answer_max_attempts_per_day: row.no_answer_max_attempts_per_day || DEFAULT_SETTINGS.no_answer_max_attempts_per_day,
    no_answer_retry_minutes: row.no_answer_retry_minutes || DEFAULT_SETTINGS.no_answer_retry_minutes,
    business_start_time: String(row.business_start_time || DEFAULT_SETTINGS.business_start_time).slice(0, 5),
    business_end_time: String(row.business_end_time || DEFAULT_SETTINGS.business_end_time).slice(0, 5),
    last_auto_create_checked_at: row.last_auto_create_checked_at || null,
    last_auto_create_created_count: row.last_auto_create_created_count || 0,
    last_auto_create_unassigned_count: row.last_auto_create_unassigned_count || 0,
    last_auto_create_error: row.last_auto_create_error || null,
    updated_at: row.updated_at || null,
    updated_by: row.updated_by || null,
  }
}

function nextBangkokRun(autoCreateTime) {
  const hhmm = String(autoCreateTime || DEFAULT_SETTINGS.auto_create_time).slice(0, 5)
  const [h, m] = hhmm.split(':').map(Number)
  const now = new Date()
  const dateText = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Bangkok', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(now)
  let next = new Date(`${dateText}T${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:00+07:00`)
  if (next <= now) next = new Date(next.getTime() + 86400000)
  return next.toISOString()
}

async function withAutomationMeta(settings) {
  const [lastRunRes, unassignedRes] = await Promise.all([
    crmDB.query(`
      SELECT MAX(created_at) AS last_auto_create_at
      FROM crm_activities
      WHERE system_created = TRUE AND followup_type = 'scheduled'
    `),
    crmDB.query(`
      SELECT COUNT(*)::int AS unassigned_followup_count
      FROM crm_activities
      WHERE requires_owner_assignment = TRUE
        AND status NOT IN ('done','cancelled','deleted')
    `),
  ])
  return {
    ...settings,
    next_auto_create_at: settings.enabled && settings.auto_create_enabled
      ? nextBangkokRun(settings.auto_create_time)
      : null,
    last_auto_create_at: lastRunRes.rows[0]?.last_auto_create_at || null,
    last_auto_create_checked_at: settings.last_auto_create_checked_at,
    last_auto_create_created_count: settings.last_auto_create_created_count,
    last_auto_create_unassigned_count: settings.last_auto_create_unassigned_count,
    last_auto_create_error: settings.last_auto_create_error,
    unassigned_followup_count: unassignedRes.rows[0]?.unassigned_followup_count || 0,
  }
}

async function ensureSettings() {
  const result = await crmDB.query(`
    INSERT INTO crm_followup_settings (
      id, enabled, auto_create_enabled, default_call_interval_days, auto_create_time,
      assignment_mode, no_owner_action, no_answer_max_attempts_per_day, no_answer_retry_minutes,
      business_start_time, business_end_time
    )
    VALUES (1,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    ON CONFLICT (id) DO NOTHING
    RETURNING *
  `, [
    DEFAULT_SETTINGS.enabled,
    DEFAULT_SETTINGS.auto_create_enabled,
    DEFAULT_SETTINGS.default_call_interval_days,
    DEFAULT_SETTINGS.auto_create_time,
    DEFAULT_SETTINGS.assignment_mode,
    DEFAULT_SETTINGS.no_owner_action,
    DEFAULT_SETTINGS.no_answer_max_attempts_per_day,
    DEFAULT_SETTINGS.no_answer_retry_minutes,
    DEFAULT_SETTINGS.business_start_time,
    DEFAULT_SETTINGS.business_end_time,
  ])
  if (result.rows.length) return normalizeSettings(result.rows[0])

  const existing = await crmDB.query(`SELECT * FROM crm_followup_settings WHERE id = 1`)
  return normalizeSettings(existing.rows[0])
}

router.get('/', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const settings = await ensureSettings()
    res.json(await withAutomationMeta(settings))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.put('/', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const autoCreateTime = normalizeTime(req.body.auto_create_time, DEFAULT_SETTINGS.auto_create_time)
    const businessStartTime = normalizeTime(req.body.business_start_time, DEFAULT_SETTINGS.business_start_time)
    const businessEndTime = normalizeTime(req.body.business_end_time, DEFAULT_SETTINGS.business_end_time)
    if (!autoCreateTime) {
      return res.status(400).json({ error: 'เวลาสร้างงานอัตโนมัติต้องอยู่ในรูปแบบ HH:MM' })
    }
    if (!businessStartTime || !businessEndTime || businessStartTime >= businessEndTime) {
      return res.status(400).json({ error: 'เวลาทำงานต้องถูกต้อง และเวลาเริ่มต้องน้อยกว่าเวลาสิ้นสุด' })
    }

    const assignmentMode = ['primary', 'all'].includes(req.body.assignment_mode)
      ? req.body.assignment_mode
      : DEFAULT_SETTINGS.assignment_mode
    const noOwnerAction = ['queue'].includes(req.body.no_owner_action)
      ? req.body.no_owner_action
      : DEFAULT_SETTINGS.no_owner_action

    const payload = {
      enabled: toBool(req.body.enabled, DEFAULT_SETTINGS.enabled),
      auto_create_enabled: toBool(req.body.auto_create_enabled, DEFAULT_SETTINGS.auto_create_enabled),
      default_call_interval_days: toInt(req.body.default_call_interval_days, DEFAULT_SETTINGS.default_call_interval_days, 1, 365),
      auto_create_time: autoCreateTime,
      assignment_mode: assignmentMode,
      no_owner_action: noOwnerAction,
      no_answer_max_attempts_per_day: toInt(req.body.no_answer_max_attempts_per_day, DEFAULT_SETTINGS.no_answer_max_attempts_per_day, 1, 10),
      no_answer_retry_minutes: toInt(req.body.no_answer_retry_minutes, DEFAULT_SETTINGS.no_answer_retry_minutes, 5, 480),
      business_start_time: businessStartTime,
      business_end_time: businessEndTime,
    }

    const result = await crmDB.query(`
      INSERT INTO crm_followup_settings (
        id, enabled, auto_create_enabled, default_call_interval_days, auto_create_time,
        assignment_mode, no_owner_action, no_answer_max_attempts_per_day, no_answer_retry_minutes,
        business_start_time, business_end_time,
        updated_by, updated_at
      )
      VALUES (1,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (id) DO UPDATE SET
        enabled = EXCLUDED.enabled,
        auto_create_enabled = EXCLUDED.auto_create_enabled,
        default_call_interval_days = EXCLUDED.default_call_interval_days,
        auto_create_time = EXCLUDED.auto_create_time,
        assignment_mode = EXCLUDED.assignment_mode,
        no_owner_action = EXCLUDED.no_owner_action,
        no_answer_max_attempts_per_day = EXCLUDED.no_answer_max_attempts_per_day,
        no_answer_retry_minutes = EXCLUDED.no_answer_retry_minutes,
        business_start_time = EXCLUDED.business_start_time,
        business_end_time = EXCLUDED.business_end_time,
        updated_by = EXCLUDED.updated_by,
        updated_at = NOW()
      RETURNING *
    `, [
      payload.enabled,
      payload.auto_create_enabled,
      payload.default_call_interval_days,
      payload.auto_create_time,
      payload.assignment_mode,
      payload.no_owner_action,
      payload.no_answer_max_attempts_per_day,
      payload.no_answer_retry_minutes,
      payload.business_start_time,
      payload.business_end_time,
      req.user.id,
    ])

    res.json(await withAutomationMeta(normalizeSettings(result.rows[0])))
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
