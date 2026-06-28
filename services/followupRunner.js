const { crmDB } = require('../db')
const { notifyMany } = require('./notifyService')
const { ensureCustomerFollowupPolicy } = require('./followupPolicy')

const ACT_PREFIX = { call: 'C', meeting: 'M', task: 'W', transfer: 'O' }
async function generateActNo(activityType) {
  const prefix = ACT_PREFIX[activityType] || 'W'
  const bkkNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Bangkok' }))
  const dateStr = `${bkkNow.getFullYear()}${String(bkkNow.getMonth() + 1).padStart(2, '0')}${String(bkkNow.getDate()).padStart(2, '0')}`
  const lockKey = (prefix.charCodeAt(0) * 100000000 + parseInt(dateStr)) % 2147483647
  const lockClient = await crmDB.connect()
  try {
    await lockClient.query(`SELECT pg_advisory_lock($1)`, [lockKey])
    const res = await lockClient.query(
      `SELECT COUNT(*) AS cnt FROM crm_activities WHERE act_no LIKE $1`,
      [`${prefix}-${dateStr}-%`]
    )
    const next = parseInt(res.rows[0].cnt) + 1
    return `${prefix}-${dateStr}-${String(next).padStart(4, '0')}`
  } finally {
    try { await lockClient.query(`SELECT pg_advisory_unlock($1)`, [lockKey]) } catch {}
    lockClient.release()
  }
}

function bangkokDate() {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date())
}

async function loadFallbackUserId(client = crmDB) {
  const result = await client.query(`
    SELECT id FROM crm_users
    WHERE is_active = TRUE
      AND (UPPER(code) = 'SUPERADMIN' OR role IN ('admin','manager'))
    ORDER BY CASE WHEN UPPER(code) = 'SUPERADMIN' THEN 0 WHEN role = 'admin' THEN 1 ELSE 2 END, id
    LIMIT 1
  `)
  return result.rows[0]?.id || null
}

async function loadAdminNotifyIds(client = crmDB) {
  const result = await client.query(`
    SELECT id FROM crm_users
    WHERE is_active = TRUE
      AND (UPPER(code) = 'SUPERADMIN' OR role IN ('admin','manager','supervisor'))
  `)
  return result.rows.map(row => row.id)
}

async function runDueFollowups({
  settings,
  actorUserId = null,
  todayStr = bangkokDate(),
  limit = 100,
  updateSettingsStats = true,
  source = 'manual',
} = {}) {
  if (!settings) {
    const settingsRes = await crmDB.query(`SELECT * FROM crm_followup_settings WHERE id = 1`)
    settings = settingsRes.rows[0]
  }
  if (!settings) throw new Error('ไม่พบการตั้งค่า Follow-up Policy')
  if (settings.enabled !== true) {
    const err = new Error('Follow-up Policy ยังปิดอยู่')
    err.statusCode = 400
    throw err
  }

  await ensureCustomerFollowupPolicy()

  const fallbackUserId = actorUserId || await loadFallbackUserId()
  if (!fallbackUserId) {
    const message = 'No admin/manager fallback user found'
    if (updateSettingsStats) {
      await crmDB.query(`
        UPDATE crm_followup_settings
        SET last_auto_create_checked_at = NOW(),
            last_auto_create_created_count = 0,
            last_auto_create_unassigned_count = 0,
            last_auto_create_error = $1
        WHERE id = 1
      `, [message])
    }
    const err = new Error(message)
    err.statusCode = 500
    throw err
  }

  const customersRes = await crmDB.query(`
    SELECT
      p.ar_code,
      p.next_followup,
      p.next_followup::text AS next_followup_text,
      COALESCE(p.followup_interval_days, $2::integer) AS effective_call_interval_days
    FROM crm_customer_profile p
    WHERE p.status = 'active'
      AND p.followup_enabled = TRUE
      AND (p.followup_pause_until IS NULL OR p.followup_pause_until < $1::date)
      AND p.next_followup IS NOT NULL
      AND p.next_followup <= $1::date
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.followup_key = ('auto-followup:' || p.ar_code || ':' || p.next_followup::text)
      )
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.ar_code = p.ar_code
          AND a.followup_type = 'scheduled'
          AND a.status NOT IN ('done','cancelled','deleted')
          AND (
            a.requires_owner_assignment = TRUE
            OR EXISTS (
              SELECT 1 FROM crm_activity_owners ao
              WHERE ao.activity_id = a.id
                AND ao.removed_at IS NULL
                AND ao.status NOT IN ('done','cancelled')
            )
          )
      )
    ORDER BY p.next_followup ASC, p.ar_code ASC
    LIMIT $3
  `, [todayStr, settings.default_call_interval_days, limit])

  // Case B: next_followup เลยกำหนดแต่มี open scheduled activity ค้างอยู่
  // → อัปเดต next_followup ไปข้างหน้าเพื่อปลดล็อก ไม่ให้ติดซ้ำทุกวัน
  const stuckRes = await crmDB.query(`
    SELECT
      p.ar_code,
      COALESCE(p.followup_interval_days, $2::integer) AS effective_call_interval_days
    FROM crm_customer_profile p
    WHERE p.status = 'active'
      AND p.followup_enabled = TRUE
      AND (p.followup_pause_until IS NULL OR p.followup_pause_until < $1::date)
      AND p.next_followup IS NOT NULL
      AND p.next_followup <= $1::date
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.followup_key = ('auto-followup:' || p.ar_code || ':' || p.next_followup::text)
      )
      AND EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.ar_code = p.ar_code
          AND a.followup_type = 'scheduled'
          AND a.status NOT IN ('done','cancelled','deleted')
          AND (
            a.requires_owner_assignment = TRUE
            OR EXISTS (
              SELECT 1 FROM crm_activity_owners ao
              WHERE ao.activity_id = a.id
                AND ao.removed_at IS NULL
                AND ao.status NOT IN ('done','cancelled')
            )
          )
      )
    ORDER BY p.next_followup ASC, p.ar_code ASC
    LIMIT $3
  `, [todayStr, settings.default_call_interval_days, limit])

  const adminNotifyIds = await loadAdminNotifyIds()
  let createdCount = 0
  let unassignedCount = 0
  let firstError = null
  const createdActivities = []

  for (const customer of customersRes.rows) {
    const client = await crmDB.connect()
    try {
      await client.query('BEGIN')

      const ownerRes = await client.query(`
        SELECT o.user_id, o.is_primary
        FROM crm_customer_owner o
        JOIN crm_users u ON u.id = o.user_id
        WHERE o.ar_code = $1
          AND u.is_active = TRUE
        ORDER BY o.is_primary DESC, o.assigned_at ASC
      `, [customer.ar_code])

      const allOwnerIds = [...new Set(ownerRes.rows.map(row => Number(row.user_id)).filter(Boolean))]
      const selectedOwnerIds = settings.assignment_mode === 'all'
        ? allOwnerIds
        : (allOwnerIds.length ? [allOwnerIds[0]] : [])
      const primaryOwnerId = selectedOwnerIds[0] || fallbackUserId
      const requiresOwnerAssignment = selectedOwnerIds.length === 0
      const followupDateText = customer.next_followup_text || String(customer.next_followup).slice(0, 10)
      const followupKey = `auto-followup:${customer.ar_code}:${followupDateText}`
      const subject = `โทรติดตามลูกค้า ${customer.ar_code}`
      const description = [
        source === 'manual'
          ? 'สร้างโดย Manual Trigger จาก Follow-up Policy'
          : 'สร้างโดยระบบจาก Follow-up Policy',
        `ครบกำหนดติดตามวันที่ ${followupDateText}`,
        requiresOwnerAssignment ? 'ลูกค้ายังไม่มีทีมผู้ดูแล CRM กรุณาระบุผู้รับผิดชอบ' : '',
      ].filter(Boolean).join('\n')

      const act_no_followup = await generateActNo('call')
      const insertRes = await client.query(`
        INSERT INTO crm_activities (
          ar_code, owner_id, created_by, activity_type, subject, description,
          status, priority, due_date, start_datetime, call_direction, system_created,
          followup_type, followup_policy_id, followup_key,
          requires_owner_assignment, owner_assignment_note, act_no
        )
        VALUES ($1,$2,$3,'call',$4,$5,'open','normal',$6,($6::date + $10::time),'outbound',TRUE,
                'scheduled',1,$7,$8,$9,$11)
        ON CONFLICT (followup_key) WHERE followup_key IS NOT NULL DO NOTHING
        RETURNING *
      `, [
        customer.ar_code,
        primaryOwnerId,
        fallbackUserId,
        subject,
        description,
        customer.next_followup,
        followupKey,
        requiresOwnerAssignment,
        requiresOwnerAssignment ? 'ไม่มี CRM owner ตอนระบบสร้างงาน' : null,
        settings.auto_create_time,
        act_no_followup,
      ])

      if (!insertRes.rows.length) {
        await client.query('ROLLBACK')
        continue
      }

      const activity = insertRes.rows[0]
      for (const uid of selectedOwnerIds) {
        await client.query(`
          INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
          VALUES ($1,$2,$3,'open',$4)
          ON CONFLICT (activity_id, user_id) DO NOTHING
        `, [activity.id, uid, uid === primaryOwnerId, fallbackUserId])
      }

      await client.query(`
        UPDATE crm_customer_profile
        SET next_followup = (GREATEST(next_followup, $2::date) + ($3 || ' days')::INTERVAL)::date
        WHERE ar_code = $1
      `, [customer.ar_code, todayStr, customer.effective_call_interval_days])

      await client.query('COMMIT')
      createdCount++
      createdActivities.push({ id: activity.id, ar_code: customer.ar_code })

      if (requiresOwnerAssignment) {
        unassignedCount++
        if (adminNotifyIds.length) {
          await notifyMany(adminNotifyIds, {
            notiType: 'activity_update',
            title: 'มีงานติดตามที่ยังไม่มีผู้รับผิดชอบ',
            message: subject,
            refType: 'activity',
            refId: activity.id,
            arCode: customer.ar_code,
          })
        }
      } else {
        await notifyMany(selectedOwnerIds, {
          notiType: 'assigned',
          title: 'ถึงเวลาติดตามลูกค้า',
          message: subject,
          refType: 'activity',
          refId: activity.id,
          arCode: customer.ar_code,
        })
      }
    } catch (err) {
      await client.query('ROLLBACK')
      if (!firstError) firstError = `${customer.ar_code}: ${err.message}`
      console.error('[FollowupRunner]', customer.ar_code, err.message)
    } finally {
      client.release()
    }
  }

  // อัปเดต next_followup สำหรับลูกค้าที่ติดค้าง (Case B)
  for (const stuck of stuckRes.rows) {
    try {
      await crmDB.query(`
        UPDATE crm_customer_profile
        SET next_followup = ($2::date + ($3 || ' days')::INTERVAL)::date
        WHERE ar_code = $1
      `, [stuck.ar_code, todayStr, stuck.effective_call_interval_days])
      console.log('[FollowupRunner] stuck advance:', stuck.ar_code, '→ today+', stuck.effective_call_interval_days, 'days')
    } catch (err) {
      console.error('[FollowupRunner] stuck update error:', stuck.ar_code, err.message)
    }
  }

  if (updateSettingsStats) {
    await crmDB.query(`
      UPDATE crm_followup_settings
      SET last_auto_create_checked_at = NOW(),
          last_auto_create_created_count = $1,
          last_auto_create_unassigned_count = $2,
          last_auto_create_error = $3
      WHERE id = 1
    `, [createdCount, unassignedCount, firstError])
  }

  return {
    checked_date: todayStr,
    eligible_count: customersRes.rows.length,
    created_count: createdCount,
    unassigned_count: unassignedCount,
    error: firstError,
    created_activities: createdActivities,
  }
}

async function runDueVisitFollowups({
  settings,
  actorUserId = null,
  todayStr = bangkokDate(),
  limit = 100,
  updateSettingsStats = true,
  source = 'manual',
} = {}) {
  if (!settings) {
    const settingsRes = await crmDB.query(`SELECT * FROM crm_followup_settings WHERE id = 1`)
    settings = settingsRes.rows[0]
  }
  if (!settings) throw new Error('ไม่พบการตั้งค่า Follow-up Policy')
  if (settings.visit_enabled !== true) {
    const err = new Error('Visit Follow-up Policy ยังปิดอยู่')
    err.statusCode = 400
    throw err
  }

  const fallbackUserId = actorUserId || await loadFallbackUserId()
  if (!fallbackUserId) {
    const message = 'No admin/manager fallback user found'
    if (updateSettingsStats) {
      await crmDB.query(`
        UPDATE crm_followup_settings
        SET last_visit_create_checked_at = NOW(),
            last_visit_create_created_count = 0,
            last_visit_create_unassigned_count = 0,
            last_visit_create_error = $1
        WHERE id = 1
      `, [message])
    }
    const err = new Error(message)
    err.statusCode = 500
    throw err
  }

  const customersRes = await crmDB.query(`
    SELECT
      p.ar_code,
      p.next_visit_followup,
      p.next_visit_followup::text AS next_visit_followup_text,
      COALESCE(p.visit_followup_interval_days, $2::integer) AS effective_visit_interval_days
    FROM crm_customer_profile p
    WHERE p.status = 'active'
      AND p.visit_followup_enabled = TRUE
      AND (p.visit_followup_pause_until IS NULL OR p.visit_followup_pause_until < $1::date)
      AND p.next_visit_followup IS NOT NULL
      AND p.next_visit_followup <= $1::date
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.followup_key = ('auto-visit:' || p.ar_code || ':' || p.next_visit_followup::text)
      )
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.ar_code = p.ar_code
          AND a.followup_type = 'visit_scheduled'
          AND a.status NOT IN ('done','cancelled','deleted')
          AND (
            a.requires_owner_assignment = TRUE
            OR EXISTS (
              SELECT 1 FROM crm_activity_owners ao
              WHERE ao.activity_id = a.id
                AND ao.removed_at IS NULL
                AND ao.status NOT IN ('done','cancelled')
            )
          )
      )
    ORDER BY p.next_visit_followup ASC, p.ar_code ASC
    LIMIT $3
  `, [todayStr, settings.default_visit_interval_days, limit])

  const stuckRes = await crmDB.query(`
    SELECT
      p.ar_code,
      COALESCE(p.visit_followup_interval_days, $2::integer) AS effective_visit_interval_days
    FROM crm_customer_profile p
    WHERE p.status = 'active'
      AND p.visit_followup_enabled = TRUE
      AND (p.visit_followup_pause_until IS NULL OR p.visit_followup_pause_until < $1::date)
      AND p.next_visit_followup IS NOT NULL
      AND p.next_visit_followup <= $1::date
      AND NOT EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.followup_key = ('auto-visit:' || p.ar_code || ':' || p.next_visit_followup::text)
      )
      AND EXISTS (
        SELECT 1 FROM crm_activities a
        WHERE a.ar_code = p.ar_code
          AND a.followup_type = 'visit_scheduled'
          AND a.status NOT IN ('done','cancelled','deleted')
          AND (
            a.requires_owner_assignment = TRUE
            OR EXISTS (
              SELECT 1 FROM crm_activity_owners ao
              WHERE ao.activity_id = a.id
                AND ao.removed_at IS NULL
                AND ao.status NOT IN ('done','cancelled')
            )
          )
      )
    ORDER BY p.next_visit_followup ASC, p.ar_code ASC
    LIMIT $3
  `, [todayStr, settings.default_visit_interval_days, limit])

  const adminNotifyIds = await loadAdminNotifyIds()
  let createdCount = 0
  let unassignedCount = 0
  let firstError = null
  const createdActivities = []

  for (const customer of customersRes.rows) {
    const client = await crmDB.connect()
    try {
      await client.query('BEGIN')

      const ownerRes = await client.query(`
        SELECT o.user_id, o.is_primary
        FROM crm_customer_owner o
        JOIN crm_users u ON u.id = o.user_id
        WHERE o.ar_code = $1
          AND u.is_active = TRUE
        ORDER BY o.is_primary DESC, o.assigned_at ASC
      `, [customer.ar_code])

      const allOwnerIds = [...new Set(ownerRes.rows.map(row => Number(row.user_id)).filter(Boolean))]
      const selectedOwnerIds = settings.visit_assignment_mode === 'all'
        ? allOwnerIds
        : (allOwnerIds.length ? [allOwnerIds[0]] : [])
      const primaryOwnerId = selectedOwnerIds[0] || fallbackUserId
      const requiresOwnerAssignment = selectedOwnerIds.length === 0
      const followupDateText = customer.next_visit_followup_text || String(customer.next_visit_followup).slice(0, 10)
      const followupKey = `auto-visit:${customer.ar_code}:${followupDateText}`
      const subject = `เยี่ยมลูกค้า ${customer.ar_code}`
      const description = [
        source === 'manual'
          ? 'สร้างโดย Manual Trigger จาก Visit Follow-up Policy'
          : 'สร้างโดยระบบจาก Visit Follow-up Policy',
        `ครบกำหนดเยี่ยมวันที่ ${followupDateText}`,
        requiresOwnerAssignment ? 'ลูกค้ายังไม่มีทีมผู้ดูแล CRM กรุณาระบุผู้รับผิดชอบ' : '',
      ].filter(Boolean).join('\n')

      // generate act_no for visit
      const prefix = 'V'
      const bkkNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Bangkok' }))
      const dateStr = `${bkkNow.getFullYear()}${String(bkkNow.getMonth() + 1).padStart(2, '0')}${String(bkkNow.getDate()).padStart(2, '0')}`
      const lockKey = (prefix.charCodeAt(0) * 100000000 + parseInt(dateStr)) % 2147483647
      const lockClient = await crmDB.connect()
      let act_no_visit
      try {
        await lockClient.query(`SELECT pg_advisory_lock($1)`, [lockKey])
        const cntRes = await lockClient.query(
          `SELECT COUNT(*) AS cnt FROM crm_activities WHERE act_no LIKE $1`,
          [`${prefix}-${dateStr}-%`]
        )
        act_no_visit = `${prefix}-${dateStr}-${String(parseInt(cntRes.rows[0].cnt) + 1).padStart(4, '0')}`
      } finally {
        try { await lockClient.query(`SELECT pg_advisory_unlock($1)`, [lockKey]) } catch {}
        lockClient.release()
      }

      const insertRes = await client.query(`
        INSERT INTO crm_activities (
          ar_code, owner_id, created_by, activity_type, subject, description,
          status, priority, due_date, start_datetime, system_created,
          followup_type, followup_policy_id, followup_key,
          requires_owner_assignment, owner_assignment_note, act_no
        )
        VALUES ($1,$2,$3,'visit',$4,$5,'open','normal',$6,($6::date + $10::time),TRUE,
                'visit_scheduled',1,$7,$8,$9,$11)
        ON CONFLICT (followup_key) WHERE followup_key IS NOT NULL DO NOTHING
        RETURNING *
      `, [
        customer.ar_code,
        primaryOwnerId,
        fallbackUserId,
        subject,
        description,
        customer.next_visit_followup,
        followupKey,
        requiresOwnerAssignment,
        requiresOwnerAssignment ? 'ไม่มี CRM owner ตอนระบบสร้างงาน' : null,
        settings.visit_auto_create_time,
        act_no_visit,
      ])

      if (!insertRes.rows.length) {
        await client.query('ROLLBACK')
        continue
      }

      const activity = insertRes.rows[0]
      for (const uid of selectedOwnerIds) {
        await client.query(`
          INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
          VALUES ($1,$2,$3,'open',$4)
          ON CONFLICT (activity_id, user_id) DO NOTHING
        `, [activity.id, uid, uid === primaryOwnerId, fallbackUserId])
      }

      await client.query(`
        UPDATE crm_customer_profile
        SET next_visit_followup = (GREATEST(next_visit_followup, $2::date) + ($3 || ' days')::INTERVAL)::date
        WHERE ar_code = $1
      `, [customer.ar_code, todayStr, customer.effective_visit_interval_days])

      await client.query('COMMIT')
      createdCount++
      createdActivities.push({ id: activity.id, ar_code: customer.ar_code })

      if (requiresOwnerAssignment) {
        unassignedCount++
        if (adminNotifyIds.length) {
          await notifyMany(adminNotifyIds, {
            notiType: 'activity_update',
            title: 'มีงานเยี่ยมที่ยังไม่มีผู้รับผิดชอบ',
            message: subject,
            refType: 'activity',
            refId: activity.id,
            arCode: customer.ar_code,
          })
        }
      } else {
        await notifyMany(selectedOwnerIds, {
          notiType: 'assigned',
          title: 'ถึงเวลาเยี่ยมลูกค้า',
          message: subject,
          refType: 'activity',
          refId: activity.id,
          arCode: customer.ar_code,
        })
      }
    } catch (err) {
      await client.query('ROLLBACK')
      if (!firstError) firstError = `${customer.ar_code}: ${err.message}`
      console.error('[VisitFollowupRunner]', customer.ar_code, err.message)
    } finally {
      client.release()
    }
  }

  for (const stuck of stuckRes.rows) {
    try {
      await crmDB.query(`
        UPDATE crm_customer_profile
        SET next_visit_followup = ($2::date + ($3 || ' days')::INTERVAL)::date
        WHERE ar_code = $1
      `, [stuck.ar_code, todayStr, stuck.effective_visit_interval_days])
      console.log('[VisitFollowupRunner] stuck advance:', stuck.ar_code, '→ today+', stuck.effective_visit_interval_days, 'days')
    } catch (err) {
      console.error('[VisitFollowupRunner] stuck update error:', stuck.ar_code, err.message)
    }
  }

  if (updateSettingsStats) {
    await crmDB.query(`
      UPDATE crm_followup_settings
      SET last_visit_create_checked_at = NOW(),
          last_visit_create_created_count = $1,
          last_visit_create_unassigned_count = $2,
          last_visit_create_error = $3
      WHERE id = 1
    `, [createdCount, unassignedCount, firstError])
  }

  return {
    checked_date: todayStr,
    eligible_count: customersRes.rows.length,
    created_count: createdCount,
    unassigned_count: unassignedCount,
    error: firstError,
    created_activities: createdActivities,
  }
}

module.exports = {
  bangkokDate,
  runDueFollowups,
  runDueVisitFollowups,
}
