const cron = require('node-cron')
const { crmDB } = require('../db')
const lineService = require('./lineService')
const { notify } = require('./notifyService')
const { syncAllFleetSheets } = require('./fleetSync')
const { runDueFollowups } = require('./followupRunner')
const purchasePlanningRoute = require('../routes/purchasePlanning')

const PURCHASE_ALERT_WAREHOUSE = process.env.PURCHASE_ALERT_WAREHOUSE || 'MMA01'
const PURCHASE_ALERT_DAYS = Number(process.env.PURCHASE_ALERT_DAYS || 30)
const PURCHASE_ALERT_LIMIT = Number(process.env.PURCHASE_ALERT_LIMIT || 50)
const PURCHASE_ALERT_BATCH_SIZE = Number(process.env.PURCHASE_ALERT_BATCH_SIZE || 100)

// ─── Purchase Alert: shared logic ────────────────────────────────────────────
// ใช้ได้ทั้ง cron daily และ manual trigger ผ่าน API
async function runPurchaseAlert({ todayStr, skipDedup = false } = {}) {
  const TZ = 'Asia/Bangkok'
  if (!todayStr) {
    todayStr = new Intl.DateTimeFormat('en-CA', {
      timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date())
  }

  const usersResult = await crmDB.query(`
    SELECT id AS user_id, code AS user_code, name AS user_name, line_user_id
    FROM crm_users u
    WHERE u.is_active = TRUE
      AND COALESCE(u.purchase_alert_notify_enabled, FALSE) = TRUE
      AND (u.role = 'admin' OR UPPER(COALESCE(u.code, '')) = 'SUPERADMIN')
      ${skipDedup ? '' : `AND NOT EXISTS (
        SELECT 1 FROM crm_line_message_log lg
        WHERE lg.user_id = u.id
          AND lg.message_type = 'purchase_reorder_alert'
          AND lg.success = TRUE
          AND lg.sent_at >= $1::date
          AND lg.sent_at <  ($1::date + INTERVAL '1 day')
      )`}
  `, skipDedup ? [] : [todayStr])

  if (!usersResult.rows.length) return { sent: 0, itemCount: 0, skipped: true }

  const job = {
    query: { alert_only: '1' },
    options: {
      asOfDate: todayStr,
      warehouse: PURCHASE_ALERT_WAREHOUSE,
      days: PURCHASE_ALERT_DAYS,
    },
    batchSize: PURCHASE_ALERT_BATCH_SIZE,
    rows: [],
    processed: 0,
    total: 0,
    cancelled: false,
  }

  await purchasePlanningRoute.runReportJob(job)
  if (job.status !== 'complete') {
    throw new Error(job.error || 'purchase planning alert job failed')
  }

  const alertRows = (job.rows || []).filter(row => Number(row.suggest_qty || 0) > 0)
  const totalCount = alertRows.length
  const rows = alertRows.slice(0, PURCHASE_ALERT_LIMIT)

  if (!rows.length) return { sent: 0, itemCount: 0, skipped: false }

  // ── ส่ง LINE ให้แต่ละ user ──
  for (const user of usersResult.rows) {
    if (user.line_user_id) {
      await lineService.sendPurchaseReorderAlert(user, rows, {
        totalCount,
        asOfDate: todayStr,
        warehouse: PURCHASE_ALERT_WAREHOUSE,
        days: PURCHASE_ALERT_DAYS,
        limit: PURCHASE_ALERT_LIMIT,
      }).catch(e => console.error(`[PurchaseAlert] LINE ส่งไม่ได้: ${user.user_code}`, e.message))
    }

    // ── CRM in-app notification (ทุก user ที่เปิด alert) ──
    await notify({
      userId: user.user_id,
      notiType: 'purchase_reorder_alert',
      title: `มีสินค้าถึงจุดสั่งซื้อ ${totalCount} รายการ`,
      message: `คลัง ${PURCHASE_ALERT_WAREHOUSE} วันที่ ${todayStr}`,
      refType: 'purchase_planning',
      linkUrl: '/purchase-planning/alerts',
    }).catch(e => console.error(`[PurchaseAlert] CRM notify ไม่ได้: ${user.user_code}`, e.message))
  }

  console.log(`[PurchaseAlert] ส่ง ${usersResult.rows.length} คน, สินค้าถึงจุดสั่งซื้อ ${totalCount} รายการ (${todayStr})`)
  return { sent: usersResult.rows.length, itemCount: totalCount, skipped: false }
}

/**
 * เริ่ม Cron Jobs ทั้งหมดของระบบ LINE
 * เรียกใช้ใน index.js: require('./services/cronJobs').start()
 */
function start() {
  console.log('⏰ Cron Jobs started')

  // ── Timezone config ──────────────────────────────────────────
  // Render/Cloud ใช้ UTC — ต้องบอก node-cron ให้ใช้เวลาไทย
  const TZ = 'Asia/Bangkok'
  const cronOpts = { timezone: TZ }

  // Helper: ดึงเวลาไทย HH:MM (ใช้ Intl เพื่อความแม่นยำ ไม่พึ่ง system TZ)
  function bangkokHHMM() {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: TZ, hour: '2-digit', minute: '2-digit', hour12: false,
    }).formatToParts(new Date()).reduce((a, p) => (a[p.type] = p.value, a), {})
    return `${parts.hour}:${parts.minute}`
  }

  // Helper: ดึงวันที่ไทย YYYY-MM-DD (ใช้ Intl เพื่อความแม่นยำ)
  function bangkokDate() {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date())
  }

  // ── Dedup guard: เก็บ HH:MM ล่าสุดที่ส่ง Daily Summary ──
  // ป้องกัน cron 2 tick ภายใน 1 นาทีเดียวกัน
  let lastDailySentRunKey = ''
  let lastAutoFollowupRunKey = ''
  let purchaseAlertRunning = false

  // ── 1. Daily Summary: ทุกนาที ตรวจว่าถึงเวลาส่งไหม ──
  cron.schedule('* * * * *', async () => {
    try {
      const hhmm = bangkokHHMM()
      const todayStr = bangkokDate()
      const runKey = `${todayStr} ${hhmm}`

      // ── In-memory dedup: ถ้านาทีนี้ส่งไปแล้ว ข้าม ──
      if (lastDailySentRunKey === runKey) return
      lastDailySentRunKey = runKey

      // ส่งเฉพาะ user ที่ตั้ง notify_time ตรงกับเวลาตอนนี้
      // + ยังไม่เคยส่งวันนี้ (ตรวจจาก crm_line_message_log)
      const result = await crmDB.query(`
        SELECT v.* FROM v_daily_summary_per_user v
        WHERE v.line_notify_enabled = TRUE
          AND v.line_user_id IS NOT NULL
          AND to_char(v.line_notify_time, 'HH24:MI') = $1
          AND NOT EXISTS (
            SELECT 1 FROM crm_line_message_log lg
            WHERE lg.user_id = v.user_id
              AND lg.message_type = 'daily_summary'
              AND lg.success = TRUE
              AND lg.sent_at >= $2::date
              AND lg.sent_at <  ($2::date + INTERVAL '1 day')
          )
      `, [hhmm, todayStr])

      for (const user of result.rows) {
        await lineService.sendDailySummary(user)
          .catch(e => console.error(`[Cron Daily] ส่งไม่ได้: ${user.user_code}`, e.message))
      }

      if (result.rows.length > 0) {
        console.log(`[Cron Daily] ส่ง ${result.rows.length} คน (${hhmm})`)
      }
    } catch (err) {
      console.error('[Cron Daily Error]', err.message)
    }
  }, cronOpts)

  // ── 1.1 Purchase Reorder Alert: ทุกวัน 08:00 น. (fix — ไม่ configurable) ──
  cron.schedule('0 8 * * *', async () => {
    if (purchaseAlertRunning) return
    purchaseAlertRunning = true
    try {
      const todayStr = bangkokDate()
      await runPurchaseAlert({ todayStr, skipDedup: false })
    } catch (err) {
      console.error('[Cron PurchaseAlert Error]', err.message)
    } finally {
      purchaseAlertRunning = false
    }
  }, cronOpts)

  // ── 1.5 Auto Follow-up: สร้างงานโทรติดตามจาก Follow-up Policy ──
  cron.schedule('* * * * *', async () => {
    try {
      const hhmm = bangkokHHMM()
      const todayStr = bangkokDate()
      const runKey = `${todayStr} ${hhmm}`

      if (lastAutoFollowupRunKey === runKey) return

      const settingsRes = await crmDB.query(`
        SELECT * FROM crm_followup_settings
        WHERE id = 1
          AND enabled = TRUE
          AND auto_create_enabled = TRUE
          AND to_char(auto_create_time, 'HH24:MI') = $1
      `, [hhmm])
      if (!settingsRes.rows.length) return

      lastAutoFollowupRunKey = runKey
      const settings = settingsRes.rows[0]
      const result = await runDueFollowups({
        settings,
        todayStr,
        limit: 100,
        source: 'cron',
      })

      if (result.created_count > 0 || result.unassigned_count > 0 || result.error) {
        console.log(
          `[Cron AutoFollowup] created ${result.created_count} activities, ` +
          `unassigned ${result.unassigned_count} (${hhmm})` +
          (result.error ? ` error=${result.error}` : '')
        )
      }
      return
    } catch (err) {
      console.error('[Cron AutoFollowup Error]', err.message)
    }
  }, cronOpts)

  // ── 1.6 Call Retry Due Alert: แจ้งเมื่อถึงเวลาโทรซ้ำ ──
  cron.schedule('* * * * *', async () => {
    try {
      const todayStr = bangkokDate()
      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.activity_type, a.start_datetime, a.retry_due_at, a.priority, a.ar_code,
               a.attempt_no,
               u.id AS user_id, u.line_user_id, u.line_notify_enabled,
               EXISTS (
                 SELECT 1 FROM crm_line_message_log lg
                 WHERE lg.user_id = u.id
                   AND lg.ref_type = 'activity'
                   AND lg.ref_id = a.id
                   AND lg.message_type IN ('task_reminder','task_overdue')
                   AND lg.success = TRUE
                   AND lg.sent_at >= $1::date
                   AND lg.sent_at <  ($1::date + INTERVAL '1 day')
               ) AS has_line_success_today
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE a.followup_type = 'no_answer_retry'
          AND COALESCE(a.retry_due_at, a.start_datetime) <= NOW()
          AND COALESCE(a.start_datetime, a.retry_due_at) <= NOW()
          AND a.status NOT IN ('done','cancelled','deleted')
          AND ao.status NOT IN ('done','cancelled')
          AND u.is_active = TRUE
          AND NOT EXISTS (
            SELECT 1 FROM crm_notifications n
            WHERE n.user_id = u.id
              AND n.ref_type = 'activity'
              AND n.ref_id = a.id
              AND n.noti_type = 'task_due'
              AND n.created_at >= $1::date
              AND n.created_at < ($1::date + INTERVAL '1 day')
          )
      `, [todayStr])

      for (const task of result.rows) {
        if (!task.has_notification_today) {
          await notify({
            userId: task.user_id,
            notiType: 'task_due',
          title: `ถึงเวลาโทรซ้ำ${task.attempt_no ? ` ครั้งที่ ${task.attempt_no}` : ''}`,
          message: task.subject,
          refType: 'activity',
          refId: task.id,
          arCode: task.ar_code,
          })
        }

        if (task.line_user_id && task.line_notify_enabled && !task.has_line_success_today) {
          await lineService.sendTaskReminder(task.line_user_id, task.user_id, task)
            .catch(e => console.error(`[Cron RetryDue] task ${task.id}`, e.message))
        }
      }

      if (result.rows.length > 0) {
        console.log(`[Cron RetryDue] แจ้งเตือนโทรซ้ำ ${result.rows.length} owner-task`)
      }
    } catch (err) {
      console.error('[Cron RetryDue Error]', err.message)
    }
  }, cronOpts)

  // ── 2. Task Overdue Alert: ทุกนาที เช็คเวลาที่ user ตั้งไว้ ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner ที่ยังไม่ได้ปิดงาน
  cron.schedule('* * * * *', async () => {
    try {
      const hhmm = bangkokHHMM()
      const todayStr = bangkokDate()

      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.activity_type, a.due_date, a.start_datetime, a.priority, a.ar_code,
               u.id AS user_id, u.line_user_id, u.line_notify_enabled,
               ao.status AS owner_status,
               EXISTS (
                 SELECT 1 FROM crm_notifications n
                 WHERE n.user_id = u.id
                   AND n.ref_type = 'activity'
                   AND n.ref_id = a.id
                   AND n.noti_type = 'task_overdue'
                   AND n.created_at >= $2::date
                   AND n.created_at <  ($2::date + INTERVAL '1 day')
               ) AS has_notification_today,
               EXISTS (
                 SELECT 1 FROM crm_line_message_log lg
                 WHERE lg.user_id = u.id
                   AND lg.ref_type = 'activity'
                   AND lg.ref_id = a.id
                   AND lg.message_type = 'task_overdue'
                   AND lg.success = TRUE
                   AND lg.sent_at >= $2::date
                   AND lg.sent_at <  ($2::date + INTERVAL '1 day')
               ) AS has_line_success_today
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE ao.status NOT IN ('done','cancelled')
          AND a.status NOT IN ('done','cancelled','deleted')
          AND COALESCE(a.due_date, DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok')) < CURRENT_DATE
          AND u.is_active = TRUE
          AND to_char(COALESCE(u.overdue_notify_time, TIME '08:00'), 'HH24:MI') = $1
      `, [hhmm, todayStr])

      for (const task of result.rows) {
        const dueDate = task.due_date || task.start_datetime
        const daysDiff = Math.floor((Date.now() - new Date(dueDate).getTime()) / 86400000)
        const typeLabel = task.activity_type === 'call' ? 'งานโทร' : task.activity_type === 'meeting' ? 'นัดประชุม' : 'งาน'
        if (!task.has_notification_today) {
          await notify({
            userId: task.user_id,
            notiType: 'task_overdue',
          title: `${typeLabel}เลยกำหนด ${daysDiff} วัน`,
          message: task.subject,
          refType: 'activity',
          refId: task.id,
          arCode: task.ar_code,
          })
        }

        if (task.line_user_id && task.line_notify_enabled && !task.has_line_success_today) {
          await lineService.sendTaskReminder(task.line_user_id, task.user_id, task)
            .catch(e => console.error(`[Cron Overdue] task ${task.id}`, e.message))
        }
      }

      if (result.rows.length > 0) {
        console.log(`[Cron Overdue] แจ้งเตือน ${result.rows.length} owner-task (${hhmm})`)
      }
    } catch (err) {
      console.error('[Cron Overdue Error]', err.message)
    }
  }, cronOpts)

  // ── 3. Task Due Tomorrow: ทุกนาที เช็คเวลาที่ user ตั้งไว้ ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner ที่ยังไม่ได้ปิดงาน
  cron.schedule('* * * * *', async () => {
    try {
      const hhmm = bangkokHHMM()
      const todayStr = bangkokDate()

      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.activity_type, a.due_date, a.start_datetime, a.priority, a.ar_code,
               u.id AS user_id, u.line_user_id, u.line_notify_enabled,
               EXISTS (
                 SELECT 1 FROM crm_notifications n
                 WHERE n.user_id = u.id
                   AND n.ref_type = 'activity'
                   AND n.ref_id = a.id
                   AND n.noti_type = 'task_due'
                   AND n.created_at >= $2::date
                   AND n.created_at <  ($2::date + INTERVAL '1 day')
               ) AS has_notification_today,
               EXISTS (
                 SELECT 1 FROM crm_line_message_log lg
                 WHERE lg.user_id = u.id
                   AND lg.ref_type = 'activity'
                   AND lg.ref_id = a.id
                   AND lg.message_type = 'task_reminder'
                   AND lg.success = TRUE
                   AND lg.sent_at >= $2::date
                   AND lg.sent_at <  ($2::date + INTERVAL '1 day')
               ) AS has_line_success_today
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE ao.status NOT IN ('done','cancelled')
          AND a.status NOT IN ('done','cancelled','deleted')
          AND COALESCE(a.due_date, DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok'))
              = (CURRENT_DATE + INTERVAL '1 day')
          AND u.is_active = TRUE
          AND to_char(COALESCE(u.due_tomorrow_notify_time, TIME '17:00'), 'HH24:MI') = $1
      `, [hhmm, todayStr])

      for (const task of result.rows) {
        const typeLabel2 = task.activity_type === 'call' ? 'งานโทร' : task.activity_type === 'meeting' ? 'นัดประชุม' : 'งาน'
        if (!task.has_notification_today) {
          await notify({
            userId: task.user_id,
            notiType: 'task_due',
          title: `${typeLabel2}ครบกำหนดพรุ่งนี้`,
          message: task.subject,
          refType: 'activity',
          refId: task.id,
          arCode: task.ar_code,
          })
        }

        if (task.line_user_id && task.line_notify_enabled && !task.has_line_success_today) {
          await lineService.sendTaskReminder(task.line_user_id, task.user_id, task)
            .catch(e => console.error(`[Cron DueTomorrow] task ${task.id}`, e.message))
        }
      }

      if (result.rows.length > 0) {
        console.log(`[Cron DueTomorrow] แจ้งเตือน ${result.rows.length} owner-task (${hhmm})`)
      }
    } catch (err) {
      console.error('[Cron DueTomorrow Error]', err.message)
    }
  }, cronOpts)

  // ── 4. Meeting Reminder: ทุก 15 นาที ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner/participant ที่ยังไม่ได้ปิด
  cron.schedule('*/15 * * * *', async () => {
    try {
      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.start_datetime, a.location, a.meeting_url, a.ar_code,
               u.id AS user_id, u.line_user_id, u.name AS user_name, u.line_notify_enabled
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE a.activity_type = 'meeting'
          AND ao.status NOT IN ('done','cancelled')
          AND a.start_datetime BETWEEN NOW() AND NOW() + INTERVAL '30 minutes'
          AND u.is_active = TRUE
          AND NOT EXISTS (
            SELECT 1 FROM crm_notifications n
            WHERE n.user_id = u.id
              AND n.ref_type = 'activity'
              AND n.ref_id = a.id
              AND n.noti_type = 'meeting_remind'
              AND n.created_at >= (CURRENT_DATE AT TIME ZONE 'Asia/Bangkok')
              AND n.created_at <  ((CURRENT_DATE AT TIME ZONE 'Asia/Bangkok') + INTERVAL '1 day')
          )
      `)

      for (const meeting of result.rows) {
        const startText = new Date(meeting.start_datetime).toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit', timeZone: TZ })

        await notify({
          userId: meeting.user_id,
          notiType: 'meeting_remind',
          title: `Meeting เริ่มใน 30 นาที`,
          message: `${meeting.subject} — ${startText}${meeting.location ? ' @ ' + meeting.location : ''}`,
          refType: 'activity',
          refId: meeting.id,
          arCode: meeting.ar_code,
        })

        if (meeting.line_user_id && meeting.line_notify_enabled) {
          const locationLine = meeting.meeting_url
            ? `🔗 ${meeting.meeting_url}`
            : meeting.location ? `📍 ${meeting.location}` : ''
          await lineService.sendMessage(meeting.line_user_id, [{
            type: 'text',
            text: `📅 แจ้งเตือน Meeting!\n` +
                  `"${meeting.subject}"\n` +
                  `⏰ เริ่ม: ${startText} น.\n` +
                  `${locationLine}\n\n` +
                  `อีกประมาณ 30 นาที`
          }]).catch(() => {})
        }
      }

      if (result.rows.length > 0) {
        console.log(`[Cron Meeting] แจ้งเตือน ${result.rows.length} owner-meeting`)
      }
    } catch (err) {
      console.error('[Cron Meeting Error]', err.message)
    }
  }, cronOpts)

  // ── 5. No-Contact Alert: ทุกวันจันทร์ 9:00 น. ──
  cron.schedule('0 9 * * 1', async () => {
    try {
      const result = await crmDB.query(`
        SELECT
          o.user_id, u.line_user_id, u.name AS user_name, u.line_notify_enabled,
          COUNT(*) AS count,
          json_agg(json_build_object('ar_code', p.ar_code, 'days',
            EXTRACT(DAY FROM NOW() - COALESCE(p.last_contacted, p.created_at))
          ) ORDER BY p.last_contacted ASC NULLS FIRST) AS customers
        FROM crm_customer_owner o
        JOIN crm_users u ON u.id = o.user_id
        JOIN crm_customer_profile p ON p.ar_code = o.ar_code
        WHERE o.is_primary = TRUE
          AND (p.last_contacted < NOW() - INTERVAL '30 days' OR p.last_contacted IS NULL)
          AND p.status = 'active'
          AND u.is_active = TRUE
        GROUP BY o.user_id, u.line_user_id, u.name, u.line_notify_enabled
        HAVING COUNT(*) > 0
      `)

      for (const u of result.rows) {
        // insert crm_notifications
        await notify({
          userId: u.user_id,
          notiType: 'no_contact',
          title: `มีลูกค้า ${u.count} รายที่ยังไม่ได้ติดต่อ (>30 วัน)`,
          message: `กรุณา Follow-up ลูกค้าด้วยนะครับ`,
          refType: 'customer',
        })

        // LINE push (เฉพาะที่เปิด notify)
        if (u.line_user_id && u.line_notify_enabled) {
          const topCustomers = u.customers.slice(0, 5)
          const text = `🔔 ลูกค้าที่ยังไม่ได้ติดต่อ (>30 วัน)\n\n` +
            topCustomers.map(c => `• ${c.ar_code} — ${Math.round(c.days)} วัน`).join('\n') +
            (u.count > 5 ? `\n...และอีก ${u.count - 5} ราย` : '') +
            `\n\nรวม ${u.count} ราย กรุณา Follow-up ด้วยนะครับ 😊`

          await lineService.sendMessage(u.line_user_id, [{ type: 'text', text }])
            .catch(() => {})
        }
      }

      console.log(`[Cron NoContact] แจ้งเตือน ${result.rows.length} คน`)
    } catch (err) {
      console.error('[Cron NoContact Error]', err.message)
    }
  }, cronOpts)

  // ── 6. Cleanup: ทุกคืน 2:00 น. ──
  cron.schedule('0 2 * * *', async () => {
    try {
      // ลบ Session หมดอายุ
      const s = await crmDB.query(`DELETE FROM crm_sessions WHERE expires_at < NOW() OR is_revoked = TRUE`)
      // ลบ Webhook log เก่า > 30 วัน
      const w = await crmDB.query(`DELETE FROM crm_line_webhook_log WHERE created_at < NOW() - INTERVAL '30 days'`)
      // ลบ Link Token หมดอายุ
      await crmDB.query(`DELETE FROM crm_line_link_token WHERE expires_at < NOW()`)
      // ลบ Notifications เก่า > 90 วัน
      const n = await crmDB.query(`DELETE FROM crm_notifications WHERE created_at < NOW() - INTERVAL '90 days'`)
      console.log(`[Cron Cleanup] Sessions: ${s.rowCount}, Webhook logs: ${w.rowCount}, Notifications: ${n.rowCount}`)
    } catch (err) {
      console.error('[Cron Cleanup Error]', err.message)
    }
  }, cronOpts)

  // ── Fleet Delivery: sync จาก Google Sheets ทุก 10 นาที ──────
  cron.schedule('*/10 * * * *', async () => {
    await syncAllFleetSheets().catch(e => console.error('[Cron FleetSync]', e.message))
  }, cronOpts)
}

module.exports = { start, runPurchaseAlert }
