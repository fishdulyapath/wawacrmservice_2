const cron = require('node-cron')
const { crmDB } = require('../db')
const lineService = require('./lineService')
const { notify, notifyMany } = require('./notifyService')

/**
 * เริ่ม Cron Jobs ทั้งหมดของระบบ LINE
 * เรียกใช้ใน index.js: require('./services/cronJobs').start()
 */
function start() {
  console.log('⏰ Cron Jobs started')

  // ── 1. Daily Summary: ทุกวัน ตรวจทุก 1 นาที ว่าถึงเวลาส่งไหม ──
  cron.schedule('* * * * *', async () => {
    try {
      const now = new Date()
      const hhmm = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`

      // ส่งเฉพาะ user ที่ตั้ง notify_time ตรงกับเวลาตอนนี้
      const result = await crmDB.query(`
        SELECT * FROM v_daily_summary_per_user
        WHERE line_notify_enabled = TRUE
          AND line_user_id IS NOT NULL
          AND to_char(line_notify_time, 'HH24:MI') = $1
      `, [hhmm])

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
  })

  // ── 2. Task Overdue Alert: ทุก 8:00 น. ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner ที่ยังไม่ได้ปิดงาน
  cron.schedule('0 8 * * *', async () => {
    try {
      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.activity_type, a.due_date, a.start_datetime, a.priority, a.ar_code,
               u.id AS user_id, u.line_user_id, u.line_notify_enabled,
               ao.status AS owner_status
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE ao.status NOT IN ('done','cancelled')
          AND COALESCE(a.due_date, DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok')) < CURRENT_DATE
          AND u.is_active = TRUE
      `)

      for (const task of result.rows) {
        const dueDate = task.due_date || task.start_datetime
        const daysDiff = Math.floor((Date.now() - new Date(dueDate).getTime()) / 86400000)
        const typeLabel = task.activity_type === 'call' ? 'งานโทร' : task.activity_type === 'meeting' ? 'นัดประชุม' : 'งาน'
        await notify({
          userId: task.user_id,
          notiType: 'task_overdue',
          title: `${typeLabel}เลยกำหนด ${daysDiff} วัน`,
          message: task.subject,
          refType: 'activity',
          refId: task.id,
          arCode: task.ar_code,
        })

        if (task.line_user_id && task.line_notify_enabled) {
          await lineService.sendTaskReminder(task.line_user_id, task.user_id, task)
            .catch(e => console.error(`[Cron Overdue] task ${task.id}`, e.message))
        }
      }

      console.log(`[Cron Overdue] แจ้งเตือน ${result.rows.length} owner-task`)
    } catch (err) {
      console.error('[Cron Overdue Error]', err.message)
    }
  })

  // ── 3. Task Due Tomorrow: ทุก 17:00 น. ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner ที่ยังไม่ได้ปิดงาน
  cron.schedule('0 17 * * *', async () => {
    try {
      const tomorrow = new Date()
      tomorrow.setDate(tomorrow.getDate() + 1)
      const tomorrowDate = tomorrow.toISOString().split('T')[0]

      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.activity_type, a.due_date, a.start_datetime, a.priority, a.ar_code,
               u.id AS user_id, u.line_user_id, u.line_notify_enabled
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE ao.status NOT IN ('done','cancelled')
          AND COALESCE(a.due_date, DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok')) = $1
          AND u.is_active = TRUE
      `, [tomorrowDate])

      for (const task of result.rows) {
        const typeLabel2 = task.activity_type === 'call' ? 'งานโทร' : task.activity_type === 'meeting' ? 'นัดประชุม' : 'งาน'
        await notify({
          userId: task.user_id,
          notiType: 'task_due',
          title: `${typeLabel2}ครบกำหนดพรุ่งนี้`,
          message: task.subject,
          refType: 'activity',
          refId: task.id,
          arCode: task.ar_code,
        })

        if (task.line_user_id && task.line_notify_enabled) {
          await lineService.sendTaskReminder(task.line_user_id, task.user_id, task)
            .catch(e => console.error(`[Cron DueTomorrow] task ${task.id}`, e.message))
        }
      }

      console.log(`[Cron DueTomorrow] แจ้งเตือน ${result.rows.length} owner-task`)
    } catch (err) {
      console.error('[Cron DueTomorrow Error]', err.message)
    }
  })

  // ── 4. Meeting Reminder: ทุก 15 นาที ──
  // ใช้ crm_activity_owners เพื่อแจ้งทุก owner/participant ที่ยังไม่ได้ปิด
  cron.schedule('*/15 * * * *', async () => {
    try {
      const now = new Date()
      const in30min = new Date(now.getTime() + 30 * 60 * 1000)

      const result = await crmDB.query(`
        SELECT a.id, a.subject, a.start_datetime, a.location, a.meeting_url, a.ar_code,
               u.id AS user_id, u.line_user_id, u.name AS user_name, u.line_notify_enabled
        FROM crm_activities a
        JOIN crm_activity_owners ao ON ao.activity_id = a.id AND ao.removed_at IS NULL
        JOIN crm_users u ON u.id = ao.user_id
        WHERE a.activity_type = 'meeting'
          AND ao.status NOT IN ('done','cancelled')
          AND a.start_datetime BETWEEN $1 AND $2
          AND u.is_active = TRUE
      `, [now, in30min])

      for (const meeting of result.rows) {
        const startText = new Date(meeting.start_datetime).toLocaleTimeString('th-TH', { hour: '2-digit', minute: '2-digit' })

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
  })

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
  })

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
  })
}

module.exports = { start }
