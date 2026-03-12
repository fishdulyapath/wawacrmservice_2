const { crmDB } = require('../db')

const LINE_API = 'https://api.line.me/v2/bot/message'
const CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN

// ─────────────────────────────────────────────────────────────
// Helper: ส่ง LINE Message ไปหา userId (Push API — มีโควตา)
// ใช้สำหรับ: cron jobs, notifications ที่ไม่ได้เกิดจาก webhook
// ─────────────────────────────────────────────────────────────
async function sendMessage(lineUserId, messages) {
  const res = await fetch(`${LINE_API}/push`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({ to: lineUserId, messages })
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(`LINE API error ${res.status}: ${JSON.stringify(err)}`)
  }
  return res.json()
}

// ─────────────────────────────────────────────────────────────
// Helper: ตอบกลับ LINE ด้วย Reply Token (Reply API — ฟรีไม่จำกัด)
// ใช้สำหรับ: ตอบกลับ message/follow/postback ใน webhook handler
// ⚠️ replyToken ใช้ได้ครั้งเดียว และหมดอายุ ~1 นาทีหลังรับ event
// ─────────────────────────────────────────────────────────────
async function replyMessage(replyToken, messages) {
  if (!replyToken) {
    console.warn('[LINE replyMessage] ไม่มี replyToken — ข้ามการตอบกลับ')
    return null
  }
  const res = await fetch(`${LINE_API}/reply`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify({ replyToken, messages })
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({}))
    throw new Error(`LINE Reply API error ${res.status}: ${JSON.stringify(err)}`)
  }
  return res.json()
}

// ─────────────────────────────────────────────────────────────
// Helper: Reply ก่อน → ถ้า replyToken หมดอายุ/ไม่มี → fallback Push
// ใช้สำหรับ webhook handlers ที่มี async work ก่อนตอบกลับ
// (เช่น query DB → reply อาจช้าจน replyToken expire)
// ─────────────────────────────────────────────────────────────
async function replyOrPush(replyToken, lineUserId, messages) {
  if (replyToken) {
    try {
      return await replyMessage(replyToken, messages)
    } catch (e) {
      console.warn(`[LINE replyOrPush] Reply ล้มเหลว (${e.message}) — fallback เป็น Push`)
    }
  }
  // fallback: ใช้ Push API
  if (lineUserId) {
    return await sendMessage(lineUserId, messages)
  }
}

// ─────────────────────────────────────────────────────────────
// บันทึก Log การส่ง LINE
// ─────────────────────────────────────────────────────────────
async function logMessage({ userId, lineUserId, messageType, refType, refId, arCode, payload, success, errorMessage }) {
  try {
    await crmDB.query(`
      INSERT INTO crm_line_message_log
        (user_id, line_user_id, message_type, ref_type, ref_id, ar_code, payload, success, error_message)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    `, [userId, lineUserId, messageType, refType, refId, arCode,
        JSON.stringify(payload), success, errorMessage || null])
  } catch (e) {
    console.error('[LINE Log Error]', e.message)
  }
}

// ─────────────────────────────────────────────────────────────
// 📊 Daily Summary — Flex Message
// ─────────────────────────────────────────────────────────────
async function sendDailySummary(user, replyToken = null) {
  const {
    user_id, line_user_id, user_name,
    open_tasks, overdue_tasks, today_meetings,
    total_customers, no_contact_30d
  } = user

  const today = new Date().toLocaleDateString('th-TH', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  })

  const overdueBadge = overdue_tasks > 0
    ? { type: 'box', layout: 'baseline', spacing: 'sm', contents: [
        { type: 'icon', url: 'https://scdn.line-apps.com/n/channel_devcenter/img/fx/review_gold_star_28.png', size: 'xxs' },
        { type: 'text', text: `งานเลยกำหนด ${overdue_tasks} รายการ!`, color: '#FF3B30', size: 'sm', weight: 'bold' }
      ]}
    : null

  const flexBody = {
    type: 'bubble',
    size: 'mega',
    header: {
      type: 'box', layout: 'vertical',
      backgroundColor: '#1d4ed8',
      paddingAll: '20px',
      contents: [
        { type: 'text', text: '📋 สรุปงานประจำวัน', color: '#ffffff', size: 'lg', weight: 'bold' },
        { type: 'text', text: today, color: '#bfdbfe', size: 'xs', margin: 'sm' },
        { type: 'text', text: `สวัสดี คุณ${user_name}`, color: '#e0f2fe', size: 'sm', margin: 'md' }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
      contents: [
        // Task Summary
        {
          type: 'box', layout: 'vertical',
          backgroundColor: '#f8fafc', cornerRadius: '8px',
          paddingAll: '15px', spacing: 'sm',
          contents: [
            { type: 'text', text: 'งานที่ต้องทำวันนี้', weight: 'bold', size: 'sm', color: '#374151' },
            {
              type: 'box', layout: 'horizontal', spacing: 'sm', margin: 'sm',
              contents: [
                _statBox('📌 กิจกรรมค้าง', open_tasks, '#2563eb'),
                _statBox('⚠️ เลยกำหนด', overdue_tasks, overdue_tasks > 0 ? '#dc2626' : '#6b7280'),
                _statBox('📅 นัดวันนี้', today_meetings, '#059669')
              ]
            },
            ...(overdueBadge ? [{ type: 'separator', margin: 'sm' }, overdueBadge] : [])
          ]
        },
        // Customer Summary
        {
          type: 'box', layout: 'vertical',
          backgroundColor: '#f0fdf4', cornerRadius: '8px',
          paddingAll: '15px', spacing: 'sm',
          contents: [
            { type: 'text', text: 'สรุปลูกค้า', weight: 'bold', size: 'sm', color: '#374151' },
            {
              type: 'box', layout: 'horizontal', spacing: 'sm', margin: 'sm',
              contents: [
                _statBox('👥 ดูแลทั้งหมด', total_customers, '#2563eb'),
                _statBox('🔔 ค้างติดต่อ 30วัน', no_contact_30d, no_contact_30d > 0 ? '#dc2626' : '#6b7280')
              ]
            }
          ]
        }
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        {
          type: 'button', style: 'primary', color: '#2563eb',
          action: { type: 'uri', label: '📋 ดูงานทั้งหมด', uri: `${process.env.FRONTEND_URL}/line/tasks` }
        }
      ]
    }
  }

  const messages = [{ type: 'flex', altText: `📋 สรุปงานวันนี้: กิจกรรมค้าง ${open_tasks} | เลยกำหนด ${overdue_tasks} | นัด ${today_meetings}`, contents: flexBody }]

  try {
    // ใช้ Reply API (ฟรี) ถ้ามี replyToken, ไม่งั้น fallback เป็น Push API
    if (replyToken) {
      await replyMessage(replyToken, messages)
    } else {
      await sendMessage(line_user_id, messages)
    }
    await logMessage({ userId: user_id, lineUserId: line_user_id, messageType: 'daily_summary', payload: messages, success: true })
  } catch (e) {
    await logMessage({ userId: user_id, lineUserId: line_user_id, messageType: 'daily_summary', payload: messages, success: false, errorMessage: e.message })
  }
}

// ─────────────────────────────────────────────────────────────
// 📞 Customer Card — แสดงข้อมูลลูกค้าพร้อมปุ่มโทร
// ─────────────────────────────────────────────────────────────
function buildCustomerCard(customer, contactors = []) {
  const phones = contactors.flatMap(c =>
    (c.telephone || '').split(',').map(p => p.trim()).filter(Boolean)
  )

  const phoneButtons = phones.slice(0, 3).map(phone => ({
    type: 'button', style: 'primary', color: '#059669', height: 'sm',
    action: { type: 'uri', label: `📞 ${phone}`, uri: `tel:${phone}` }
  }))

  return {
    type: 'bubble',
    header: {
      type: 'box', layout: 'vertical', backgroundColor: '#1e40af', paddingAll: '15px',
      contents: [
        { type: 'text', text: '👤 ข้อมูลลูกค้า', color: '#bfdbfe', size: 'xs' },
        { type: 'text', text: customer.name_1, color: '#ffffff', size: 'lg', weight: 'bold', wrap: true }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        _infoRow('🏷️ รหัส', customer.code),
        ...(customer.province ? [_infoRow('📍 จังหวัด', customer.province)] : []),
        ...(customer.address  ? [_infoRow('🏠 ที่อยู่', customer.address)] : []),
        ...(contactors.length > 0 ? [
          { type: 'separator', margin: 'md' },
          { type: 'text', text: 'ผู้ติดต่อ', weight: 'bold', size: 'sm', margin: 'md', color: '#374151' },
          ...contactors.slice(0, 3).map(c => _infoRow('👤', `${c.name}${c.email ? ' | ' + c.email : ''}`))
        ] : [])
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        ...phoneButtons,
        {
          type: 'button', style: 'secondary', height: 'sm',
          action: {
            type: 'uri', label: '📝 บันทึกการติดต่อ',
            uri: `${process.env.FRONTEND_URL}/line/log-call?ar_code=${customer.code}`
          }
        }
      ]
    }
  }
}

// ─────────────────────────────────────────────────────────────
// ✅ Task Reminder — แจ้งเตือน Task ใกล้ครบกำหนด
// ─────────────────────────────────────────────────────────────
async function sendTaskReminder(lineUserId, userId, activity) {
  const dueDate = activity.due_date || activity.start_datetime
  const dueText = dueDate ? new Date(dueDate).toLocaleDateString('th-TH', {
    weekday: 'short', day: 'numeric', month: 'short',
    ...(activity.start_datetime && !activity.due_date ? { hour: '2-digit', minute: '2-digit' } : {})
  }) : 'ไม่ระบุ'

  const typeLabel = activity.activity_type === 'call' ? '📞 โทร' : activity.activity_type === 'meeting' ? '📅 นัดประชุม' : '✅ งาน'
  const isOverdue = dueDate && new Date(dueDate) < new Date()
  const headerColor = isOverdue ? '#dc2626' : '#d97706'
  const headerText  = isOverdue ? `⚠️ ${typeLabel} เลยกำหนดแล้ว!` : `🔔 ${typeLabel} ใกล้ครบกำหนด`

  const flex = {
    type: 'bubble',
    header: {
      type: 'box', layout: 'vertical', backgroundColor: headerColor, paddingAll: '15px',
      contents: [
        { type: 'text', text: headerText, color: '#ffffff', size: 'sm', weight: 'bold' }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        { type: 'text', text: activity.subject, weight: 'bold', size: 'md', wrap: true },
        _infoRow('📅 กำหนด', dueText),
        _infoRow('🎯 Priority', activity.priority === 'high' ? '🔴 สูง' : activity.priority === 'low' ? '🟢 ต่ำ' : '🟡 ปกติ'),
        ...(activity.ar_code ? [_infoRow('👤 ลูกค้า', activity.ar_code)] : [])
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        {
          type: 'button', style: 'primary', color: headerColor,
          action: {
            type: 'uri', label: '📋 ดูรายละเอียด',
            uri: `${process.env.FRONTEND_URL}/line/tasks`
          }
        }
      ]
    }
  }

  const messages = [{ type: 'flex', altText: `${headerText}: ${activity.subject}`, contents: flex }]

  try {
    await sendMessage(lineUserId, messages)
    await logMessage({ userId, lineUserId, messageType: isOverdue ? 'task_overdue' : 'task_reminder', refType: 'activity', refId: activity.id, arCode: activity.ar_code, payload: messages, success: true })
  } catch (e) {
    await logMessage({ userId, lineUserId, messageType: 'task_reminder', payload: messages, success: false, errorMessage: e.message })
  }
}

// ─────────────────────────────────────────────────────────────
// 👤 New Assignment — แจ้งได้รับลูกค้าใหม่
// ─────────────────────────────────────────────────────────────
async function sendNewAssignment(lineUserId, userId, customer) {
  const messages = [{
    type: 'flex',
    altText: `🎯 ได้รับลูกค้าใหม่: ${customer.name_1}`,
    contents: {
      type: 'bubble',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: '#059669', paddingAll: '15px',
        contents: [{ type: 'text', text: '🎯 ได้รับมอบหมายลูกค้าใหม่!', color: '#ffffff', weight: 'bold' }]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'text', text: customer.name_1, weight: 'bold', size: 'lg', wrap: true },
          _infoRow('🏷️ รหัส', customer.code),
          ...(customer.province ? [_infoRow('📍 จังหวัด', customer.province)] : [])
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', paddingAll: '15px',
        contents: [{
          type: 'button', style: 'primary', color: '#059669',
          action: { type: 'uri', label: '👤 ดูข้อมูลลูกค้า', uri: `${process.env.FRONTEND_URL}/line/customers/${customer.code}` }
        }]
      }
    }
  }]

  try {
    await sendMessage(lineUserId, messages)
    await logMessage({ userId, lineUserId, messageType: 'new_assignment', arCode: customer.code, payload: messages, success: true })
  } catch (e) {
    await logMessage({ userId, lineUserId, messageType: 'new_assignment', payload: messages, success: false, errorMessage: e.message })
  }
}

// ─────────────────────────────────────────────────────────────
// Helpers — UI Components
// ─────────────────────────────────────────────────────────────
function _statBox(label, value, color = '#374151') {
  return {
    type: 'box', layout: 'vertical', flex: 1,
    backgroundColor: '#ffffff', cornerRadius: '6px',
    paddingAll: '10px', alignItems: 'center',
    contents: [
      { type: 'text', text: String(value), size: 'xl', weight: 'bold', color, align: 'center' },
      { type: 'text', text: label, size: 'xxs', color: '#6b7280', align: 'center', wrap: true }
    ]
  }
}

function _infoRow(label, value) {
  return {
    type: 'box', layout: 'baseline', spacing: 'sm',
    contents: [
      { type: 'text', text: label, size: 'sm', color: '#6b7280', flex: 2 },
      { type: 'text', text: String(value || '—'), size: 'sm', color: '#111827', flex: 5, wrap: true }
    ]
  }
}

module.exports = {
  sendMessage, replyMessage, replyOrPush, sendDailySummary, sendTaskReminder,
  sendNewAssignment, buildCustomerCard, logMessage
}
