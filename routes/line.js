const express   = require('express')
const router    = express.Router()
const crypto    = require('crypto')
const { crmDB, posDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')
const lineService = require('../services/lineService')

const CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET

// ─────────────────────────────────────────────────────────────
// Middleware: ตรวจ LINE Signature
// ─────────────────────────────────────────────────────────────
function verifyLineSignature(req, res, next) {
  const signature = req.headers['x-line-signature']
  if (!signature) return res.status(401).send('Missing signature')

  // req.body เป็น Buffer จาก express.raw (ใน index.js)
  const rawBody = Buffer.isBuffer(req.body)
    ? req.body
    : Buffer.from(JSON.stringify(req.body))

  const hash = crypto
    .createHmac('sha256', CHANNEL_SECRET)
    .update(rawBody)
    .digest('base64')

  if (hash !== signature) {
    console.error('[LINE Webhook] Invalid signature — ตรวจสอบ CHANNEL_SECRET')
    return res.status(401).send('Invalid signature')
  }

  // parse JSON จาก raw body เพื่อให้ handler ใช้ req.body เป็น object
  try {
    req.body = JSON.parse(rawBody.toString())
  } catch {
    return res.status(400).send('Invalid JSON')
  }
  next()
}

// ─────────────────────────────────────────────────────────────
// POST /api/line/webhook  — รับ Event จาก LINE Platform
// ─────────────────────────────────────────────────────────────
router.post('/webhook', verifyLineSignature, async (req, res) => {
  // ตอบ LINE ก่อนทันที (LINE require ตอบ 200 ภายใน 3 วิ)
  res.status(200).json({ status: 'ok' })

  const events = req.body.events || []
  console.log(`[LINE Webhook] ${events.length} event(s)`, events.map(e => `${e.type}/${e.source?.userId}`))

  for (const event of events) {
    // บันทึก webhook log
    await crmDB.query(`
      INSERT INTO crm_line_webhook_log (event_type, line_user_id, raw_body)
      VALUES ($1,$2,$3)
    `, [event.type, event.source?.userId, JSON.stringify(event)]).catch(() => {})

    try {
      // ส่ง replyToken ไปกับทุก handler เพื่อใช้ Reply API (ฟรี) แทน Push API
      if (event.type === 'follow')    await handleFollow(event, event.replyToken)
      if (event.type === 'unfollow')  await handleUnfollow(event)
      if (event.type === 'message')   await handleMessage(event, event.replyToken)
      if (event.type === 'postback')  await handlePostback(event, event.replyToken)
    } catch (e) {
      console.error('[LINE Webhook Error]', e)
    }
  }
})

// ── Follow: เพิ่มเพื่อน LINE Bot ──────────────────────────────
async function handleFollow(event, replyToken) {
  await lineService.replyMessage(replyToken, [{
    type: 'text',
    text: `👋 ยินดีต้อนรับสู่ระบบ WAWA CRM!\n\n` +
          `เพื่อเริ่มใช้งาน กรุณาเข้าสู่ระบบ CRM และทำการ **เชื่อมต่อ LINE** ในหน้าโปรไฟล์ของคุณ\n\n` +
          `หรือส่งรหัสยืนยัน (OTP) ที่ได้รับจากระบบ CRM มาที่นี่`
  }])
}

// ── Unfollow: บล็อก LINE Bot ──────────────────────────────────
async function handleUnfollow(event) {
  const lineUserId = event.source.userId
  await crmDB.query(`
    UPDATE crm_users
    SET line_user_id=NULL, line_display_name=NULL, line_picture_url=NULL, line_linked_at=NULL
    WHERE line_user_id=$1
  `, [lineUserId]).catch(() => {})
}

// ── Message: รับข้อความ ────────────────────────────────────────
async function handleMessage(event, replyToken) {
  const lineUserId = event.source.userId
  const text = (event.message?.text || '').trim()

  // ตรวจว่าเป็น OTP token ผูก account ไหม (6 ตัวเลข หรือ 32 char hex)
  if (/^[0-9]{6}$/.test(text) || /^[a-f0-9]{32}$/i.test(text)) {
    await handleLinkByOTP(lineUserId, text, replyToken)
    return
  }

  // คำสั่งอื่น
  if (text === 'งานวันนี้' || text === 'สรุปงาน') {
    await triggerDailySummaryForUser(lineUserId, replyToken)
    return
  }

  // Default message — ตอบกลับด้วย replyToken (ฟรี)
  await lineService.replyMessage(replyToken, [{
    type: 'text',
    text: `คำสั่งที่ใช้ได้:\n` +
          `• ส่ง OTP เพื่อผูก account\n` +
          `• "งานวันนี้" — ดูสรุปงาน\n\n` +
          `📱 เข้าดูงานได้ที่: ${process.env.FRONTEND_URL}/line`
  }])
}

// ── OTP Link: ผูก LINE กับ Employee ───────────────────────────
async function handleLinkByOTP(lineUserId, token, replyToken) {
  const tokenResult = await crmDB.query(`
    SELECT t.user_id, t.expires_at, u.name, u.code
    FROM crm_line_link_token t
    JOIN crm_users u ON u.id = t.user_id
    WHERE t.token=$1 AND t.is_used=FALSE AND t.expires_at > NOW()
  `, [token])

  if (tokenResult.rows.length === 0) {
    await lineService.replyOrPush(replyToken, lineUserId, [{
      type: 'text',
      text: '❌ รหัสยืนยันไม่ถูกต้องหรือหมดอายุ\nกรุณาขอรหัสใหม่จากระบบ CRM'
    }])
    return
  }

  const { user_id, name, code } = tokenResult.rows[0]

  // ดึง Profile จาก LINE
  let lineProfile = {}
  try {
    const profileRes = await fetch(`https://api.line.me/v2/bot/profile/${lineUserId}`, {
      headers: { 'Authorization': `Bearer ${process.env.LINE_CHANNEL_ACCESS_TOKEN}` }
    })
    lineProfile = await profileRes.json()
  } catch {}

  // ตรวจว่า line_user_id นี้ถูกใช้โดย user คนอื่นหรือไม่
  const existingLink = await crmDB.query(
    `SELECT id, code FROM crm_users WHERE line_user_id=$1`,
    [lineUserId]
  )
  if (existingLink.rows.length > 0 && existingLink.rows[0].id !== user_id) {
    await lineService.replyOrPush(replyToken, lineUserId, [{
      type: 'text',
      text: `❌ LINE account นี้เชื่อมต่อกับรหัสพนักงาน "${existingLink.rows[0].code}" อยู่แล้ว\nหากต้องการเชื่อมต่อใหม่ กรุณาติดต่อผู้ดูแลระบบ`
    }])
    return
  }

  // ถ้าเชื่อมต่อกับ user คนนี้แล้ว (re-link)
  if (existingLink.rows.length > 0 && existingLink.rows[0].id === user_id) {
    await lineService.replyOrPush(replyToken, lineUserId, [{
      type: 'text',
      text: `ℹ️ LINE account นี้เชื่อมต่อกับรหัสพนักงาน "${code}" อยู่แล้ว`
    }])
    await crmDB.query(`UPDATE crm_line_link_token SET is_used=TRUE WHERE token=$1`, [token])
    return
  }

  // อัปเดต crm_users
  await crmDB.query(`
    UPDATE crm_users
    SET line_user_id=$1, line_display_name=$2, line_picture_url=$3, line_linked_at=NOW()
    WHERE id=$4
  `, [lineUserId, lineProfile.displayName || name, lineProfile.pictureUrl || null, user_id])

  // Mark token ว่าใช้แล้ว
  await crmDB.query(`UPDATE crm_line_link_token SET is_used=TRUE WHERE token=$1`, [token])

  await lineService.replyOrPush(replyToken, lineUserId, [{
    type: 'flex',
    altText: `✅ เชื่อมต่อ LINE สำเร็จ! ยินดีต้อนรับ คุณ${name}`,
    contents: {
      type: 'bubble',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: '#059669', paddingAll: '15px',
        contents: [{ type: 'text', text: '✅ เชื่อมต่อสำเร็จ!', color: '#ffffff', weight: 'bold', size: 'lg' }]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'text', text: `ยินดีต้อนรับ คุณ${name}`, weight: 'bold', size: 'md' },
          { type: 'text', text: `รหัสพนักงาน: ${code}`, size: 'sm', color: '#6b7280' },
          { type: 'text', text: 'คุณจะได้รับการแจ้งเตือนงานผ่าน LINE นี้', size: 'sm', margin: 'md', wrap: true, color: '#374151' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', paddingAll: '15px',
        contents: [{
          type: 'button', style: 'primary', color: '#2563eb',
          action: { type: 'uri', label: '📋 ดูงานของฉัน', uri: `${process.env.FRONTEND_URL}/line/tasks` }
        }]
      }
    }
  }])
}

// ── Postback ────────────────────────────────────────────────
async function handlePostback(event, replyToken) {
  const data = event.postback?.data || ''
  const lineUserId = event.source.userId
  // รองรับ action ในอนาคต เช่น done_task, log_call
  console.log('[Postback]', lineUserId, data)
}

// ── Trigger Daily Summary สำหรับ User คนเดียว ──────────────
async function triggerDailySummaryForUser(lineUserId, replyToken) {
  const result = await crmDB.query(`
    SELECT * FROM v_daily_summary_per_user WHERE line_user_id=$1
  `, [lineUserId])
  if (result.rows.length > 0) {
    await lineService.sendDailySummary(result.rows[0], replyToken)
  }
}

// ─────────────────────────────────────────────────────────────
// POST /api/line/generate-otp  — สร้าง OTP สำหรับผูก LINE
// ต้อง Login ก่อน (authMiddleware)
// ─────────────────────────────────────────────────────────────
router.post('/generate-otp', authMiddleware, async (req, res) => {
  try {
    const userId = req.user.id

    // ยกเลิก OTP เก่าของ user นี้
    await crmDB.query(`
      UPDATE crm_line_link_token SET is_used=TRUE WHERE user_id=$1 AND is_used=FALSE
    `, [userId])

    // สร้าง OTP ใหม่ (6 หลัก อ่านง่าย)
    const otp       = String(Math.floor(100000 + Math.random() * 900000))
    const expiresAt = new Date(Date.now() + 10 * 60 * 1000) // 10 นาที

    await crmDB.query(`
      INSERT INTO crm_line_link_token (user_id, token, expires_at)
      VALUES ($1, $2, $3)
    `, [userId, otp, expiresAt])

    res.json({
      otp,
      expires_at: expiresAt,
      bot_id:    process.env.LINE_BOT_BASIC_ID,     // @bc-crm หรือ ตาม config
      qr_url:    `https://line.me/R/ti/p/${process.env.LINE_BOT_BASIC_ID}`,
      instructions: `ส่งรหัส ${otp} ไปหา LINE Bot ภายใน 10 นาที`
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/line/status  — ดูสถานะการผูก LINE
// ─────────────────────────────────────────────────────────────
router.get('/status', authMiddleware, async (req, res) => {
  try {
    const result = await crmDB.query(`
      SELECT line_user_id, line_display_name, line_picture_url,
             line_linked_at, line_notify_enabled, line_notify_time
      FROM crm_users WHERE id=$1
    `, [req.user.id])
    const u = result.rows[0]
    res.json({
      linked:           !!u.line_user_id,
      line_display_name: u.line_display_name,
      line_picture_url:  u.line_picture_url,
      line_linked_at:    u.line_linked_at,
      notify_enabled:    u.line_notify_enabled,
      notify_time:       u.line_notify_time
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PUT /api/line/settings  — ตั้งค่า Notification
// ─────────────────────────────────────────────────────────────
router.put('/settings', authMiddleware, async (req, res) => {
  const { notify_enabled, notify_time } = req.body
  try {
    await crmDB.query(`
      UPDATE crm_users
      SET line_notify_enabled=$1, line_notify_time=$2
      WHERE id=$3
    `, [notify_enabled, notify_time || '08:00', req.user.id])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/line/unlink  — ยกเลิกการผูก LINE
// ─────────────────────────────────────────────────────────────
router.post('/unlink', authMiddleware, async (req, res) => {
  try {
    await crmDB.query(`
      UPDATE crm_users
      SET line_user_id=NULL, line_display_name=NULL, line_picture_url=NULL, line_linked_at=NULL
      WHERE id=$1
    `, [req.user.id])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/line/send-daily  — Manual trigger (test / cron)
// ─────────────────────────────────────────────────────────────
router.post('/send-daily', authMiddleware, async (req, res) => {
  try {
    const result = await crmDB.query(`SELECT * FROM v_daily_summary_per_user`)
    let sent = 0, failed = 0

    for (const user of result.rows) {
      try {
        await lineService.sendDailySummary(user)
        sent++
      } catch { failed++ }
    }

    res.json({ success: true, sent, failed, total: result.rows.length })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/line/log-call  — บันทึก Log การโทรจาก LINE
// ─────────────────────────────────────────────────────────────
router.post('/log-call', authMiddleware, async (req, res) => {
  const { ar_code, phone_number, call_status, duration_sec, notes, activity_id } = req.body
  try {
    const result = await crmDB.query(`
      INSERT INTO crm_call_log
        (ar_code, user_id, call_direction, phone_number, call_status,
         duration_sec, initiated_from, activity_id, notes, call_ended_at)
      VALUES ($1,$2,'outbound',$3,$4,$5,'line',$6,$7,NOW())
      RETURNING id
    `, [ar_code, req.user.id, phone_number, call_status || 'initiated',
        duration_sec || 0, activity_id || null, notes || null])

    // อัปเดต last_contacted ใน crm_customer_profile
    await crmDB.query(`
      UPDATE crm_customer_profile
      SET last_contacted=NOW()
      WHERE ar_code=$1
    `, [ar_code])

    res.json({ success: true, call_log_id: result.rows[0].id })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
