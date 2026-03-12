const express = require('express')
const router  = express.Router()
const jwt     = require('jsonwebtoken')
const { posDB, crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')

const MAX_ATTEMPTS    = parseInt(process.env.MAX_LOGIN_ATTEMPTS  || '5')
const LOCK_MINUTES    = parseInt(process.env.LOCK_DURATION_MINUTES || '15')
const JWT_SECRET      = process.env.JWT_SECRET
const JWT_EXPIRES     = process.env.JWT_EXPIRES_IN    || '8h'
const JWT_REF_EXPIRES = process.env.JWT_REFRESH_EXPIRES_IN || '7d'
const LIFF_CHANNEL_ID = process.env.LIFF_CHANNEL_ID  || process.env.LINE_LOGIN_CHANNEL_ID || ''

function getClientIp(req) {
  return req.headers['x-forwarded-for']?.split(',')[0]?.trim()
      || req.socket?.remoteAddress
      || 'unknown'
}

// ─────────────────────────────────────────────
// POST /api/auth/login
// body: { code, password }
// ─────────────────────────────────────────────
router.post('/login', async (req, res) => {
  const { code: rawCode, password } = req.body
  const code      = rawCode?.trim().toUpperCase()
  const ip        = getClientIp(req)
  const userAgent = req.headers['user-agent'] || ''

  if (!code || !password) {
    return res.status(400).json({ error: 'กรุณากรอกรหัสพนักงานและรหัสผ่าน' })
  }

  try {
    // ── 1. ตรวจ Password กับ erp_user (POS DB) ก่อนเลย ──
    const posResult = await posDB.query(
      `SELECT code, name_1, password FROM erp_user WHERE code = $1`,
      [code]
    )

    if (posResult.rows.length === 0) {
      await _logLogin(null, code, false, 'user_not_found', ip, userAgent)
      return res.status(401).json({ error: 'รหัสพนักงานหรือรหัสผ่านไม่ถูกต้อง' })
    }

    const posUser = posResult.rows[0]

    // POS เก็บ password เป็น plaintext (ตรวจก่อน ค่อย load/create crm_user)
    const passwordMatch = posUser.password === password

    // ── 2. ดึง crm_user — ถ้าไม่มีให้ auto-create ──
    let crmResult = await crmDB.query(
      `SELECT id, code, name, role, is_active, failed_attempts, locked_until
       FROM crm_users WHERE code = $1`,
      [code]
    )

    if (crmResult.rows.length === 0) {
      // Auto-create จาก erp_user (role = sales_rep โดย default)
      crmResult = await crmDB.query(
        `INSERT INTO crm_users (code, name, role)
         VALUES ($1, $2, 'sales_rep')
         RETURNING id, code, name, role, is_active, failed_attempts, locked_until`,
        [posUser.code, posUser.name_1]
      )
    }

    const user = crmResult.rows[0]

    // ── 3. ตรวจ Account status ──────────────────
    if (!user.is_active) {
      await _logLogin(user.id, code, false, 'inactive', ip, userAgent)
      return res.status(403).json({ error: 'บัญชีถูกระงับ กรุณาติดต่อผู้ดูแลระบบ' })
    }

    // ── 4. ตรวจว่าถูกล็อคอยู่ไหม ──────────────
    if (user.locked_until && new Date(user.locked_until) > new Date()) {
      const remain = Math.ceil((new Date(user.locked_until) - new Date()) / 60000)
      await _logLogin(user.id, code, false, 'locked', ip, userAgent)
      return res.status(403).json({
        error: `บัญชีถูกล็อคชั่วคราว กรุณาลองอีกครั้งใน ${remain} นาที`
      })
    }

    if (!passwordMatch) {
      // เพิ่ม failed_attempts
      const newAttempts = user.failed_attempts + 1
      let lockedUntil = null

      if (newAttempts >= MAX_ATTEMPTS) {
        lockedUntil = new Date(Date.now() + LOCK_MINUTES * 60 * 1000)
      }

      await crmDB.query(
        `UPDATE crm_users SET failed_attempts=$1, locked_until=$2 WHERE id=$3`,
        [newAttempts, lockedUntil, user.id]
      )

      await _logLogin(user.id, code, false, 'wrong_password', ip, userAgent)

      const remaining = MAX_ATTEMPTS - newAttempts
      if (remaining > 0) {
        return res.status(401).json({
          error: `รหัสผ่านไม่ถูกต้อง (เหลืออีก ${remaining} ครั้ง)`
        })
      } else {
        return res.status(403).json({
          error: `รหัสผ่านผิดเกิน ${MAX_ATTEMPTS} ครั้ง บัญชีถูกล็อค ${LOCK_MINUTES} นาที`
        })
      }
    }

    // ── 5. Login สำเร็จ — Reset failed attempts ─
    await crmDB.query(
      `UPDATE crm_users
       SET failed_attempts=0, locked_until=NULL, last_login=NOW(), last_login_ip=$1
       WHERE id=$2`,
      [ip, user.id]
    )

    // ── 6. สร้าง JWT ─────────────────────────────
    const tokenPayload = { sub: user.id, code: user.code, role: user.role }
    const token        = jwt.sign(tokenPayload, JWT_SECRET, { expiresIn: JWT_EXPIRES })
    const refreshToken = jwt.sign({ sub: user.id, type: 'refresh' }, JWT_SECRET, { expiresIn: JWT_REF_EXPIRES })

    const expiresAt = new Date()
    expiresAt.setHours(expiresAt.getHours() + 8)

    const refreshExpiresAt = new Date()
    refreshExpiresAt.setDate(refreshExpiresAt.getDate() + 7)

    // ── 7. บันทึก Session ────────────────────────
    await crmDB.query(`
      INSERT INTO crm_sessions (user_id, token, refresh_token, ip_address, user_agent, expires_at)
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [user.id, token, refreshToken, ip, userAgent, expiresAt])

    await _logLogin(user.id, code, true, null, ip, userAgent)

    res.json({
      token,
      refresh_token: refreshToken,
      expires_at: expiresAt,
      user: {
        id:   user.id,
        code: user.code,
        name: user.name || posUser.name_1,
        role: user.role
      }
    })

  } catch (err) {
    console.error('[Login Error]', err)
    res.status(500).json({ error: 'เกิดข้อผิดพลาด กรุณาลองใหม่' })
  }
})

// ─────────────────────────────────────────────
// POST /api/auth/liff
// LIFF Auto-Auth: ใช้ LIFF Access Token แลก CRM JWT
// ─────────────────────────────────────────────
router.post('/liff', async (req, res) => {
  const { liffAccessToken } = req.body
  const ip        = getClientIp(req)
  const userAgent = req.headers['user-agent'] || ''

  console.log('[LIFF Auth] Request received', {
    hasToken: !!liffAccessToken,
    tokenLength: liffAccessToken?.length,
    ip,
    contentType: req.headers['content-type']
  })

  if (!liffAccessToken) {
    return res.status(400).json({ error: 'กรุณาส่ง liffAccessToken' })
  }

  try {
    // ── 1. Verify token กับ LINE API ──
    console.log('[LIFF Auth] Verifying token with LINE API...')
    const verifyRes = await fetch(
      `https://api.line.me/oauth2/v2.1/verify?access_token=${encodeURIComponent(liffAccessToken)}`
    )
    const verifyData = await verifyRes.json()
    console.log('[LIFF Auth] Verify result:', { ok: verifyRes.ok, client_id: verifyData.client_id, error: verifyData.error })

    if (!verifyRes.ok || verifyData.error) {
      return res.status(401).json({ error: 'LIFF Access Token ไม่ถูกต้องหรือหมดอายุ' })
    }

    // ตรวจ Channel ID ว่าตรงกับ LIFF Channel ของเรา (ป้องกัน token จาก app อื่น)
    if (LIFF_CHANNEL_ID && String(verifyData.client_id) !== String(LIFF_CHANNEL_ID)) {
      console.error('[LIFF Auth] Channel ID mismatch:', verifyData.client_id, '!=', LIFF_CHANNEL_ID)
      return res.status(401).json({ error: 'LIFF Token ไม่ตรงกับ Channel ที่กำหนด' })
    }

    // ── 2. ดึง LINE Profile จาก Access Token ──
    console.log('[LIFF Auth] Getting LINE profile...')
    const profileRes = await fetch('https://api.line.me/v2/profile', {
      headers: { 'Authorization': `Bearer ${liffAccessToken}` }
    })
    const profile = await profileRes.json()

    if (!profileRes.ok || !profile.userId) {
      console.error('[LIFF Auth] Profile failed:', { status: profileRes.status, profile })
      return res.status(401).json({ error: 'ไม่สามารถดึงข้อมูล LINE Profile ได้' })
    }

    const lineUserId = profile.userId
    console.log('[LIFF Auth] LINE userId:', lineUserId, '| displayName:', profile.displayName)

    // ── 3. หา CRM User ที่ผูก LINE แล้ว ──
    const userResult = await crmDB.query(
      `SELECT id, code, name, role, is_active, locked_until
       FROM crm_users WHERE line_user_id = $1`,
      [lineUserId]
    )
    console.log('[LIFF Auth] CRM user found:', userResult.rows.length > 0 ? userResult.rows[0].code : 'NOT FOUND')

    if (userResult.rows.length === 0) {
      return res.status(404).json({
        error: 'LINE ยังไม่ได้ผูกกับบัญชี CRM',
        code: 'NOT_LINKED',
        message: 'กรุณาผูก LINE กับบัญชี CRM ก่อน โดยส่ง OTP จากระบบ CRM มาที่ LINE Bot'
      })
    }

    const user = userResult.rows[0]

    if (!user.is_active) {
      return res.status(403).json({ error: 'บัญชีถูกระงับ กรุณาติดต่อผู้ดูแลระบบ' })
    }

    if (user.locked_until && new Date(user.locked_until) > new Date()) {
      return res.status(403).json({ error: 'บัญชีถูกล็อคชั่วคราว' })
    }

    // ── 4. สร้าง JWT + Session (เหมือน login ปกติ) ──
    const tokenPayload = { sub: user.id, code: user.code, role: user.role }
    const token        = jwt.sign(tokenPayload, JWT_SECRET, { expiresIn: JWT_EXPIRES })
    const refreshToken = jwt.sign({ sub: user.id, type: 'refresh' }, JWT_SECRET, { expiresIn: JWT_REF_EXPIRES })

    const expiresAt = new Date()
    expiresAt.setHours(expiresAt.getHours() + 8)

    await crmDB.query(`
      INSERT INTO crm_sessions (user_id, token, refresh_token, ip_address, user_agent, expires_at)
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [user.id, token, refreshToken, ip, `LIFF:${userAgent}`, expiresAt])

    // อัปเดต display name + picture (อาจเปลี่ยนได้)
    await crmDB.query(`
      UPDATE crm_users
      SET line_display_name = $1, line_picture_url = $2
      WHERE id = $3
    `, [profile.displayName || user.name, profile.pictureUrl || null, user.id])

    await _logLogin(user.id, user.code, true, null, ip, `LIFF:${userAgent}`)

    res.json({
      token,
      refresh_token: refreshToken,
      expires_at: expiresAt,
      user: {
        id:   user.id,
        code: user.code,
        name: user.name,
        role: user.role
      }
    })

  } catch (err) {
    console.error('[LIFF Auth Error]', err)
    res.status(500).json({ error: 'เกิดข้อผิดพลาดในการยืนยันตัวตน' })
  }
})

// ─────────────────────────────────────────────
// POST /api/auth/logout
// ─────────────────────────────────────────────
router.post('/logout', authMiddleware, async (req, res) => {
  try {
    const header = req.headers['authorization'] || ''
    const token  = header.startsWith('Bearer ') ? header.slice(7) : null

    if (token) {
      await crmDB.query(
        `UPDATE crm_sessions SET is_revoked=TRUE WHERE token=$1`,
        [token]
      )
    }
    res.json({ success: true, message: 'Logout เรียบร้อย' })
  } catch (err) {
    console.error('[Logout Error]', err)
    res.status(500).json({ error: 'เกิดข้อผิดพลาด' })
  }
})

// ─────────────────────────────────────────────
// POST /api/auth/refresh
// body: { refresh_token }
// ─────────────────────────────────────────────
router.post('/refresh', async (req, res) => {
  const { refresh_token } = req.body
  if (!refresh_token) return res.status(400).json({ error: 'ไม่พบ refresh_token' })

  try {
    let payload
    try {
      payload = jwt.verify(refresh_token, JWT_SECRET)
    } catch {
      return res.status(401).json({ error: 'refresh_token ไม่ถูกต้องหรือหมดอายุ' })
    }

    if (payload.type !== 'refresh') {
      return res.status(401).json({ error: 'Token ประเภทไม่ถูกต้อง' })
    }

    // หา session
    const sessionResult = await crmDB.query(`
      SELECT s.id, s.user_id, s.is_revoked,
             u.code, u.name, u.role, u.is_active
      FROM crm_sessions s
      JOIN crm_users u ON u.id = s.user_id
      WHERE s.refresh_token = $1
    `, [refresh_token])

    if (sessionResult.rows.length === 0 || sessionResult.rows[0].is_revoked) {
      return res.status(401).json({ error: 'Session ไม่ถูกต้อง กรุณา Login ใหม่' })
    }

    const s = sessionResult.rows[0]
    if (!s.is_active) {
      return res.status(403).json({ error: 'บัญชีถูกระงับ' })
    }

    // ออก Token ใหม่
    const newToken = jwt.sign(
      { sub: s.user_id, code: s.code, role: s.role },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES }
    )
    const newExpiresAt = new Date()
    newExpiresAt.setHours(newExpiresAt.getHours() + 8)

    await crmDB.query(
      `UPDATE crm_sessions SET token=$1, expires_at=$2, last_used_at=NOW() WHERE id=$3`,
      [newToken, newExpiresAt, s.id]
    )

    res.json({ token: newToken, expires_at: newExpiresAt })

  } catch (err) {
    console.error('[Refresh Error]', err)
    res.status(500).json({ error: 'เกิดข้อผิดพลาด' })
  }
})

// ─────────────────────────────────────────────
// GET /api/auth/me  — ดูข้อมูล User ปัจจุบัน
// ─────────────────────────────────────────────
router.get('/me', authMiddleware, async (req, res) => {
  try {
    const result = await crmDB.query(
      `SELECT id, code, name, email, phone, role, last_login, last_login_ip
       FROM crm_users WHERE id=$1`,
      [req.user.id]
    )
    res.json(result.rows[0] || req.user)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────
// Helper: บันทึก Login Log
// ─────────────────────────────────────────────
async function _logLogin(userId, code, success, failReason, ip, userAgent) {
  try {
    await crmDB.query(`
      INSERT INTO crm_login_log (user_id, user_code, success, ip_address, user_agent, fail_reason)
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [userId, code, success, ip, userAgent, failReason])
  } catch (err) {
    console.error('[Login Log Error]', err.message)
  }
}

module.exports = router
