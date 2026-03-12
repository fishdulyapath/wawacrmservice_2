const jwt = require('jsonwebtoken')
const { crmDB } = require('../db')

/**
 * Middleware: ตรวจสอบ JWT Token
 * ใส่ req.user = { id, code, name, role } เมื่อผ่าน
 */
async function authMiddleware(req, res, next) {
  try {
    const header = req.headers['authorization'] || ''
    const token  = header.startsWith('Bearer ') ? header.slice(7) : null

    if (!token) {
      return res.status(401).json({ error: 'ไม่พบ Token กรุณา Login ก่อน' })
    }

    // Verify JWT
    let payload
    try {
      payload = jwt.verify(token, process.env.JWT_SECRET)
    } catch (e) {
      const msg = e.name === 'TokenExpiredError'
        ? 'Token หมดอายุ กรุณา Login ใหม่'
        : 'Token ไม่ถูกต้อง'
      return res.status(401).json({ error: msg })
    }

    // ตรวจสอบ session ใน DB ว่ายังใช้งานได้ไหม
    const sessionResult = await crmDB.query(`
      SELECT s.id, s.user_id, s.expires_at, s.is_revoked,
             u.code, u.name, u.role, u.is_active, u.locked_until
      FROM crm_sessions s
      JOIN crm_users u ON u.id = s.user_id
      WHERE s.token = $1
    `, [token])

    if (sessionResult.rows.length === 0) {
      return res.status(401).json({ error: 'Session ไม่พบ กรุณา Login ใหม่' })
    }

    const session = sessionResult.rows[0]

    if (session.is_revoked) {
      return res.status(401).json({ error: 'Session ถูกยกเลิกแล้ว กรุณา Login ใหม่' })
    }
    if (new Date(session.expires_at) < new Date()) {
      return res.status(401).json({ error: 'Session หมดอายุ กรุณา Login ใหม่' })
    }
    if (!session.is_active) {
      return res.status(403).json({ error: 'บัญชีถูกระงับ กรุณาติดต่อผู้ดูแลระบบ' })
    }
    if (session.locked_until && new Date(session.locked_until) > new Date()) {
      return res.status(403).json({ error: 'บัญชีถูกล็อคชั่วคราว กรุณาลองใหม่ภายหลัง' })
    }

    // อัปเดต last_used_at ของ session
    await crmDB.query(
      `UPDATE crm_sessions SET last_used_at = NOW() WHERE id = $1`,
      [session.id]
    )

    // แนบ user info + ip ไปกับ request
    req.user       = { id: session.user_id, code: session.code, name: session.name, role: session.role }
    req.sessionId  = session.id
    req.clientIp   = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
                     || req.socket.remoteAddress
                     || 'unknown'

    next()
  } catch (err) {
    console.error('[Auth Middleware]', err)
    res.status(500).json({ error: 'เกิดข้อผิดพลาดในการตรวจสอบสิทธิ์' })
  }
}

/**
 * Middleware: ตรวจสอบ Role
 * ใช้หลัง authMiddleware เช่น requireRole('admin', 'manager')
 */
function requireRole(...roles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'ไม่ได้รับอนุญาต' })
    }
    if (!roles.includes(req.user.role)) {
      return res.status(403).json({
        error: `ไม่มีสิทธิ์ ต้องการสิทธิ์: ${roles.join(' หรือ ')}`
      })
    }
    next()
  }
}

module.exports = { authMiddleware, requireRole }
