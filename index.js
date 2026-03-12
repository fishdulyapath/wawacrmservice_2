require('dotenv').config()
const express   = require('express')
const cors      = require('cors')
const helmet    = require('helmet')
const rateLimit = require('express-rate-limit')
const path      = require('path')
const { posDB, crmDB } = require('./db')

const app  = express()
const PORT = process.env.PORT || 3000

// ── Security headers ────────────────────
// Dev: ปิด CSP เพื่อให้ LIFF SDK ทำงานได้ (inline scripts, LINE CDN)
// Production: ควรตั้ง CSP ที่เหมาะสม
app.use(helmet({
  contentSecurityPolicy: false,   // LIFF SDK ต้อง load จาก LINE CDN + inline scripts
  crossOriginEmbedderPolicy: false,
  crossOriginOpenerPolicy: false,
}))

// ── Rate limiting ───────────────────────
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,  // 15 นาที
  max: 300,                    // จำกัด 300 requests ต่อ IP ต่อ 15 นาที
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later.' }
})

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,                     // Login จำกัด 20 ครั้ง ต่อ 15 นาที
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many login attempts, please try again later.' }
})

app.use('/api/', apiLimiter)
app.use('/api/auth/login', authLimiter)

// CORS: รองรับหลาย origin (production + ngrok + dev)
const allowedOrigins = (process.env.FRONTEND_URL || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)

app.use(cors({
  origin: allowedOrigins.length > 0
    ? (origin, cb) => {
        // อนุญาต requests ที่ไม่มี origin (mobile apps, curl, etc.)
        if (!origin) return cb(null, true)
        // ตรวจว่า origin ตรงกับ allowed list หรือเป็น ngrok
        if (
          allowedOrigins.includes(origin) ||
          origin.endsWith('.ngrok-free.app') ||
          origin.endsWith('.ngrok.io') ||
          origin.endsWith('.web.app') ||
          origin.endsWith('.firebaseapp.com') ||
          origin.startsWith('http://localhost')
        ) {
          return cb(null, true)
        }
        cb(new Error('Not allowed by CORS'))
      }
    : '*',
  credentials: true
}))

// LINE webhook ต้องการ raw body เพื่อ verify signature
app.use('/api/line/webhook', express.raw({ type: 'application/json' }))

// ทุก route อื่นใช้ JSON parser ปกติ
app.use(express.json())

// ── Request logger (dev) ───────────────
if (process.env.NODE_ENV !== 'production') {
  app.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl} (origin: ${req.headers.origin || 'none'})`)
    next()
  })
}

// ── Routes ─────────────────────────────
app.use('/api/liff',       require('./routes/liff'))         // LIFF API
app.use('/api/line',       require('./routes/line'))         // LINE webhooks
app.use('/api/auth',            require('./routes/auth'))          // Public
app.use('/api/customers', require('./routes/customers'))     // Auth required
app.use('/api/employees',      require('./routes/employees'))      // Auth required
app.use('/api/activities',     require('./routes/activities'))     // Auth required
app.use('/api/notifications',  require('./routes/notifications'))  // Auth required
app.use('/api/reports',        require('./routes/reports'))         // Manager+ only
app.use('/api/cdr',           require('./routes/cdr'))             // CDR proxy
app.use('/api/notes',          require('./routes/notes'))          // Auth required
app.use('/api/activities/:id/attachments', require('./routes/attachments'))

// Static: serve uploaded files
app.use('/uploads', express.static(path.join(__dirname, 'uploads')))

// ── Health check ────────────────────────
app.get('/api/health', async (req, res) => {
  const status = { pos: false, crm: false }
  try { await posDB.query('SELECT 1'); status.pos = true } catch {}
  try { await crmDB.query('SELECT 1'); status.crm = true } catch {}
  const ok = status.pos && status.crm
  res.status(ok ? 200 : 503).json({ status: ok ? 'ok' : 'degraded', db: status })
})

// ── Serve Vue Frontend (SPA) — dev only ─
// Production: frontend อยู่ที่ Firebase Hosting แยก
// Dev: proxy ไป Vite dev server (port 5173) เพื่อให้ ngrok ทำงานได้
const frontendDist = path.join(__dirname, '..', 'frontend', 'dist')
const fs = require('fs')
const isDev = process.env.NODE_ENV !== 'production'

if (isDev) {
  const { createProxyMiddleware } = (() => {
    try { return require('http-proxy-middleware') } catch { return {} }
  })()

  if (createProxyMiddleware) {
    const viteProxy = createProxyMiddleware({
      target: 'http://localhost:5173',
      changeOrigin: true,
      ws: true
    })
    app.use((req, res, next) => {
      if (req.path.startsWith('/api/') || req.path.startsWith('/uploads/')) {
        return next()
      }
      viteProxy(req, res, next)
    })
    console.log('🔄 Dev proxy: non-API requests → http://localhost:5173')
  } else if (fs.existsSync(path.join(frontendDist, 'index.html'))) {
    app.use(express.static(frontendDist))
    app.get('*', (req, res) => res.sendFile(path.join(frontendDist, 'index.html')))
    console.log('📦 Serving Vue frontend from ../frontend/dist')
  }
}

// ── 404 handler (API routes ที่ไม่เจอ) ──
app.use((req, res) => res.status(404).json({ error: 'Not found' }))

// ── Error handler ───────────────────────
app.use((err, req, res, next) => {
  console.error(err)
  res.status(500).json({ error: 'Internal server error' })
})

require('./services/cronJobs').start()
app.listen(PORT, () => {
  console.log(`✅ CRM API running → http://localhost:${PORT}`)
  console.log(`   POS DB: ${process.env.POS_HOST}:${process.env.POS_PORT}`)
  console.log(`   CRM DB: ${process.env.CRM_HOST}:${process.env.CRM_PORT}`)
})
