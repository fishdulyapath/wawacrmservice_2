const express  = require('express')
const router   = express.Router()
const { authMiddleware } = require('../middleware/auth')

const CDR_KEY        = '6cfc65fc23101592cb74e058dfb7eadaa025a3e6aba156266e3c5a826f18c9b5'
const CDR_COMPANY_ID = '9979794d-8a7c-44d3-bef6-56524509238f'
const CDR_BASE       = 'https://client.yalecom.co.th/api/cdr'

router.use(authMiddleware)

// ─────────────────────────────────────────────────────────────
// GET /api/cdr?phone=xxx&date_from=yyyy-mm-dd&date_to=yyyy-mm-dd&direction=outbound
// ─────────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const { phone, date_from, date_to, direction } = req.query

    const params = new URLSearchParams({
      key:        CDR_KEY,
      company_id: CDR_COMPANY_ID,
    })
    if (direction) params.set('direction', direction)
    if (date_from) params.set('date_from', date_from)
    if (date_to)   params.set('date_to',   date_to)

    const url = `${CDR_BASE}?${params}`
    const resp = await fetch(url)
    if (!resp.ok) return res.status(resp.status).json({ error: 'CDR API error' })

    let data = await resp.json()

    // กรองเบอร์ถ้าระบุ (strip non-digits ก่อนเทียบ)
    if (phone) {
      const clean = phone.replace(/\D/g, '')
      data = data.filter(r => {
        const dest = (r.destination_number || '').replace(/\D/g, '')
        const src  = (r.caller_id_number   || '').replace(/\D/g, '')
        return dest.endsWith(clean) || src.endsWith(clean) ||
               clean.endsWith(dest.slice(-9)) || clean.endsWith(src.slice(-9))
      })
    }

    res.json(data)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
