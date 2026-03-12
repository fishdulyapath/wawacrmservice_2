const express  = require('express')
const router   = express.Router({ mergeParams: true }) // รับ :id จาก parent
const multer   = require('multer')
const sharp    = require('sharp')
const path     = require('path')
const fs       = require('fs')
const { crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')

router.use(authMiddleware)

const UPLOAD_DIR = path.join(__dirname, '../uploads/activities')
const MAX_SIZE   = 20 * 1024 * 1024  // 20 MB per file
const MAX_FILES  = 10
const IMAGE_MIME = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic', 'image/heif']

// multer: เก็บใน memory ก่อน แล้วค่อย process ด้วย sharp
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_SIZE, files: MAX_FILES },
  fileFilter: (req, file, cb) => {
    // รับทุก mime type: รูป + pdf + doc + etc.
    cb(null, true)
  }
})

// ── GET /api/activities/:id/attachments ──────────────────────
router.get('/', async (req, res) => {
  try {
    const result = await crmDB.query(
      `SELECT a.*, u.name AS uploader_name
       FROM crm_activity_attachments a
       LEFT JOIN crm_users u ON u.id = a.user_id
       WHERE a.activity_id = $1
       ORDER BY a.created_at DESC`,
      [req.params.id]
    )
    res.json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ── POST /api/activities/:id/attachments ─────────────────────
router.post('/', upload.array('files', MAX_FILES), async (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'ไม่พบไฟล์ที่อัปโหลด' })
  }

  // ตรวจว่า activity มีอยู่และ user มีสิทธิ์
  try {
    const act = await crmDB.query('SELECT id, created_by FROM crm_activities WHERE id=$1', [req.params.id])
    if (!act.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })
    // sales_rep ต้องเป็น owner (crm_activity_owners) หรือเป็นคนสร้าง
    if (req.user.role === 'sales_rep') {
      const ownerCheck = await crmDB.query(
        'SELECT 1 FROM crm_activity_owners WHERE activity_id=$1 AND user_id=$2',
        [req.params.id, req.user.id]
      )
      const isCreator = act.rows[0].created_by === req.user.id
      if (!ownerCheck.rows.length && !isCreator) {
        return res.status(403).json({ error: 'ไม่มีสิทธิ์' })
      }
    }
  } catch (err) {
    return res.status(500).json({ error: err.message })
  }

  // สร้าง folder: uploads/activities/:activity_id/
  const actDir = path.join(UPLOAD_DIR, String(req.params.id))
  fs.mkdirSync(actDir, { recursive: true })

  const saved = []

  for (const file of req.files) {
    const isImage = IMAGE_MIME.includes(file.mimetype)
    const ext     = path.extname(file.originalname).toLowerCase() || (isImage ? '.jpg' : '')
    const baseName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    const fileName  = `${baseName}${ext}`
    const filePath  = path.join(actDir, fileName)
    let   thumbPath = null
    let   fileSize  = file.size

    try {
      if (isImage) {
        // ลดขนาดรูป: max 1920px, quality 80, แปลงเป็น webp
        const outName = `${baseName}.webp`
        const outPath = path.join(actDir, outName)
        const info = await sharp(file.buffer)
          .rotate()                          // auto-rotate จาก EXIF
          .resize(1920, 1920, { fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 80 })
          .toFile(outPath)

        fileSize = info.size

        // สร้าง thumbnail 400px
        const thumbName = `thumb_${baseName}.webp`
        thumbPath = path.join(actDir, thumbName)
        await sharp(file.buffer)
          .rotate()
          .resize(400, 400, { fit: 'cover' })
          .webp({ quality: 70 })
          .toFile(thumbPath)

        // เก็บ path แบบ relative
        const relOut   = `activities/${req.params.id}/${outName}`
        const relThumb = `activities/${req.params.id}/${thumbName}`

        const row = await crmDB.query(
          `INSERT INTO crm_activity_attachments
             (activity_id, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [req.params.id, req.user.id, outName, file.originalname,
           'image/webp', fileSize, relOut, relThumb]
        )
        saved.push(row.rows[0])

      } else {
        // ไฟล์ทั่วไป: เขียนตรง ไม่ compress
        fs.writeFileSync(filePath, file.buffer)
        const relPath = `activities/${req.params.id}/${fileName}`

        const row = await crmDB.query(
          `INSERT INTO crm_activity_attachments
             (activity_id, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
          [req.params.id, req.user.id, fileName, file.originalname,
           file.mimetype, fileSize, relPath, null]
        )
        saved.push(row.rows[0])
      }
    } catch (err) {
      console.error('[Upload Error]', file.originalname, err.message)
      // ข้าม file นี้ไป ไม่ crash ทั้งหมด
    }
  }

  res.status(201).json(saved)
})

// ── DELETE /api/activities/:id/attachments/:attId ────────────
router.delete('/:attId', async (req, res) => {
  try {
    const result = await crmDB.query(
      `SELECT * FROM crm_activity_attachments WHERE id=$1 AND activity_id=$2`,
      [req.params.attId, req.params.id]
    )
    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบไฟล์' })

    const att = result.rows[0]

    // ตรวจสิทธิ์: เจ้าของไฟล์ หรือ admin/manager เท่านั้น
    if (req.user.role === 'sales_rep' && att.user_id !== req.user.id) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบไฟล์นี้' })
    }

    // ลบไฟล์จาก disk
    const baseDir = path.join(__dirname, '../uploads')
    ;[att.file_path, att.thumb_path].filter(Boolean).forEach(p => {
      try { fs.unlinkSync(path.join(baseDir, p)) } catch {}
    })

    await crmDB.query('DELETE FROM crm_activity_attachments WHERE id=$1', [req.params.attId])
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
