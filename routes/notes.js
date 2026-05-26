const express = require('express')
const router = express.Router()
const multer = require('multer')
const sharp = require('sharp')
const path = require('path')
const fs = require('fs')
const { crmDB } = require('../db')
const { authMiddleware } = require('../middleware/auth')

router.use(authMiddleware)

const UPLOAD_ROOT = path.join(__dirname, '../uploads')
const NOTE_UPLOAD_DIR = path.join(UPLOAD_ROOT, 'notes')
const MAX_SIZE = 20 * 1024 * 1024
const MAX_FILES = 10
const IMAGE_MIME = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic', 'image/heif']

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: MAX_SIZE, files: MAX_FILES },
  fileFilter: (req, file, cb) => cb(null, true)
})

let noteAttachmentsReady = false

async function ensureNoteAttachmentsTable() {
  if (noteAttachmentsReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS crm_note_attachments (
      id SERIAL PRIMARY KEY,
      note_id INTEGER NOT NULL,
      ar_code TEXT NOT NULL,
      user_id INTEGER,
      filename TEXT NOT NULL,
      original_name TEXT NOT NULL,
      mime_type TEXT,
      file_size BIGINT,
      file_path TEXT NOT NULL,
      thumb_path TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await crmDB.query(`CREATE INDEX IF NOT EXISTS idx_crm_note_attachments_note_id ON crm_note_attachments(note_id)`)
  await crmDB.query(`CREATE INDEX IF NOT EXISTS idx_crm_note_attachments_ar_code ON crm_note_attachments(ar_code)`)
  noteAttachmentsReady = true
}

router.use(async (req, res, next) => {
  try {
    await ensureNoteAttachmentsTable()
    next()
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

function safeSegment(value) {
  return String(value || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80) || 'unknown'
}

function toBool(value) {
  return value === true || value === 'true' || value === '1' || value === 1
}

function deleteUploadFiles(att) {
  ;[att.file_path, att.thumb_path].filter(Boolean).forEach(p => {
    try { fs.unlinkSync(path.join(UPLOAD_ROOT, p)) } catch {}
  })
}

async function saveNoteFiles(noteId, arCode, files, userId) {
  if (!files?.length) return []

  const noteDir = path.join(NOTE_UPLOAD_DIR, safeSegment(arCode), String(noteId))
  fs.mkdirSync(noteDir, { recursive: true })

  const saved = []

  for (const file of files) {
    const isImage = IMAGE_MIME.includes(file.mimetype)
    const ext = path.extname(file.originalname).toLowerCase() || (isImage ? '.jpg' : '')
    const baseName = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`
    const fileName = `${baseName}${ext}`
    const filePath = path.join(noteDir, fileName)
    let fileSize = file.size

    try {
      if (isImage) {
        const outName = `${baseName}.webp`
        const outPath = path.join(noteDir, outName)
        const info = await sharp(file.buffer)
          .rotate()
          .resize(1920, 1920, { fit: 'inside', withoutEnlargement: true })
          .webp({ quality: 82 })
          .toFile(outPath)

        const thumbName = `thumb_${baseName}.webp`
        const thumbPath = path.join(noteDir, thumbName)
        await sharp(file.buffer)
          .rotate()
          .resize(480, 480, { fit: 'cover' })
          .webp({ quality: 74 })
          .toFile(thumbPath)

        fileSize = info.size
        const relOut = `notes/${safeSegment(arCode)}/${noteId}/${outName}`
        const relThumb = `notes/${safeSegment(arCode)}/${noteId}/${thumbName}`

        const row = await crmDB.query(
          `INSERT INTO crm_note_attachments
             (note_id, ar_code, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           RETURNING *`,
          [noteId, arCode, userId, outName, file.originalname, 'image/webp', fileSize, relOut, relThumb]
        )
        saved.push(row.rows[0])
      } else {
        fs.writeFileSync(filePath, file.buffer)
        const relPath = `notes/${safeSegment(arCode)}/${noteId}/${fileName}`

        const row = await crmDB.query(
          `INSERT INTO crm_note_attachments
             (note_id, ar_code, user_id, filename, original_name, mime_type, file_size, file_path, thumb_path)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           RETURNING *`,
          [noteId, arCode, userId, fileName, file.originalname, file.mimetype, fileSize, relPath, null]
        )
        saved.push(row.rows[0])
      }
    } catch (err) {
      console.error('[Note Upload Error]', file.originalname, err.message)
    }
  }

  return saved
}

// GET /api/notes?ar_code=xxx
router.get('/', async (req, res) => {
  const { ar_code } = req.query
  if (!ar_code) return res.status(400).json({ error: 'ต้องระบุ ar_code' })
  try {
    const result = await crmDB.query(`
      SELECT n.*, u.name AS created_by_name,
        COALESCE(att.attachments, '[]'::json) AS attachments
      FROM crm_notes n
      LEFT JOIN crm_users u ON u.id = n.created_by
      LEFT JOIN LATERAL (
        SELECT json_agg(a ORDER BY a.created_at DESC) AS attachments
        FROM crm_note_attachments a
        WHERE a.note_id = n.id
      ) att ON true
      WHERE n.ar_code = $1
      ORDER BY n.is_pinned DESC, n.created_at DESC
    `, [ar_code])
    res.json(result.rows)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /api/notes
router.post('/', upload.array('files', MAX_FILES), async (req, res) => {
  const { ar_code } = req.body
  const noteText = String(req.body.note_text || '').trim()
  const files = req.files || []
  if (!ar_code || (!noteText && files.length === 0)) {
    return res.status(400).json({ error: 'ต้องระบุ ar_code และบันทึกหรือไฟล์แนบ' })
  }

  try {
    const result = await crmDB.query(`
      INSERT INTO crm_notes (ar_code, note_text, is_pinned, created_by)
      VALUES ($1, $2, $3, $4)
      RETURNING *
    `, [ar_code, noteText || 'แนบไฟล์', toBool(req.body.is_pinned), req.user.id])

    const note = result.rows[0]
    note.attachments = await saveNoteFiles(note.id, ar_code, files, req.user.id)
    note.created_by_name = req.user.name || req.user.code || null
    res.status(201).json(note)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// PATCH /api/notes/:id/pin - toggle pin
router.patch('/:id/pin', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_notes WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบบันทึก' })
    const result = await crmDB.query(
      `UPDATE crm_notes SET is_pinned = NOT is_pinned WHERE id=$1 RETURNING *`,
      [req.params.id]
    )
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// DELETE /api/notes/:id
router.delete('/:id', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_notes WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบบันทึก' })

    const note = existing.rows[0]
    if (note.created_by !== req.user.id && !['admin','manager'].includes(req.user.role)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์ลบบันทึกนี้' })
    }

    const attachments = await crmDB.query('SELECT * FROM crm_note_attachments WHERE note_id=$1', [req.params.id])
    await crmDB.query('DELETE FROM crm_note_attachments WHERE note_id=$1', [req.params.id])
    await crmDB.query('DELETE FROM crm_notes WHERE id=$1', [req.params.id])
    attachments.rows.forEach(deleteUploadFiles)
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
