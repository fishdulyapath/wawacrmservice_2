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
);

CREATE INDEX IF NOT EXISTS idx_crm_note_attachments_note_id
  ON crm_note_attachments(note_id);

CREATE INDEX IF NOT EXISTS idx_crm_note_attachments_ar_code
  ON crm_note_attachments(ar_code);
