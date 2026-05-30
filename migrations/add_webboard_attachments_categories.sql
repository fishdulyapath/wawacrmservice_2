-- Migration: Webboard attachments + category created_by

-- เพิ่ม created_by ใน categories (สำหรับ permission check)
ALTER TABLE crm_webboard_categories ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES crm_users(id);

-- Attachments สำหรับ thread (โพสต์หลัก)
CREATE TABLE IF NOT EXISTS crm_webboard_thread_attachments (
  id            SERIAL       PRIMARY KEY,
  thread_id     INTEGER      NOT NULL REFERENCES crm_webboard_threads(id) ON DELETE CASCADE,
  user_id       INTEGER      NOT NULL REFERENCES crm_users(id),
  filename      VARCHAR(255) NOT NULL,
  original_name VARCHAR(255) NOT NULL,
  mime_type     VARCHAR(100) NOT NULL,
  file_size     INTEGER      NOT NULL,
  file_path     TEXT         NOT NULL,
  thumb_path    TEXT,
  created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_thread_att_thread ON crm_webboard_thread_attachments(thread_id);

-- Attachments สำหรับ comment
CREATE TABLE IF NOT EXISTS crm_webboard_comment_attachments (
  id            SERIAL       PRIMARY KEY,
  comment_id    INTEGER      NOT NULL REFERENCES crm_webboard_comments(id) ON DELETE CASCADE,
  user_id       INTEGER      NOT NULL REFERENCES crm_users(id),
  filename      VARCHAR(255) NOT NULL,
  original_name VARCHAR(255) NOT NULL,
  mime_type     VARCHAR(100) NOT NULL,
  file_size     INTEGER      NOT NULL,
  file_path     TEXT         NOT NULL,
  thumb_path    TEXT,
  created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wb_comment_att_comment ON crm_webboard_comment_attachments(comment_id);
