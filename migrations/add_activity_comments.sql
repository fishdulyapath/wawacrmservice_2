-- Migration: ระบบ comment + แนบไฟล์ใน ActivityDetail
-- ผูกกับ activity_id โดยตรง แยกจาก crm_notes

CREATE TABLE IF NOT EXISTS crm_activity_comments (
  id           SERIAL    PRIMARY KEY,
  activity_id  INTEGER   NOT NULL REFERENCES crm_activities(id) ON DELETE CASCADE,
  user_id      INTEGER   NOT NULL REFERENCES crm_users(id),
  comment_text TEXT      NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_activity_comments_activity_id ON crm_activity_comments(activity_id);

CREATE TABLE IF NOT EXISTS crm_activity_comment_attachments (
  id            SERIAL       PRIMARY KEY,
  comment_id    INTEGER      NOT NULL REFERENCES crm_activity_comments(id) ON DELETE CASCADE,
  user_id       INTEGER      NOT NULL REFERENCES crm_users(id),
  filename      VARCHAR(255) NOT NULL,
  original_name VARCHAR(255) NOT NULL,
  mime_type     VARCHAR(100) NOT NULL,
  file_size     INTEGER      NOT NULL,
  file_path     TEXT         NOT NULL,
  thumb_path    TEXT,
  created_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_comment_attachments_comment_id ON crm_activity_comment_attachments(comment_id);
