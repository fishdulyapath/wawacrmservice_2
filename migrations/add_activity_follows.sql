-- Migration: ระบบติดตามกิจกรรม (activity follow/watch)
-- ผู้ใช้ที่ไม่ใช่ owner สามารถติดตามกิจกรรม เพื่อรับ notification เมื่อมีการอัปเดตหรือ comment

CREATE TABLE IF NOT EXISTS crm_activity_follows (
  activity_id  INTEGER   NOT NULL REFERENCES crm_activities(id) ON DELETE CASCADE,
  user_id      INTEGER   NOT NULL REFERENCES crm_users(id),
  followed_at  TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (activity_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_activity_follows_activity ON crm_activity_follows(activity_id);
CREATE INDEX IF NOT EXISTS idx_activity_follows_user    ON crm_activity_follows(user_id);
