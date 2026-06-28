-- ============================================================
-- Visit Follow-up System — Phase 1 Migration
-- แยก setting งานเยี่ยม (visit) ออกจากงานโทร (call)
-- ============================================================

-- 1. เพิ่ม visit settings ใน crm_followup_settings (singleton id=1)
ALTER TABLE crm_followup_settings
  ADD COLUMN IF NOT EXISTS visit_enabled                      BOOLEAN   NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS visit_auto_create_enabled          BOOLEAN   NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS default_visit_interval_days        INTEGER   NOT NULL DEFAULT 30,
  ADD COLUMN IF NOT EXISTS visit_auto_create_time             TIME      NOT NULL DEFAULT '08:00',
  ADD COLUMN IF NOT EXISTS visit_assignment_mode              VARCHAR(20) NOT NULL DEFAULT 'primary',
  ADD COLUMN IF NOT EXISTS no_met_max_attempts_per_day        INTEGER   NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS no_met_retry_minutes               INTEGER   NOT NULL DEFAULT 60,
  ADD COLUMN IF NOT EXISTS last_visit_create_checked_at       TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_visit_create_created_count    INTEGER   NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_visit_create_unassigned_count INTEGER   NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_visit_create_error            TEXT;

-- Constraints สำหรับ visit settings
ALTER TABLE crm_followup_settings
  DROP CONSTRAINT IF EXISTS chk_visit_interval_days,
  ADD  CONSTRAINT chk_visit_interval_days
       CHECK (default_visit_interval_days BETWEEN 1 AND 365);

ALTER TABLE crm_followup_settings
  DROP CONSTRAINT IF EXISTS chk_no_met_max_attempts,
  ADD  CONSTRAINT chk_no_met_max_attempts
       CHECK (no_met_max_attempts_per_day BETWEEN 1 AND 10);

ALTER TABLE crm_followup_settings
  DROP CONSTRAINT IF EXISTS chk_no_met_retry_minutes,
  ADD  CONSTRAINT chk_no_met_retry_minutes
       CHECK (no_met_retry_minutes BETWEEN 5 AND 480);

ALTER TABLE crm_followup_settings
  DROP CONSTRAINT IF EXISTS chk_visit_assignment_mode,
  ADD  CONSTRAINT chk_visit_assignment_mode
       CHECK (visit_assignment_mode IN ('primary', 'all'));

-- 2. เพิ่ม visit follow-up columns ใน crm_customer_profile
ALTER TABLE crm_customer_profile
  ADD COLUMN IF NOT EXISTS visit_followup_enabled             BOOLEAN   NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS next_visit_followup                DATE,
  ADD COLUMN IF NOT EXISTS visit_followup_pause_until         DATE,
  ADD COLUMN IF NOT EXISTS visit_followup_pause_reason        TEXT,
  ADD COLUMN IF NOT EXISTS visit_followup_interval_days       INTEGER,
  ADD COLUMN IF NOT EXISTS visit_followup_interval_updated_by INTEGER   REFERENCES crm_users(id),
  ADD COLUMN IF NOT EXISTS visit_followup_interval_updated_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS last_visited                       TIMESTAMPTZ;

ALTER TABLE crm_customer_profile
  DROP CONSTRAINT IF EXISTS chk_visit_followup_interval,
  ADD  CONSTRAINT chk_visit_followup_interval
       CHECK (visit_followup_interval_days IS NULL
              OR (visit_followup_interval_days BETWEEN 1 AND 365));

-- 3. Indexes สำหรับ visit follow-up
CREATE INDEX IF NOT EXISTS idx_crm_customer_profile_visit_followup
  ON crm_customer_profile(next_visit_followup)
  WHERE visit_followup_enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_crm_activities_visit_retry_due
  ON crm_activities(retry_due_at)
  WHERE followup_type = 'no_met_retry'
    AND status NOT IN ('done', 'cancelled', 'deleted');

CREATE INDEX IF NOT EXISTS idx_crm_activities_retry_group_visit
  ON crm_activities(retry_group_key)
  WHERE retry_group_key IS NOT NULL
    AND followup_type = 'no_met_retry'
    AND status NOT IN ('done', 'cancelled', 'deleted');
