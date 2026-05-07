-- Follow-up Policy Phase 3 + 4

ALTER TABLE crm_followup_settings
  ADD COLUMN IF NOT EXISTS last_auto_create_checked_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_auto_create_created_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_auto_create_unassigned_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_auto_create_error TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_activities_retry_group_open
  ON crm_activities(retry_group_key)
  WHERE retry_group_key IS NOT NULL
    AND followup_type = 'no_answer_retry'
    AND status NOT IN ('done','cancelled','deleted');
