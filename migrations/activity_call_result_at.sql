-- Migration: separate call result timestamp from activity feed updated_at.
-- updated_at is now the latest activity movement time; call_result_at is the
-- timestamp used for call retry attempt counting.

ALTER TABLE crm_activities
  ADD COLUMN IF NOT EXISTS call_result_at TIMESTAMP;

UPDATE crm_activities
SET call_result_at = COALESCE(cdr_end_stamp, cdr_start_stamp, updated_at, created_at)
WHERE activity_type = 'call'
  AND call_result IS NOT NULL
  AND call_result_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_crm_activities_call_result_at
  ON crm_activities(call_result_at DESC)
  WHERE activity_type = 'call' AND call_result IS NOT NULL;
