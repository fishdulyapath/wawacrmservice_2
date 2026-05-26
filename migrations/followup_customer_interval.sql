-- Customer-specific follow-up interval.
-- NULL means use crm_followup_settings.default_call_interval_days.

ALTER TABLE crm_customer_profile
  ADD COLUMN IF NOT EXISTS followup_interval_days INTEGER,
  ADD COLUMN IF NOT EXISTS followup_interval_updated_by INTEGER REFERENCES crm_users(id),
  ADD COLUMN IF NOT EXISTS followup_interval_updated_at TIMESTAMP;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_customer_followup_interval_days'
  ) THEN
    ALTER TABLE crm_customer_profile
      ADD CONSTRAINT chk_customer_followup_interval_days
      CHECK (followup_interval_days IS NULL OR followup_interval_days BETWEEN 1 AND 365);
  END IF;
END $$;
