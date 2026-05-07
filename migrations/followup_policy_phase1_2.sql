-- Follow-up Policy Phase 1 + 2

CREATE TABLE IF NOT EXISTS crm_followup_settings (
  id                               INTEGER PRIMARY KEY DEFAULT 1,
  enabled                          BOOLEAN NOT NULL DEFAULT FALSE,
  auto_create_enabled              BOOLEAN NOT NULL DEFAULT TRUE,
  default_call_interval_days        INTEGER NOT NULL DEFAULT 30,
  auto_create_time                 TIME    NOT NULL DEFAULT '08:00',
  assignment_mode                  VARCHAR(20) NOT NULL DEFAULT 'primary',
  no_owner_action                  VARCHAR(20) NOT NULL DEFAULT 'queue',
  no_answer_max_attempts_per_day    INTEGER NOT NULL DEFAULT 3,
  no_answer_retry_minutes          INTEGER NOT NULL DEFAULT 30,
  business_start_time               TIME    NOT NULL DEFAULT '08:30',
  business_end_time                 TIME    NOT NULL DEFAULT '17:30',
  updated_by                       INTEGER REFERENCES crm_users(id),
  updated_at                       TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_followup_settings_singleton CHECK (id = 1),
  CONSTRAINT chk_followup_call_interval CHECK (default_call_interval_days BETWEEN 1 AND 365),
  CONSTRAINT chk_followup_assignment_mode CHECK (assignment_mode IN ('primary','all')),
  CONSTRAINT chk_followup_no_owner_action CHECK (no_owner_action IN ('queue')),
  CONSTRAINT chk_followup_no_answer_attempts CHECK (no_answer_max_attempts_per_day BETWEEN 1 AND 10),
  CONSTRAINT chk_followup_retry_minutes CHECK (no_answer_retry_minutes BETWEEN 5 AND 480),
  CONSTRAINT chk_followup_business_hours CHECK (business_start_time < business_end_time)
);

ALTER TABLE crm_followup_settings
  ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS auto_create_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS default_call_interval_days INTEGER NOT NULL DEFAULT 30,
  ADD COLUMN IF NOT EXISTS auto_create_time TIME NOT NULL DEFAULT '08:00',
  ADD COLUMN IF NOT EXISTS assignment_mode VARCHAR(20) NOT NULL DEFAULT 'primary',
  ADD COLUMN IF NOT EXISTS no_owner_action VARCHAR(20) NOT NULL DEFAULT 'queue',
  ADD COLUMN IF NOT EXISTS no_answer_max_attempts_per_day INTEGER NOT NULL DEFAULT 3,
  ADD COLUMN IF NOT EXISTS no_answer_retry_minutes INTEGER NOT NULL DEFAULT 30,
  ADD COLUMN IF NOT EXISTS business_start_time TIME NOT NULL DEFAULT '08:30',
  ADD COLUMN IF NOT EXISTS business_end_time TIME NOT NULL DEFAULT '17:30',
  ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES crm_users(id),
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_settings_singleton'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_settings_singleton CHECK (id = 1);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_call_interval'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_call_interval CHECK (default_call_interval_days BETWEEN 1 AND 365);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_assignment_mode'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_assignment_mode CHECK (assignment_mode IN ('primary','all'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_no_owner_action'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_no_owner_action CHECK (no_owner_action IN ('queue'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_no_answer_attempts'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_no_answer_attempts CHECK (no_answer_max_attempts_per_day BETWEEN 1 AND 10);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_retry_minutes'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_retry_minutes CHECK (no_answer_retry_minutes BETWEEN 5 AND 480);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_followup_business_hours'
  ) THEN
    ALTER TABLE crm_followup_settings
      ADD CONSTRAINT chk_followup_business_hours CHECK (business_start_time < business_end_time);
  END IF;
END $$;

INSERT INTO crm_followup_settings (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE crm_activities
  ADD COLUMN IF NOT EXISTS system_created BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS followup_type VARCHAR(30),
  ADD COLUMN IF NOT EXISTS followup_policy_id INTEGER REFERENCES crm_followup_settings(id),
  ADD COLUMN IF NOT EXISTS followup_key TEXT,
  ADD COLUMN IF NOT EXISTS requires_owner_assignment BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS owner_assignment_note TEXT,
  ADD COLUMN IF NOT EXISTS retry_of_activity_id INTEGER REFERENCES crm_activities(id),
  ADD COLUMN IF NOT EXISTS attempt_no INTEGER,
  ADD COLUMN IF NOT EXISTS retry_due_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS retry_group_key TEXT;

ALTER TABLE crm_customer_profile
  ADD COLUMN IF NOT EXISTS followup_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS followup_pause_until DATE,
  ADD COLUMN IF NOT EXISTS followup_pause_reason TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_crm_activities_followup_key
  ON crm_activities(followup_key)
  WHERE followup_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_crm_activities_followup_type
  ON crm_activities(followup_type);

CREATE INDEX IF NOT EXISTS idx_crm_activities_requires_owner
  ON crm_activities(requires_owner_assignment)
  WHERE requires_owner_assignment = TRUE;

CREATE INDEX IF NOT EXISTS idx_crm_activities_retry_due
  ON crm_activities(retry_due_at)
  WHERE followup_type = 'no_answer_retry' AND status NOT IN ('done','cancelled','deleted');

CREATE INDEX IF NOT EXISTS idx_crm_customer_profile_followup
  ON crm_customer_profile(next_followup)
  WHERE followup_enabled = TRUE;
