-- New customers should opt in to automatic follow-up explicitly.
ALTER TABLE crm_customer_profile
  ALTER COLUMN followup_enabled SET DEFAULT FALSE;
