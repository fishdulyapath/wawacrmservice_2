CREATE TABLE IF NOT EXISTS crm_customer_visit_owner (
  ar_code     VARCHAR(20) NOT NULL,
  user_id     INTEGER NOT NULL REFERENCES crm_users(id),
  is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
  assigned_by INTEGER REFERENCES crm_users(id),
  assigned_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ar_code, user_id)
);

CREATE INDEX IF NOT EXISTS idx_crm_customer_visit_owner_ar_code
  ON crm_customer_visit_owner(ar_code);

CREATE INDEX IF NOT EXISTS idx_crm_customer_visit_owner_user_id
  ON crm_customer_visit_owner(user_id);

CREATE INDEX IF NOT EXISTS idx_crm_customer_visit_owner_primary
  ON crm_customer_visit_owner(ar_code) WHERE is_primary = TRUE;
