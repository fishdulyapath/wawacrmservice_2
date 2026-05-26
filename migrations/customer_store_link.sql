CREATE TABLE IF NOT EXISTS crm_customer_store_link (
  ar_code     TEXT NOT NULL,
  store_id    TEXT NOT NULL,
  link_type   TEXT NOT NULL DEFAULT 'manual',
  confidence  NUMERIC,
  note        TEXT,
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at  TIMESTAMPTZ,
  PRIMARY KEY (ar_code, store_id)
);

CREATE INDEX IF NOT EXISTS idx_crm_customer_store_link_ar_code
  ON crm_customer_store_link(ar_code) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_crm_customer_store_link_store_id
  ON crm_customer_store_link(store_id) WHERE deleted_at IS NULL;
