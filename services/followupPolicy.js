const { crmDB } = require('../db')

let followupCustomerPolicyReady = false
let customerVisitOwnerTableReady = false

async function ensureCustomerFollowupPolicy(client = crmDB) {
  if (followupCustomerPolicyReady) return
  await client.query(`
    ALTER TABLE crm_customer_profile
      ADD COLUMN IF NOT EXISTS followup_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS followup_interval_days INTEGER,
      ADD COLUMN IF NOT EXISTS followup_interval_updated_by INTEGER REFERENCES crm_users(id),
      ADD COLUMN IF NOT EXISTS followup_interval_updated_at TIMESTAMP;

    ALTER TABLE crm_customer_profile
      ALTER COLUMN followup_enabled SET DEFAULT FALSE;

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
  `)
  followupCustomerPolicyReady = true
}

async function ensureCustomerVisitOwnerTable(client = crmDB) {
  if (customerVisitOwnerTableReady && client === crmDB) return
  await client.query(`
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
  `)
  if (client === crmDB) customerVisitOwnerTableReady = true
}

function normalizeFollowupInterval(value) {
  if (value === null || value === undefined || value === '') return null
  const n = Number(value)
  if (!Number.isInteger(n) || n < 1 || n > 365) {
    const err = new Error('รอบโทรรายลูกค้าต้องอยู่ระหว่าง 1-365 วัน')
    err.statusCode = 400
    throw err
  }
  return n
}

module.exports = {
  ensureCustomerFollowupPolicy,
  ensureCustomerVisitOwnerTable,
  normalizeFollowupInterval,
}
