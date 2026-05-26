const { crmDB } = require('../db')

let followupCustomerPolicyReady = false

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
  normalizeFollowupInterval,
}
