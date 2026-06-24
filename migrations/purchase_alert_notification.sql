-- LINE purchase reorder point alert settings
-- Run on CRM database.

ALTER TABLE IF EXISTS crm_users
  ADD COLUMN IF NOT EXISTS purchase_alert_notify_enabled BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS purchase_alert_notify_time TIME DEFAULT '08:00';

UPDATE crm_users
SET purchase_alert_notify_enabled = COALESCE(purchase_alert_notify_enabled, FALSE),
    purchase_alert_notify_time = COALESCE(purchase_alert_notify_time, TIME '08:00');

