-- Remove configurable time for purchase alert — fixed at 08:00 daily.
-- Add link_url column to crm_notifications for clickable navigation.
-- Run on CRM database.

ALTER TABLE crm_users
  DROP COLUMN IF EXISTS purchase_alert_notify_time;

ALTER TABLE crm_notifications
  ADD COLUMN IF NOT EXISTS link_url varchar(500);
