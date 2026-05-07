ALTER TABLE crm_users
  ADD COLUMN IF NOT EXISTS overdue_notify_time TIME DEFAULT '08:00',
  ADD COLUMN IF NOT EXISTS due_tomorrow_notify_time TIME DEFAULT '17:00';

UPDATE crm_users
SET overdue_notify_time = COALESCE(overdue_notify_time, TIME '08:00'),
    due_tomorrow_notify_time = COALESCE(due_tomorrow_notify_time, TIME '17:00');
