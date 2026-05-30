-- Migration: เพิ่มรหัสกิจกรรม act_no
-- รูปแบบ: C-yyyymmdd-0001 (Call), M-yyyymmdd-0001 (Meeting), W-yyyymmdd-0001 (Task)
-- running number แยกต่อวันและแยกตาม prefix

ALTER TABLE crm_activities
  ADD COLUMN IF NOT EXISTS act_no VARCHAR(20);

CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_activities_act_no
  ON crm_activities(act_no) WHERE act_no IS NOT NULL;

-- backfill ข้อมูลเก่า: ใช้ id เป็น running number เพื่อให้ค่าไม่ซ้ำกัน
UPDATE crm_activities SET act_no =
  CASE activity_type
    WHEN 'call'    THEN 'C-' || TO_CHAR(created_at AT TIME ZONE 'Asia/Bangkok', 'YYYYMMDD') || '-' || LPAD(id::text, 4, '0')
    WHEN 'meeting' THEN 'M-' || TO_CHAR(created_at AT TIME ZONE 'Asia/Bangkok', 'YYYYMMDD') || '-' || LPAD(id::text, 4, '0')
    ELSE                'W-' || TO_CHAR(created_at AT TIME ZONE 'Asia/Bangkok', 'YYYYMMDD') || '-' || LPAD(id::text, 4, '0')
  END
WHERE act_no IS NULL;
