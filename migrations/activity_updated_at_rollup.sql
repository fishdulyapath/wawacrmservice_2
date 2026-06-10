-- Migration: roll up activity updated_at from comments and attachments.
-- Purpose: make the Activities list sortable by the real latest activity update.

CREATE INDEX IF NOT EXISTS idx_crm_activities_updated_at_desc
  ON crm_activities(updated_at DESC, created_at DESC)
  WHERE status != 'deleted';

CREATE INDEX IF NOT EXISTS idx_activity_comments_activity_updated
  ON crm_activity_comments(activity_id, updated_at DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_activity_attachments_activity_created
  ON crm_activity_attachments(activity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_comment_attachments_comment_created
  ON crm_activity_comment_attachments(comment_id, created_at DESC);

-- The crm_activities updated_at trigger always sets NOW() on update.
-- Disable it only inside this protected block so old rows keep the true
-- historical latest timestamp, and ensure it is enabled again on errors.
DO $$
DECLARE
  has_updated_at_trigger boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'crm_activities'::regclass
      AND tgname = 'trg_crm_activities_updated_at'
  ) INTO has_updated_at_trigger;

  IF has_updated_at_trigger THEN
    EXECUTE 'ALTER TABLE crm_activities DISABLE TRIGGER trg_crm_activities_updated_at';
  END IF;

  WITH latest_comment AS (
    SELECT
      c.activity_id,
      MAX(GREATEST(c.created_at, c.updated_at, COALESCE(ca.latest_attachment_at, c.created_at))) AS latest_at
    FROM crm_activity_comments c
    LEFT JOIN (
      SELECT comment_id, MAX(created_at) AS latest_attachment_at
      FROM crm_activity_comment_attachments
      GROUP BY comment_id
    ) ca ON ca.comment_id = c.id
    GROUP BY c.activity_id
  ),
  latest_activity_attachment AS (
    SELECT activity_id, MAX(created_at) AS latest_at
    FROM crm_activity_attachments
    GROUP BY activity_id
  ),
  latest_rollup AS (
    SELECT
      a.id,
      GREATEST(
        a.created_at,
        a.updated_at,
        COALESCE(lc.latest_at, a.created_at),
        COALESCE(la.latest_at, a.created_at)
      ) AS latest_at
    FROM crm_activities a
    LEFT JOIN latest_comment lc ON lc.activity_id = a.id
    LEFT JOIN latest_activity_attachment la ON la.activity_id = a.id
  )
  UPDATE crm_activities a
  SET updated_at = r.latest_at
  FROM latest_rollup r
  WHERE a.id = r.id
    AND a.updated_at IS DISTINCT FROM r.latest_at;

  IF has_updated_at_trigger THEN
    EXECUTE 'ALTER TABLE crm_activities ENABLE TRIGGER trg_crm_activities_updated_at';
  END IF;
EXCEPTION WHEN OTHERS THEN
  IF has_updated_at_trigger THEN
    EXECUTE 'ALTER TABLE crm_activities ENABLE TRIGGER trg_crm_activities_updated_at';
  END IF;
  RAISE;
END $$;
