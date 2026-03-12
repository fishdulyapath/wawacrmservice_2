const express = require('express')
const router  = express.Router()
const { crmDB, posDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')
const { logAudit } = require('../middleware/audit')
const { notify, notifyMany } = require('../services/notifyService')

router.use(authMiddleware)

// ── Helper: สิทธิ์สูงกว่า sales_rep ──────────────────────────
const isSA       = u => u.code?.toUpperCase() === 'SUPERADMIN' || ['admin','manager','supervisor'].includes(u.role)
const canCreate  = u => isSA(u)   // supervisor ขึ้นไป
const canViewAll = u => ['admin','manager'].includes(u.role) || u.code?.toUpperCase() === 'SUPERADMIN'

// ── Helper: ดึง owners ของ activity ──────────────────────────
async function getOwners(activityId, client) {
  const db = client || crmDB
  const res = await db.query(`
    SELECT ao.user_id, ao.is_primary, ao.status, ao.assigned_at, ao.removed_at,
           u.name, u.code
    FROM crm_activity_owners ao
    JOIN crm_users u ON u.id = ao.user_id
    WHERE ao.activity_id = $1
    ORDER BY ao.is_primary DESC, ao.assigned_at ASC
  `, [activityId])
  return res.rows
}

// ── Helper: ตรวจสิทธิ์ — เป็น active owner หรือ superadmin ──
async function isActiveOwner(activityId, userId, client) {
  const db = client || crmDB
  const res = await db.query(`
    SELECT 1 FROM crm_activity_owners
    WHERE activity_id = $1 AND user_id = $2 AND removed_at IS NULL
  `, [activityId, userId])
  return res.rows.length > 0
}

// ── Helper: สรุป activity-level status จาก owner statuses ───
// ถ้าทุกคน done → done; มี open อยู่ → open; ทุกคน cancelled → cancelled
function deriveActivityStatus(owners) {
  const active = owners.filter(o => !o.removed_at)
  if (!active.length) return 'open'
  if (active.every(o => o.status === 'done'))      return 'done'
  if (active.every(o => o.status === 'cancelled')) return 'cancelled'
  if (active.some(o => o.status === 'open'))        return 'open'
  return 'open'
}

// ─────────────────────────────────────────────────────────────
// GET /api/activities/stats — KPI สรุปตัวเลข (รองรับ multi-owner)
// ─────────────────────────────────────────────────────────────
router.get('/stats', async (req, res) => {
  try {
    const isMine = !canViewAll(req.user)
    const params = isMine ? [req.user.id] : []
    // openCond: นับเฉพาะ activity ที่ยังเปิดอยู่ (มี owner status=open)
    const openSub  = isMine
      ? `EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.user_id=$1 AND ax.removed_at IS NULL AND ax.status NOT IN ('done','cancelled'))`
      : `EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status NOT IN ('done','cancelled'))`

    const r = await crmDB.query(`
      SELECT
        COUNT(*) FILTER (WHERE (
          (a.activity_type = 'task' AND a.due_date < CURRENT_DATE)
          OR (a.activity_type IN ('call','meeting') AND a.start_datetime < NOW())
        ) AND ${openSub}) AS overdue,
        COUNT(*) FILTER (WHERE (
          (a.activity_type = 'task' AND DATE(a.due_date) = CURRENT_DATE)
          OR (a.activity_type IN ('call','meeting') AND DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok') = CURRENT_DATE AT TIME ZONE 'Asia/Bangkok')
        ) AND ${openSub}) AS today,
        COUNT(*) FILTER (WHERE ${openSub}) AS open,
        COUNT(*) FILTER (WHERE a.activity_type = 'meeting' AND DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok') = CURRENT_DATE AT TIME ZONE 'Asia/Bangkok' AND ${openSub}) AS meetings_today
      FROM crm_activities a
    `, params)
    res.json(r.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/activities
// ─────────────────────────────────────────────────────────────
router.get('/', async (req, res) => {
  try {
    const { type, status, ar_code, owner_id, due, search, page = 1, limit = 20 } = req.query
    const offset = (parseInt(page) - 1) * parseInt(limit)
    const params = []
    const conditions = []

    if (type)    { params.push(type);    conditions.push(`a.activity_type = $${params.length}`) }
    if (ar_code) { params.push(ar_code); conditions.push(`a.ar_code = $${params.length}`) }

    // filter by owner — ใช้ owners table
    const filterUser = owner_id ? parseInt(owner_id) : (!canViewAll(req.user) ? req.user.id : null)
    if (filterUser) {
      params.push(filterUser)
      conditions.push(`EXISTS (
        SELECT 1 FROM crm_activity_owners ao
        WHERE ao.activity_id = a.id AND ao.user_id = $${params.length} AND ao.removed_at IS NULL
      )`)
    }

    if (due === 'overdue') {
      conditions.push(`(
        (a.activity_type = 'task' AND a.due_date < CURRENT_DATE)
        OR (a.activity_type IN ('call','meeting') AND a.start_datetime < NOW())
      )`)
      conditions.push(`EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status NOT IN ('done','cancelled'))`)
    } else if (due === 'today') {
      conditions.push(`(
        (a.activity_type = 'task' AND DATE(a.due_date) = CURRENT_DATE)
        OR (a.activity_type IN ('call','meeting') AND DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok') = (CURRENT_DATE AT TIME ZONE 'Asia/Bangkok'))
      )`)
      conditions.push(`EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status NOT IN ('done','cancelled'))`)
    } else if (due === 'week') {
      conditions.push(`(
        (a.activity_type = 'task' AND a.due_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days')
        OR (a.activity_type IN ('call','meeting') AND DATE(a.start_datetime AT TIME ZONE 'Asia/Bangkok') BETWEEN (CURRENT_DATE AT TIME ZONE 'Asia/Bangkok') AND (CURRENT_DATE AT TIME ZONE 'Asia/Bangkok') + INTERVAL '7 days')
      )`)
      conditions.push(`EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status NOT IN ('done','cancelled'))`)
    } else if (status) {
      // กรองตาม my_status ของ user ที่ login (ถ้าเป็น sales_rep หรือส่ง owner_id มา)
      // admin/manager ที่ไม่ได้กรอง owner → ใช้ derived status
      const statusUser = filterUser || (!canViewAll(req.user) ? req.user.id : null)
      if (statusUser) {
        params.push(statusUser); params.push(status)
        conditions.push(`EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.user_id=$${params.length-1} AND ax.removed_at IS NULL AND ax.status=$${params.length})`)
      } else if (status === 'open') {
        conditions.push(`EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status='open')`)
      } else {
        params.push(status)
        conditions.push(`NOT EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status='open')
          AND EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status=$${params.length})`)
      }
    }

    if (search) {
      params.push(`%${search}%`)
      conditions.push(`(a.subject ILIKE $${params.length} OR a.ar_code ILIKE $${params.length})`)
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : ''

    const countResult = await crmDB.query(`SELECT COUNT(*) FROM crm_activities a ${where}`, params)
    const total = parseInt(countResult.rows[0].count)

    params.push(parseInt(limit), offset)
    const dataResult = await crmDB.query(`
      SELECT
        a.*,
        -- primary owner
        (SELECT u.name FROM crm_activity_owners ao JOIN crm_users u ON u.id = ao.user_id
         WHERE ao.activity_id = a.id AND ao.is_primary = TRUE AND ao.removed_at IS NULL LIMIT 1) AS owner_name,
        -- all active owners as JSON
        (SELECT json_agg(json_build_object(
           'user_id', ao.user_id, 'name', u.name, 'code', u.code,
           'is_primary', ao.is_primary, 'status', ao.status
         ) ORDER BY ao.is_primary DESC, ao.assigned_at ASC)
         FROM crm_activity_owners ao JOIN crm_users u ON u.id = ao.user_id
         WHERE ao.activity_id = a.id AND ao.removed_at IS NULL) AS owners,
        -- derived status (open หากมีคนใดยัง open)
        CASE
          WHEN EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status='open') THEN 'open'
          WHEN EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status='done') THEN 'done'
          ELSE 'cancelled'
        END AS derived_status,
        -- status ของ current user
        (SELECT ao.status FROM crm_activity_owners ao WHERE ao.activity_id=a.id AND ao.user_id=${req.user.id} AND ao.removed_at IS NULL LIMIT 1) AS my_status
      FROM crm_activities a
      ${where}
      ORDER BY
        CASE WHEN EXISTS (SELECT 1 FROM crm_activity_owners ax WHERE ax.activity_id=a.id AND ax.removed_at IS NULL AND ax.status='open') THEN 0 ELSE 1 END,
        a.due_date ASC NULLS LAST,
        a.start_datetime ASC NULLS LAST,
        a.created_at DESC
      LIMIT $${params.length - 1} OFFSET $${params.length}
    `, params)

    // ดึง customer names จาก POS
    const arCodes = [...new Set(dataResult.rows.map(r => r.ar_code).filter(Boolean))]
    let customerMap = {}
    if (arCodes.length > 0) {
      const placeholders = arCodes.map((_, i) => `$${i + 1}`).join(',')
      const posResult = await posDB.query(
        `SELECT code, name_1 FROM ar_customer WHERE code IN (${placeholders})`, arCodes
      )
      posResult.rows.forEach(r => { customerMap[r.code] = r.name_1 })
    }

    const pageInt  = parseInt(page)
    const limitInt = parseInt(limit)
    res.json({
      data: dataResult.rows.map(r => ({
        ...r,
        status: r.my_status || r.derived_status || 'open',
        customer_name: customerMap[r.ar_code] || null,
      })),
      pagination: {
        total,
        page:  pageInt,
        limit: limitInt,
        pages: Math.ceil(total / limitInt),
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// GET /api/activities/:id
// ─────────────────────────────────────────────────────────────
router.get('/:id', async (req, res) => {
  try {
    const result = await crmDB.query(
      `SELECT a.* FROM crm_activities a WHERE a.id = $1`, [req.params.id]
    )
    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })
    const activity = result.rows[0]

    // ดึง owners ทั้งหมด
    const owners = await getOwners(activity.id)
    activity.owners = owners.filter(o => !o.removed_at)
    activity.removed_owners = owners.filter(o => o.removed_at)

    // ตรวจสิทธิ์ — sales_rep ต้องเป็น active owner
    if (!canViewAll(req.user)) {
      const ok = activity.owners.some(o => o.user_id === req.user.id)
      if (!ok) return res.status(403).json({ error: 'ไม่มีสิทธิ์เข้าถึง Activity นี้' })
    }

    // derived status
    activity.derived_status = deriveActivityStatus(owners)
    activity.my_status = activity.owners.find(o => o.user_id === req.user.id)?.status || null

    // ดึง customer_name จาก POS
    if (activity.ar_code) {
      try {
        const cusRes = await posDB.query(
          `SELECT name_1 FROM ar_customer WHERE code = $1`, [activity.ar_code]
        )
        activity.customer_name = cusRes.rows[0]?.name_1 || null
      } catch {}
    }

    // external invitees (ลูกค้า/contact)
    if (activity.activity_type === 'meeting') {
      const extRes = await crmDB.query(
        `SELECT ar_code, contact_name, response_status
         FROM crm_activity_invitees
         WHERE activity_id = $1 AND is_external = TRUE`, [activity.id]
      )
      activity.external_invitees = extRes.rows
    }

    // group members (ถ้า activity นี้เป็นส่วนหนึ่งของกลุ่ม)
    if (activity.group_id) {
      const groupRes = await crmDB.query(
        `SELECT id, ar_code FROM crm_activities WHERE group_id=$1 ORDER BY created_at ASC`,
        [activity.group_id]
      )
      if (groupRes.rows.length > 1) {
        // ดึง customer names จาก POS
        const arCodes = groupRes.rows.map(r => r.ar_code).filter(Boolean)
        let customerMap = {}
        if (arCodes.length > 0) {
          const placeholders = arCodes.map((_, i) => `$${i + 1}`).join(',')
          try {
            const posRes = await posDB.query(
              `SELECT code, name_1 FROM ar_customer WHERE code IN (${placeholders})`, arCodes
            )
            posRes.rows.forEach(r => { customerMap[r.code] = r.name_1 })
          } catch {}
        }
        activity.group_members = groupRes.rows.map(r => ({
          id: r.id,
          ar_code: r.ar_code,
          customer_name: customerMap[r.ar_code] || null,
        }))
      }
    }

    // override status ด้วย derived_status (crm_activities.status ไม่ได้ถูก update เมื่อปิดงาน)
    activity.status = activity.my_status || activity.derived_status || 'open'

    res.json(activity)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// POST /api/activities
// body: { owners: [userId,...], ...fields }
// ─────────────────────────────────────────────────────────────
router.post('/', async (req, res) => {
  if (!canCreate(req.user)) {
    return res.status(403).json({ error: 'ไม่มีสิทธิ์สร้างกิจกรรม' })
  }

  const {
    ar_code, activity_type, subject, description, status = 'open', priority = 'normal',
    due_date, start_datetime, end_datetime, location,
    call_direction, call_result, call_phone, duration_sec,
    owners = [], primary_owner_id,
    meeting_url, outcome, all_day = false,
    external_invitees = [],
    group_id,
    // backward compat: รับ owner_id scalar ด้วย
    owner_id,
  } = req.body

  if (!activity_type || !subject) {
    return res.status(400).json({ error: 'กรุณากรอก activity_type และ subject' })
  }

  // resolve owners list
  let ownerList = owners.length ? owners.map(Number) : (owner_id ? [Number(owner_id)] : [])
  if (!ownerList.length) ownerList = [req.user.id]
  // deduplicate
  ownerList = [...new Set(ownerList)]

  // primary owner
  const primaryId = primary_owner_id
    ? Number(primary_owner_id)
    : (ownerList.includes(req.user.id) ? req.user.id : ownerList[0])

  const client = await crmDB.connect()
  try {
    await client.query('BEGIN')

    const result = await client.query(`
      INSERT INTO crm_activities
        (ar_code, owner_id, created_by, activity_type, subject, description, status, priority,
         due_date, start_datetime, end_datetime, location,
         call_direction, call_result, call_phone, duration_sec,
         meeting_url, outcome, all_day, group_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      RETURNING *`,
      [ar_code || null, primaryId, req.user.id,
       activity_type, subject, description, status, priority,
       due_date || null, start_datetime || null, end_datetime || null, location || null,
       call_direction || null, call_result || null, call_phone || null, duration_sec || null,
       meeting_url || null, outcome || null, all_day, group_id || null]
    )
    const activity = result.rows[0]

    // insert owners
    for (const uid of ownerList) {
      await client.query(`
        INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
        VALUES ($1,$2,$3,'open',$4)
        ON CONFLICT (activity_id, user_id) DO NOTHING
      `, [activity.id, uid, uid === primaryId, req.user.id])
    }

    // external invitees (ลูกค้า/contact)
    for (const inv of external_invitees) {
      if (!inv.ar_code && !inv.contact_name) continue
      await client.query(`
        INSERT INTO crm_activity_invitees (activity_id, ar_code, contact_name, is_external)
        VALUES ($1,$2,$3,TRUE)
      `, [activity.id, inv.ar_code || null, inv.contact_name || null])
    }

    await client.query('COMMIT')

    await logAudit({
      tableName: 'crm_activities', recordId: activity.id, arCode: ar_code,
      action: 'INSERT', newData: { ...activity, owners: ownerList },
    }, req)

    // notify owners
    const othersToNotify = ownerList.filter(uid => uid !== req.user.id)
    if (othersToNotify.length) {
      await notifyMany(othersToNotify, {
        notiType: 'assigned',
        title: 'งานใหม่ถูก assign ให้คุณ',
        message: subject,
        refType: 'activity',
        refId: activity.id,
        arCode: ar_code || null,
      })
    } else {
      await notify({
        userId: req.user.id,
        notiType: 'assigned',
        title: 'งานใหม่ของคุณ',
        message: subject,
        refType: 'activity',
        refId: activity.id,
        arCode: ar_code || null,
      })
    }

    // notify meeting invitees
    if (activity_type === 'meeting' && othersToNotify.length) {
      const meetingDate = start_datetime
        ? new Date(start_datetime).toLocaleDateString('th-TH', { day: '2-digit', month: 'short' })
        : ''
      await notifyMany(othersToNotify, {
        notiType: 'assigned',
        title: 'คุณถูกเชิญเข้า Meeting',
        message: `${subject}${meetingDate ? ' — ' + meetingDate : ''}`,
        refType: 'activity',
        refId: activity.id,
        arCode: ar_code || null,
      })
    }

    const allOwners = await getOwners(activity.id)
    res.status(201).json({ ...activity, owners: allOwners })
  } catch (err) {
    await client.query('ROLLBACK')
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

// ─────────────────────────────────────────────────────────────
// PUT /api/activities/:id  — แก้ไข shared fields + manage owners
// ─────────────────────────────────────────────────────────────
router.put('/:id', async (req, res) => {
  if (!canCreate(req.user)) {
    return res.status(403).json({ error: 'ไม่มีสิทธิ์แก้ไขกิจกรรม' })
  }

  const {
    subject, description, priority, ar_code,
    due_date, start_datetime, end_datetime, location,
    call_direction, call_result, call_phone, duration_sec,
    meeting_url, outcome, all_day,
    owners,          // array of user IDs (active owners list ใหม่)
    primary_owner_id,
    external_invitees,
    group_update,    // { add_ar_codes, remove_activity_ids, owners, primary_owner_id }
    // backward compat
    owner_id, invitees,
  } = req.body

  const client = await crmDB.connect()
  try {
    const existing = await client.query('SELECT * FROM crm_activities WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })
    const old = existing.rows[0]

    await client.query('BEGIN')

    // ── update shared fields ──
    const result = await client.query(`
      UPDATE crm_activities SET
        subject=$1, description=$2, priority=$3,
        due_date=$4, start_datetime=$5, end_datetime=$6, location=$7,
        call_direction=$8, call_result=$9, call_phone=$10, duration_sec=$11,
        meeting_url=$12, outcome=$13, all_day=$14, ar_code=$15,
        updated_at=NOW()
      WHERE id=$16 RETURNING *`,
      [
        subject        ?? old.subject,
        description    ?? old.description,
        priority       ?? old.priority,
        due_date       !== undefined ? (due_date || null)       : old.due_date,
        start_datetime !== undefined ? (start_datetime || null) : old.start_datetime,
        end_datetime   !== undefined ? (end_datetime || null)   : old.end_datetime,
        location       !== undefined ? (location || null)       : old.location,
        call_direction !== undefined ? (call_direction || null) : old.call_direction,
        call_result    !== undefined ? (call_result || null)    : old.call_result,
        call_phone     !== undefined ? (call_phone || null)     : old.call_phone,
        duration_sec   !== undefined ? (duration_sec || null)   : old.duration_sec,
        meeting_url    !== undefined ? (meeting_url || null)    : old.meeting_url,
        outcome        !== undefined ? (outcome || null)        : old.outcome,
        all_day        !== undefined ? all_day                  : old.all_day,
        ar_code        !== undefined ? (ar_code || null)        : old.ar_code,
        req.params.id,
      ]
    )
    const updated = result.rows[0]

    // ── manage owners ──
    // normalize: ถ้าส่ง invitees (backward compat) ให้แปลงเป็น owners
    let newOwnerList = owners
    if (!newOwnerList && invitees)  newOwnerList = invitees
    if (!newOwnerList && owner_id)  newOwnerList = [Number(owner_id)]

    let addedOwners = [], removedOwners = []
    if (Array.isArray(newOwnerList)) {
      newOwnerList = [...new Set(newOwnerList.map(Number))]
      const newPrimaryId = primary_owner_id ? Number(primary_owner_id) : null

      const curRes = await client.query(
        `SELECT user_id, is_primary FROM crm_activity_owners WHERE activity_id=$1 AND removed_at IS NULL`,
        [req.params.id]
      )
      const curOwnerIds = curRes.rows.map(r => r.user_id)

      addedOwners   = newOwnerList.filter(uid => !curOwnerIds.includes(uid))
      removedOwners = curOwnerIds.filter(uid => !newOwnerList.includes(uid))

      // INSERT new owners
      for (const uid of addedOwners) {
        const isPrimary = newPrimaryId ? uid === newPrimaryId : false
        await client.query(`
          INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
          VALUES ($1,$2,$3,'open',$4)
          ON CONFLICT (activity_id, user_id) DO UPDATE
          SET removed_at = NULL, removed_by = NULL, assigned_by = $4, status = 'open'
        `, [req.params.id, uid, isPrimary, req.user.id])
      }

      // soft-delete removed owners
      for (const uid of removedOwners) {
        await client.query(`
          UPDATE crm_activity_owners
          SET removed_at = NOW(), removed_by = $3
          WHERE activity_id = $1 AND user_id = $2
        `, [req.params.id, uid, req.user.id])
      }

      // update primary flag
      if (newPrimaryId) {
        await client.query(`
          UPDATE crm_activity_owners SET is_primary = (user_id = $2)
          WHERE activity_id = $1 AND removed_at IS NULL
        `, [req.params.id, newPrimaryId])
      }

      // update owner_id column (compat)
      const primaryRow = newPrimaryId || newOwnerList[0]
      if (primaryRow) {
        await client.query(`UPDATE crm_activities SET owner_id=$2 WHERE id=$1`, [req.params.id, primaryRow])
      }
    }

    // external invitees
    if (Array.isArray(external_invitees)) {
      await client.query(`DELETE FROM crm_activity_invitees WHERE activity_id=$1 AND is_external=TRUE`, [req.params.id])
      for (const inv of external_invitees) {
        if (!inv.ar_code && !inv.contact_name) continue
        await client.query(`
          INSERT INTO crm_activity_invitees (activity_id, ar_code, contact_name, is_external)
          VALUES ($1,$2,$3,TRUE)
        `, [req.params.id, inv.ar_code || null, inv.contact_name || null])
      }
    }

    // ── group update ──
    if (group_update && old.group_id) {
      const {
        add_ar_codes = [],
        remove_activity_ids = [],
        owners: grpOwners,
        primary_owner_id: grpPrimary,
      } = group_update

      // 1. อัปเดต shared fields ทุก activity ใน group (ยกเว้น ar_code)
      await client.query(`
        UPDATE crm_activities SET
          subject=$1, description=$2, priority=$3,
          due_date=$4, start_datetime=$5, end_datetime=$6,
          location=$7, meeting_url=$8, updated_at=NOW()
        WHERE group_id=$9 AND id != $10
      `, [
        subject        ?? old.subject,
        description    ?? old.description,
        priority       ?? old.priority,
        due_date       !== undefined ? (due_date || null)       : old.due_date,
        start_datetime !== undefined ? (start_datetime || null) : old.start_datetime,
        end_datetime   !== undefined ? (end_datetime || null)   : old.end_datetime,
        location       !== undefined ? (location || null)       : old.location,
        meeting_url    !== undefined ? (meeting_url || null)    : old.meeting_url,
        old.group_id, req.params.id,
      ])

      // 2. ลบ activities ที่ถูกเลือกออกจากกลุ่ม
      if (remove_activity_ids.length) {
        const safeIds = remove_activity_ids.map(Number).filter(id => id !== parseInt(req.params.id))
        if (safeIds.length) {
          await client.query(`DELETE FROM crm_activity_owners WHERE activity_id = ANY($1)`, [safeIds])
          await client.query(`DELETE FROM crm_activity_invitees WHERE activity_id = ANY($1)`, [safeIds])
          await client.query(`DELETE FROM crm_activities WHERE id = ANY($1) AND group_id=$2`, [safeIds, old.group_id])
        }
      }

      // 3. เพิ่มลูกค้าใหม่เข้ากลุ่ม
      const resolvedPrimary = grpPrimary ? Number(grpPrimary) : (updated.owner_id || req.user.id)
      const resolvedOwners  = Array.isArray(grpOwners)
        ? [...new Set(grpOwners.map(Number))]
        : (Array.isArray(owners) ? [...new Set(owners.map(Number))] : [resolvedPrimary])

      for (const newArCode of add_ar_codes) {
        if (!newArCode) continue
        const newAct = await client.query(`
          INSERT INTO crm_activities
            (ar_code, owner_id, created_by, activity_type, subject, description, status, priority,
             due_date, start_datetime, end_datetime, location, meeting_url, outcome, all_day, group_id)
          VALUES ($1,$2,$3,$4,$5,$6,'open',$7,$8,$9,$10,$11,$12,$13,$14,$15)
          RETURNING *
        `, [
          newArCode, resolvedPrimary, req.user.id,
          updated.activity_type, updated.subject, updated.description, updated.priority,
          updated.due_date || null, updated.start_datetime || null, updated.end_datetime || null,
          updated.location || null, updated.meeting_url || null, updated.outcome || null,
          updated.all_day, old.group_id,
        ])
        const newActId = newAct.rows[0].id
        for (const uid of resolvedOwners) {
          await client.query(`
            INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
            VALUES ($1,$2,$3,'open',$4) ON CONFLICT DO NOTHING
          `, [newActId, uid, uid === resolvedPrimary, req.user.id])
        }
      }

      // 4. อัปเดต owners ทุก activity ในกลุ่ม (ถ้าระบุมา)
      if (Array.isArray(grpOwners)) {
        const grpOwnerList = [...new Set(grpOwners.map(Number))]
        const grpPrimaryId = grpPrimary ? Number(grpPrimary) : grpOwnerList[0]

        const allInGroup = await client.query(
          `SELECT id FROM crm_activities WHERE group_id=$1 AND id != $2`,
          [old.group_id, req.params.id]
        )
        for (const row of allInGroup.rows) {
          const actId = row.id
          const curOwners = await client.query(
            `SELECT user_id FROM crm_activity_owners WHERE activity_id=$1 AND removed_at IS NULL`, [actId]
          )
          const curIds = curOwners.rows.map(r => r.user_id)
          const toAdd    = grpOwnerList.filter(uid => !curIds.includes(uid))
          const toRemove = curIds.filter(uid => !grpOwnerList.includes(uid))

          for (const uid of toAdd) {
            await client.query(`
              INSERT INTO crm_activity_owners (activity_id, user_id, is_primary, status, assigned_by)
              VALUES ($1,$2,$3,'open',$4)
              ON CONFLICT (activity_id, user_id) DO UPDATE
              SET removed_at=NULL, removed_by=NULL, assigned_by=$4, status='open'
            `, [actId, uid, uid === grpPrimaryId, req.user.id])
          }
          for (const uid of toRemove) {
            await client.query(
              `UPDATE crm_activity_owners SET removed_at=NOW(), removed_by=$3 WHERE activity_id=$1 AND user_id=$2`,
              [actId, uid, req.user.id]
            )
          }
          if (grpPrimaryId) {
            await client.query(
              `UPDATE crm_activity_owners SET is_primary=(user_id=$2) WHERE activity_id=$1 AND removed_at IS NULL`,
              [actId, grpPrimaryId]
            )
            await client.query(`UPDATE crm_activities SET owner_id=$2 WHERE id=$1`, [actId, grpPrimaryId])
          }
        }
      }
    }

    await client.query('COMMIT')

    await logAudit({
      tableName: 'crm_activities', recordId: updated.id, arCode: updated.ar_code,
      action: 'UPDATE', oldData: old, newData: updated,
    }, req)

    // notify: แจ้ง owners ทั้งหมดที่ยังอยู่ว่ามีการแก้ไข (ยกเว้นผู้แก้ไข)
    const allOwners = await getOwners(updated.id)
    const activeOwners = allOwners.filter(o => !o.removed_at && o.user_id !== req.user.id)
    if (activeOwners.length) {
      await notifyMany(activeOwners.map(o => o.user_id), {
        notiType: 'updated',
        title: 'กิจกรรมถูกแก้ไข',
        message: updated.subject,
        refType: 'activity',
        refId: updated.id,
        arCode: updated.ar_code || null,
      })
    }

    // notify new owners
    if (addedOwners.length) {
      const meetingDate = updated.start_datetime
        ? new Date(updated.start_datetime).toLocaleDateString('th-TH', { day: '2-digit', month: 'short' })
        : ''
      const newNotify = addedOwners.filter(uid => uid !== req.user.id)
      if (newNotify.length) {
        await notifyMany(newNotify, {
          notiType: 'assigned',
          title: updated.activity_type === 'meeting' ? 'คุณถูกเชิญเข้า Meeting' : 'งานใหม่ถูก assign ให้คุณ',
          message: `${updated.subject}${meetingDate ? ' — ' + meetingDate : ''}`,
          refType: 'activity',
          refId: updated.id,
          arCode: updated.ar_code || null,
        })
      }
    }

    // notify removed owners
    if (removedOwners.length) {
      await notifyMany(removedOwners, {
        notiType: 'info',
        title: 'คุณถูกนำออกจากกิจกรรม',
        message: updated.subject,
        refType: 'activity',
        refId: updated.id,
        arCode: updated.ar_code || null,
      })
    }

    // ถ้าเป็น group update ให้ส่ง IDs ทุก activity ในกลุ่มกลับมาด้วย
    let groupMemberIds = null
    if (group_update && updated.group_id) {
      const grpRes = await crmDB.query(
        `SELECT id FROM crm_activities WHERE group_id=$1`, [updated.group_id]
      )
      groupMemberIds = grpRes.rows.map(r => r.id)
    }

    res.json({
      ...updated,
      owners: allOwners.filter(o => !o.removed_at),
      ...(groupMemberIds ? { group_member_ids: groupMemberIds } : {}),
    })
  } catch (err) {
    await client.query('ROLLBACK')
    res.status(500).json({ error: err.message })
  } finally {
    client.release()
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/activities/:id/status — เปลี่ยน status ของตัวเอง
// body: { status: 'open'|'done'|'cancelled' }
// ─────────────────────────────────────────────────────────────
router.patch('/:id/status', async (req, res) => {
  try {
    const { status } = req.body
    if (!['open','done','cancelled'].includes(status)) {
      return res.status(400).json({ error: 'status ไม่ถูกต้อง' })
    }

    // ตรวจสิทธิ์ — ต้องเป็น active owner
    const ok = await isActiveOwner(req.params.id, req.user.id)
    if (!ok && !canCreate(req.user)) {
      return res.status(403).json({ error: 'ไม่มีสิทธิ์' })
    }

    await crmDB.query(`
      UPDATE crm_activity_owners SET status = $3
      WHERE activity_id = $1 AND user_id = $2 AND removed_at IS NULL
    `, [req.params.id, req.user.id, status])

    res.json({ success: true, status })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/activities/:id/snooze — เลื่อนวันที่
// ─────────────────────────────────────────────────────────────
router.patch('/:id/snooze', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_activities WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })
    const old = existing.rows[0]

    if (!canCreate(req.user)) {
      const ok = await isActiveOwner(req.params.id, req.user.id)
      if (!ok) return res.status(403).json({ error: 'ไม่มีสิทธิ์' })
    }

    const { days, date } = req.body
    let result
    if (date) {
      if (old.activity_type === 'meeting' && old.start_datetime) {
        result = await crmDB.query(
          `UPDATE crm_activities SET start_datetime=$2::timestamptz, updated_at=NOW() WHERE id=$1 RETURNING *`,
          [req.params.id, date]
        )
      } else {
        result = await crmDB.query(
          `UPDATE crm_activities SET due_date=$2::date, updated_at=NOW() WHERE id=$1 RETURNING *`,
          [req.params.id, date]
        )
      }
    } else {
      const d = parseInt(days) || 1
      if (old.activity_type === 'meeting' && old.start_datetime) {
        result = await crmDB.query(
          `UPDATE crm_activities SET start_datetime=start_datetime + ($2 || ' days')::INTERVAL, updated_at=NOW() WHERE id=$1 RETURNING *`,
          [req.params.id, String(d)]
        )
      } else {
        result = await crmDB.query(
          `UPDATE crm_activities SET due_date=COALESCE(due_date,CURRENT_DATE) + ($2 || ' days')::INTERVAL, updated_at=NOW() WHERE id=$1 RETURNING *`,
          [req.params.id, String(d)]
        )
      }
    }
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// PATCH /api/activities/:id/done — ปิดงาน (per-user status)
// ─────────────────────────────────────────────────────────────
router.patch('/:id/done', async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT id FROM crm_activities WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })

    if (!canCreate(req.user)) {
      const ok = await isActiveOwner(req.params.id, req.user.id)
      if (!ok) return res.status(403).json({ error: 'ไม่มีสิทธิ์' })
    }

    const { outcome, call_phone, call_result, call_direction, duration_sec,
            cdr_uuid, cdr_recording_url, cdr_start_stamp, cdr_end_stamp } = req.body

    // update shared outcome/call fields บน activity row
    const result = await crmDB.query(`
      UPDATE crm_activities SET
        outcome           = COALESCE($2, outcome),
        call_phone        = COALESCE($3, call_phone),
        call_result       = COALESCE($4, call_result),
        call_direction    = COALESCE($5, call_direction),
        duration_sec      = COALESCE($6::int, duration_sec),
        cdr_uuid          = COALESCE($7, cdr_uuid),
        cdr_recording_url = COALESCE($8, cdr_recording_url),
        cdr_start_stamp   = COALESCE($9::timestamptz, cdr_start_stamp),
        cdr_end_stamp     = COALESCE($10::timestamptz, cdr_end_stamp),
        updated_at        = NOW()
      WHERE id=$1 RETURNING *`,
      [req.params.id,
       outcome||null, call_phone||null, call_result||null, call_direction||null, duration_sec||null,
       cdr_uuid||null, cdr_recording_url||null, cdr_start_stamp||null, cdr_end_stamp||null]
    )

    // update status: meeting หรือ admin/manager → ปิดทุก owner; call/task ของ sales_rep → เฉพาะ user นี้
    const actRow = result.rows[0]
    const closeAll = actRow.activity_type === 'meeting' || canCreate(req.user)
    if (closeAll) {
      await crmDB.query(
        `UPDATE crm_activity_owners SET status = 'done' WHERE activity_id = $1 AND removed_at IS NULL`,
        [req.params.id]
      )
    } else {
      const updated = await crmDB.query(
        `UPDATE crm_activity_owners SET status = 'done' WHERE activity_id = $1 AND user_id = $2 AND removed_at IS NULL`,
        [req.params.id, req.user.id]
      )
      // ถ้า user นี้ไม่มีใน owners (เช่น manager ปิดงาน) → ปิดทั้งหมด
      if (updated.rowCount === 0) {
        await crmDB.query(
          `UPDATE crm_activity_owners SET status = 'done' WHERE activity_id = $1 AND removed_at IS NULL`,
          [req.params.id]
        )
      }
    }

    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ─────────────────────────────────────────────────────────────
// DELETE /api/activities/:id
// ─────────────────────────────────────────────────────────────
router.delete('/:id', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const existing = await crmDB.query('SELECT * FROM crm_activities WHERE id=$1', [req.params.id])
    if (!existing.rows.length) return res.status(404).json({ error: 'ไม่พบ Activity' })
    const old = existing.rows[0]

    // แจ้ง owners ก่อนลบ
    const owners = await getOwners(old.id)
    const toNotify = owners.filter(o => !o.removed_at && o.user_id !== req.user.id).map(o => o.user_id)
    if (toNotify.length) {
      await notifyMany(toNotify, {
        notiType: 'info',
        title: 'กิจกรรมถูกลบ',
        message: old.subject,
        refType: 'activity',
        refId: old.id,
        arCode: old.ar_code || null,
      })
    }

    await crmDB.query('DELETE FROM crm_activity_owners WHERE activity_id=$1', [req.params.id])
    await crmDB.query('DELETE FROM crm_activity_invitees WHERE activity_id=$1', [req.params.id])
    await crmDB.query('DELETE FROM crm_activities WHERE id=$1', [req.params.id])

    await logAudit({
      tableName: 'crm_activities', recordId: old.id, arCode: old.ar_code,
      action: 'DELETE', oldData: old,
    }, req)

    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

module.exports = router
