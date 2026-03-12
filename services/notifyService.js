const { crmDB } = require('../db')

/**
 * Insert notification เข้า crm_notifications
 * @param {object} opts
 * @param {number}   opts.userId   — user ที่จะรับ notification
 * @param {string}   opts.notiType — 'assigned'|'task_due'|'task_overdue'|'meeting_remind'|'no_contact'|'activity_update'
 * @param {string}   opts.title
 * @param {string}   [opts.message]
 * @param {string}   [opts.refType]  — 'activity'|'customer'|'note'
 * @param {number}   [opts.refId]
 * @param {string}   [opts.arCode]
 */
async function notify({ userId, notiType, title, message, refType, refId, arCode }) {
  try {
    await crmDB.query(
      `INSERT INTO crm_notifications (user_id, noti_type, title, message, ref_type, ref_id, ar_code)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [userId, notiType, title, message || null, refType || null, refId || null, arCode || null]
    )
  } catch (err) {
    console.error('[notifyService]', err.message)
  }
}

/**
 * notify หลาย user พร้อมกัน
 */
async function notifyMany(users, opts) {
  await Promise.all(users.map(userId => notify({ ...opts, userId })))
}

module.exports = { notify, notifyMany }
