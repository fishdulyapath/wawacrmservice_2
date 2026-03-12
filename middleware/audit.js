const { crmDB } = require('../db')

/**
 * บันทึก Audit Log
 * @param {object} opts
 * @param {string} opts.tableName   - ชื่อตาราง เช่น 'ar_customer'
 * @param {string} opts.recordId    - PK ของ record
 * @param {string} [opts.arCode]    - รหัสลูกค้า
 * @param {string} opts.action      - 'INSERT' | 'UPDATE' | 'DELETE'
 * @param {object} [opts.oldData]   - ข้อมูลเก่า (สำหรับ UPDATE/DELETE)
 * @param {object} [opts.newData]   - ข้อมูลใหม่ (สำหรับ INSERT/UPDATE)
 * @param {object} req              - Express request (สำหรับ user + ip)
 */
async function logAudit(opts, req) {
  try {
    const { tableName, recordId, arCode, action, oldData, newData } = opts

    // หา fields ที่เปลี่ยน (เฉพาะ UPDATE)
    let changedFields = null
    if (action === 'UPDATE' && oldData && newData) {
      changedFields = Object.keys(newData).filter(k => {
        const ov = JSON.stringify(oldData[k])
        const nv = JSON.stringify(newData[k])
        return ov !== nv && k !== 'updated_at'
      })
    }

    const userId   = req?.user?.id   || null
    const userCode = req?.user?.code || null
    const ip       = req?.clientIp
                     || req?.headers?.['x-forwarded-for']?.split(',')[0]?.trim()
                     || req?.socket?.remoteAddress
                     || null

    await crmDB.query(`
      INSERT INTO crm_audit_log
        (table_name, record_id, ar_code, action, old_data, new_data,
         changed_fields, user_id, user_code, ip_address)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `, [
      tableName,
      String(recordId),
      arCode || null,
      action,
      oldData  ? JSON.stringify(oldData)  : null,
      newData  ? JSON.stringify(newData)  : null,
      changedFields?.length ? changedFields : null,
      userId,
      userCode,
      ip
    ])
  } catch (err) {
    // Audit log ไม่ควร block การทำงานหลัก
    console.error('[Audit Log Error]', err.message)
  }
}

module.exports = { logAudit }
