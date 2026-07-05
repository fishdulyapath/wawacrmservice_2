const { crmDB } = require('../db')

const USER_COLORS = ['#dc2626', '#16a34a', '#2563eb', '#ca8a04', '#9333ea', '#0891b2', '#ea580c', '#4f46e5']
let purchasePlanningUserSettingsReady = false

function clean(value) {
  return String(value || '').trim()
}

function isPlanningAdmin(user) {
  return clean(user?.code).toUpperCase() === 'SUPERADMIN' || user?.role === 'admin'
}

function defaultPlanningAccess(user) {
  return isPlanningAdmin(user) ? 1 : 0
}

function defaultPlanningColor(userId) {
  const n = Number(userId || 0)
  return USER_COLORS[Math.abs(n) % USER_COLORS.length]
}

function validPlanningColor(value, fallback = '#2563eb') {
  const color = clean(value)
  return /^#[0-9A-Fa-f]{6}$/.test(color) ? color : fallback
}

async function ensurePurchasePlanningUserSettings() {
  if (purchasePlanningUserSettingsReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS public.crm_purchase_planning_user_setting (
      user_id integer PRIMARY KEY REFERENCES public.crm_users(id) ON DELETE CASCADE,
      can_access smallint NOT NULL DEFAULT 0,
      cart_color varchar(20) NOT NULL DEFAULT '#2563eb',
      remark varchar(255) NOT NULL DEFAULT '',
      create_datetime timestamp without time zone NOT NULL DEFAULT now(),
      last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
      create_code varchar(50) NOT NULL DEFAULT '',
      last_update_code varchar(50) NOT NULL DEFAULT '',
      CONSTRAINT crm_purchase_planning_user_setting_access_chk CHECK (can_access IN (0, 1)),
      CONSTRAINT crm_purchase_planning_user_setting_color_chk CHECK (cart_color ~ '^#[0-9A-Fa-f]{6}$')
    );
  `)
  purchasePlanningUserSettingsReady = true
}

async function ensurePurchasePlanningUserSettingForUser(user, client = crmDB) {
  await ensurePurchasePlanningUserSettings()
  const userId = Number(user?.id || user?.user_id || 0)
  if (!userId) return null
  const fallbackAccess = defaultPlanningAccess(user)
  const fallbackColor = defaultPlanningColor(userId)
  await client.query(
    `INSERT INTO public.crm_purchase_planning_user_setting
       (user_id, can_access, cart_color, create_code, last_update_code)
     VALUES ($1, $2, $3, $4, $4)
     ON CONFLICT (user_id) DO NOTHING`,
    [userId, fallbackAccess, fallbackColor, clean(user?.code)],
  )
  const result = await client.query(
    `SELECT can_access::int AS can_access, cart_color, remark
     FROM public.crm_purchase_planning_user_setting
     WHERE user_id = $1::int`,
    [userId],
  )
  const row = result.rows[0] || {}
  return {
    can_access: isPlanningAdmin(user) ? 1 : Number(row.can_access || 0),
    cart_color: validPlanningColor(row.cart_color, fallbackColor),
    remark: row.remark || '',
  }
}

module.exports = {
  defaultPlanningAccess,
  defaultPlanningColor,
  ensurePurchasePlanningUserSettings,
  ensurePurchasePlanningUserSettingForUser,
  isPlanningAdmin,
  validPlanningColor,
}
