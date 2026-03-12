const { Pool } = require('pg')
require('dotenv').config()

// POS Database — Master ลูกค้า (demserver.3bbddns.com)
const posDB = new Pool({
  host: process.env.POS_HOST,
  port: parseInt(process.env.POS_PORT),
  user: process.env.POS_USER,
  password: process.env.POS_PASSWORD,
  database: process.env.POS_DB,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
})

// CRM Database — CRM Data (wawa.iszai.com)
const crmDB = new Pool({
  host: process.env.CRM_HOST,
  port: parseInt(process.env.CRM_PORT),
  user: process.env.CRM_USER,
  password: process.env.CRM_PASSWORD,
  database: process.env.CRM_DB,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
})

// ตั้ง timezone GMT+7 ทุก connection
crmDB.on('connect', client => {
  client.query("SET timezone = 'Asia/Bangkok'")
})

posDB.on('error', (err) => console.error('[POS DB] Unexpected error:', err))
crmDB.on('error', (err) => console.error('[CRM DB] Unexpected error:', err))

module.exports = { posDB, crmDB }
