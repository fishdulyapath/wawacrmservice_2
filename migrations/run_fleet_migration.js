'use strict'
require('dotenv').config()
const { Pool } = require('pg')
const fs = require('fs')
const path = require('path')

const pool = new Pool({
  host:     process.env.CRM_HOST,
  port:     parseInt(process.env.CRM_PORT),
  user:     process.env.CRM_USER,
  password: process.env.CRM_PASSWORD,
  database: process.env.CRM_DB,
  ssl:      false,
})

async function run() {
  const sql = fs.readFileSync(path.join(__dirname, 'fleet_tables.sql'), 'utf8')
  const client = await pool.connect()
  try {
    await client.query(sql)
    console.log('✅ Migration สำเร็จ — fleet tables สร้างแล้วใน CRM DB')
  } finally {
    client.release()
    await pool.end()
  }
}

run().catch(e => { console.error('❌ Migration ล้มเหลว:', e.message); process.exit(1) })
