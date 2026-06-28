'use strict'
require('dotenv').config()
const { posDB } = require('../db')
const fs = require('fs')
const path = require('path')

async function main() {
  const migrationsDir = path.join(__dirname, '..', 'migrations')
  const files = [
    'purchase_planning_master.sql',
    'purchase_planning_configured_only.sql',
  ]

  for (const file of files) {
    const filePath = path.join(migrationsDir, file)
    if (!fs.existsSync(filePath)) {
      console.log(`SKIP (not found): ${file}`)
      continue
    }
    const sql = fs.readFileSync(filePath, 'utf8')
    console.log(`\nRunning: ${file}`)
    try {
      await posDB.query(sql)
      console.log(`  OK`)
    } catch (err) {
      console.error(`  ERROR: ${err.message}`)
    }
  }

  // ตรวจสอบ view definition หลัง run
  console.log('\nVerifying view definition...')
  try {
    const { rows } = await posDB.query(
      `SELECT definition FROM pg_views WHERE viewname = 'purchase_planning_item_supplier_resolved'`
    )
    if (rows.length) {
      console.log('View definition (planning_enabled line):')
      const lines = rows[0].definition.split('\n').filter(l => l.toLowerCase().includes('planning_enabled'))
      lines.forEach(l => console.log(' ', l.trim()))
    } else {
      console.log('View not found!')
    }
  } catch (err) {
    console.error('Verify error:', err.message)
  }

  await posDB.end()
  process.exit(0)
}

main()
