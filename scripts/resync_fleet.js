'use strict'
require('dotenv').config()
const { syncSingleSheet } = require('../services/fleetSync')

const SHEETS = ['car', 'store', 'name_car_release', 'car_release', 'car_return', 'list_store', 'check_in', 'check_out', 'problem']

async function main() {
  for (const sheet of SHEETS) {
    console.log(`\nSyncing ${sheet}...`)
    try {
      const { synced, failed } = await syncSingleSheet(sheet)
      console.log(`  Done: synced=${synced} failed=${failed}`)
    } catch (err) {
      console.error(`  ERROR: ${err.message}`)
    }
  }
  process.exit(0)
}

main()
