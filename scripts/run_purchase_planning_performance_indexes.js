'use strict'

require('dotenv').config()
const { posDB } = require('../db')

const indexes = [
  {
    name: 'idx_ic_trans_detail_pp_latest_receipt',
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ic_trans_detail_pp_latest_receipt
        ON public.ic_trans_detail (item_code, cust_code, unit_code, doc_date DESC, doc_time DESC, doc_no DESC)
        WHERE trans_flag = 310 AND COALESCE(status, 0) = 0
    `,
  },
  {
    name: 'idx_ap_item_by_supplier_pp_ic_ap',
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ap_item_by_supplier_pp_ic_ap
        ON public.ap_item_by_supplier (ic_code, ap_code)
    `,
  },
  {
    name: 'idx_ap_item_by_supplier_pp_ap_ic',
    sql: `
      CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ap_item_by_supplier_pp_ap_ic
        ON public.ap_item_by_supplier (ap_code, ic_code)
    `,
  },
]

async function main() {
  try {
    for (const index of indexes) {
      console.log(`Running: ${index.name}`)
      await posDB.query(index.sql)
      console.log('  OK')
    }
  } finally {
    await posDB.end()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
