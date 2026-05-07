'use strict'
require('dotenv').config()
const { google } = require('googleapis')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_SPREADSHEET_ID
const KEY_PATH       = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH

async function main() {
  const auth = new google.auth.GoogleAuth({ keyFile: KEY_PATH, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] })
  const sheets = google.sheets({ version: 'v4', auth })

  // Get first 20 store rows
  const r1 = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'store!A1:J21' })
  const rows1 = r1.data.values || []
  const headers1 = rows1[0]
  console.log('=== store (first 20 data rows) ===')
  rows1.slice(1).forEach(row => {
    console.log(headers1.map((h, i) => `${h}=${row[i]??''}`).join(' | '))
  })

  // Get first 20 list_store rows
  const r2 = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: 'list_store!A1:P21' })
  const rows2 = r2.data.values || []
  const headers2 = rows2[0]
  console.log('\n=== list_store (first 20 data rows) ===')
  rows2.slice(1).forEach(row => {
    console.log(`store_name=${row[3]??''} | data_store_id=${row[7]??''} | group_store_id=${row[6]??''}`)
  })
}

main().catch(console.error)
