'use strict'
require('dotenv').config()
const { google } = require('googleapis')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_SPREADSHEET_ID
const KEY_PATH       = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH

const SHEETS_TO_CHECK = ['user', 'car', 'store', 'name_car_release', 'car_release', 'list_store', 'check_in', 'check_out', 'problem']

async function main() {
  if (!SPREADSHEET_ID || !KEY_PATH) {
    console.error('Missing GOOGLE_SHEETS_SPREADSHEET_ID or GOOGLE_SERVICE_ACCOUNT_KEY_PATH')
    process.exit(1)
  }
  console.log('SPREADSHEET_ID:', SPREADSHEET_ID)
  console.log('KEY_PATH:', KEY_PATH)

  const auth = new google.auth.GoogleAuth({
    keyFile: KEY_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  })
  const sheets = google.sheets({ version: 'v4', auth })

  for (const sheetName of SHEETS_TO_CHECK) {
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!A1:ZZ1`, // just header row
      })
      const rows = res.data.values || []
      if (!rows.length) {
        console.log(`\n[${sheetName}] EMPTY or NOT FOUND`)
        continue
      }
      const headers = rows[0].map(h => h?.trim())
      console.log(`\n[${sheetName}] headers (${headers.length}):`)
      headers.forEach((h, i) => console.log(`  ${i}: "${h}"`))

      // Also show first 2 data rows to understand format
      const res2 = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetName}!A1:ZZ3`,
      })
      const dataRows = (res2.data.values || []).slice(1)
      dataRows.forEach((row, ri) => {
        console.log(`  row${ri+1}:`, headers.map((h, i) => `${h}=${row[i] ?? '(null)'}`).join(' | '))
      })
    } catch (err) {
      console.log(`\n[${sheetName}] ERROR: ${err.message}`)
    }
  }
}

main().catch(console.error)
