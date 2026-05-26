'use strict'

const { google } = require('googleapis')
const path = require('path')
const fs   = require('fs')
const { crmDB } = require('../db')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_SPREADSHEET_ID
  || '1JVK5l--hKyv8KmvsL_gfpykyJEAu572ye0UkJe6pGkM'

// หา key file: ลอง env path ก่อน → ถ้าไม่เจอ ลองหาไฟล์ appsheet-*.json ใน backend dir
function resolveKeyPath() {
  const envPath = process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH
  if (envPath) {
    const abs = path.isAbsolute(envPath) ? envPath : path.resolve(__dirname, '..', envPath)
    if (fs.existsSync(abs)) return abs
  }
  try {
    const backendDir = path.resolve(__dirname, '..')
    const match = fs.readdirSync(backendDir).find(f => /^appsheet-.*\.json$/.test(f))
    if (match) return path.join(backendDir, match)
  } catch {}
  return null
}
const KEY_PATH = resolveKeyPath()

// ---------------------------------------------------------------------------
// Google Sheets client — Service Account (read-only, ไม่มีวันหมดอายุ)
// ---------------------------------------------------------------------------
let _sheetsClient = null

async function getSheetsClient() {
  if (_sheetsClient) return _sheetsClient
  const auth = new google.auth.GoogleAuth({
    keyFile: KEY_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  })
  _sheetsClient = google.sheets({ version: 'v4', auth })
  return _sheetsClient
}

// ---------------------------------------------------------------------------
// getSheet — ดึง sheet แล้ว return array of objects (header row เป็น key)
// ---------------------------------------------------------------------------
async function getSheet(sheetName, retries = 3) {
  const sheets = await getSheetsClient()
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: sheetName,
      })
      const rows = res.data.values || []
      if (rows.length < 2) return []
      const headers = rows[0].map(h => h.trim())
      return rows.slice(1).map(row => {
        const obj = {}
        headers.forEach((h, i) => { obj[h] = row[i] ?? null })
        return obj
      })
    } catch (err) {
      if (err.code === 429 && attempt < retries) {
        await sleep(1000 * 2 ** attempt)
        continue
      }
      throw err
    }
  }
}

// ---------------------------------------------------------------------------
// Type helpers
// ---------------------------------------------------------------------------
const toText = v => (v == null || v === '' ? null : String(v).trim())
const toNum = v => {
  const n = parseFloat(String(v).replace(/,/g, ''))
  return isNaN(n) ? null : n
}
const toInt = v => {
  const n = parseInt(String(v).replace(/,/g, ''), 10)
  return isNaN(n) ? null : n
}
const toBool = v => {
  if (v == null || v === '') return null
  return ['ใช่', 'มี', 'true', '1', 'yes'].includes(String(v).toLowerCase().trim())
}
const toTs = v => {
  if (!v) return null
  const s = String(v).trim()
  // Try direct parse first (handles ISO and M/D/YYYY H:M:S from AppSheet)
  const d = new Date(s)
  if (!isNaN(d.getTime())) return d.toISOString()
  // Fallback: replace space with T for ISO-like strings
  const d2 = new Date(s.replace(' ', 'T'))
  return isNaN(d2.getTime()) ? null : d2.toISOString()
}
const toDate = v => {
  if (!v) return null
  const s = String(v).trim()
  // M/D/YYYY or M/D/YYYY H:M:S format from AppSheet
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/)
  if (m) {
    const [, mo, day, yr] = m
    return `${yr}-${mo.padStart(2,'0')}-${day.padStart(2,'0')}`
  }
  return s.split(' ')[0] || null
}

// parse วันจาก image path เช่น "car_release_image/2568/06/xxx.jpg"
// พ.ศ. → ค.ศ. (ลบ 543)
const dateFromImagePath = v => {
  if (!v) return null
  const m = String(v).match(/\/(\d{4})\/(\d{2})\//)
  if (!m) return null
  const year = parseInt(m[1])
  const month = m[2]
  const ce = year > 2400 ? year - 543 : year   // แปลง พ.ศ. → ค.ศ.
  return `${ce}-${month}-01`
}

const imagePathInFolder = (value, targetFolder) => {
  const text = toText(value)
  if (!text) return null
  const normalized = text.replace(/\\/g, '/').replace(/^\/+/, '')
  const parts = normalized.split('/')
  if (parts.length < 2) return `${targetFolder}/${normalized}`
  parts[0] = targetFolder
  return parts.join('/')
}

const sleep = ms => new Promise(r => setTimeout(r, ms))

async function markMissingRows(tableName, idColumn, activeIds) {
  const ids = [...new Set(activeIds.filter(Boolean))]
  if (!ids.length) return
  const allowed = new Set([
    'fleet_payments:payment_id',
    'fleet_visits:visit_id',
    'fleet_return_products:return_product_id',
    'fleet_return_documents:return_document_id',
  ])
  if (!allowed.has(`${tableName}:${idColumn}`)) throw new Error(`Unsafe sync table: ${tableName}.${idColumn}`)
  await crmDB.query(
    `UPDATE ${tableName}
     SET deleted_at = NOW(), synced_at = NOW()
     WHERE deleted_at IS NULL AND NOT (${idColumn} = ANY($1))`,
    [ids]
  )
}

let problemImagesTableReady = false

async function ensureProblemImagesTable() {
  if (problemImagesTableReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS fleet_problem_images (
      image_problem_id TEXT PRIMARY KEY,
      problem_id       TEXT NOT NULL,
      image_path       TEXT,
      note             TEXT,
      created_at       TIMESTAMPTZ,
      synced_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at       TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_problem_images_problem_id ON fleet_problem_images(problem_id);
    CREATE INDEX IF NOT EXISTS idx_fleet_problem_images_created_at ON fleet_problem_images(created_at);
  `)
  problemImagesTableReady = true
}

let storeReportTablesReady = false

async function ensureStoreReportTables() {
  if (storeReportTablesReady) return
  await crmDB.query(`
    CREATE TABLE IF NOT EXISTS fleet_payments (
      payment_id   TEXT PRIMARY KEY,
      payment_name TEXT,
      created_at   TIMESTAMPTZ,
      synced_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at   TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_payments_name ON fleet_payments(payment_name);

    CREATE TABLE IF NOT EXISTS fleet_visits (
      visit_id   TEXT PRIMARY KEY,
      visit_name TEXT,
      created_by TEXT,
      created_at TIMESTAMPTZ,
      synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_visits_name ON fleet_visits(visit_name);

    CREATE TABLE IF NOT EXISTS fleet_return_products (
      return_product_id TEXT PRIMARY KEY,
      check_out_id      TEXT NOT NULL,
      no                INTEGER,
      product_name      TEXT,
      quantity          NUMERIC,
      total             NUMERIC,
      synced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at        TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_return_products_check_out_id ON fleet_return_products(check_out_id);
    CREATE INDEX IF NOT EXISTS idx_fleet_return_products_check_out_total ON fleet_return_products(check_out_id, total) WHERE deleted_at IS NULL;

    CREATE TABLE IF NOT EXISTS fleet_return_documents (
      return_document_id TEXT PRIMARY KEY,
      car_release_id     TEXT NOT NULL,
      image_path         TEXT,
      created_by         TEXT,
      created_at         TIMESTAMPTZ,
      synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      deleted_at         TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_fleet_return_documents_car_release_id ON fleet_return_documents(car_release_id);
    CREATE INDEX IF NOT EXISTS idx_fleet_return_documents_created_at ON fleet_return_documents(created_at);
  `)
  storeReportTablesReady = true
}

// ---------------------------------------------------------------------------
// Sync log helpers
// ---------------------------------------------------------------------------
async function startLog(sheetName) {
  const res = await crmDB.query(
    `INSERT INTO fleet_sync_logs (sheet_name, status) VALUES ($1, 'running') RETURNING id`,
    [sheetName]
  )
  return res.rows[0].id
}

async function finishLog(id, status, rowsSynced, rowsFailed, durationMs, errorMsg) {
  await crmDB.query(
    `UPDATE fleet_sync_logs
     SET status=$2, rows_synced=$3, rows_failed=$4, duration_ms=$5, error_message=$6, finished_at=NOW()
     WHERE id=$1`,
    [id, status, rowsSynced, rowsFailed, durationMs, errorMsg]
  )
}

// ---------------------------------------------------------------------------
// Individual sheet syncs
// ---------------------------------------------------------------------------
async function syncUsers() {
  const rows = await getSheet('user')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['user_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_users
           (user_id, username, name, level_user_id, phone_number, image_profile, location_now, language, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
         ON CONFLICT (user_id) DO UPDATE SET
           username=EXCLUDED.username, name=EXCLUDED.name,
           level_user_id=EXCLUDED.level_user_id, phone_number=EXCLUDED.phone_number,
           image_profile=EXCLUDED.image_profile, location_now=EXCLUDED.location_now,
           language=EXCLUDED.language, created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['username']), toText(r['name']), toText(r['level_user_id']),
         toText(r['phone_number 1'] ?? r['phone_number']), toText(r['image_profile']),
         toText(r['location_now']), toText(r['language']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncUsers] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCars() {
  const rows = await getSheet('car')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['car_id'])
    if (!id) continue
    try {
      const carName = [r['brand'], r['model'], r['sub_model']].filter(Boolean).join(' ').trim() || null
      await crmDB.query(
        `INSERT INTO fleet_cars (car_id, car_name, license_plate, car_type, brand, model, sub_model, year, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
         ON CONFLICT (car_id) DO UPDATE SET
           car_name=EXCLUDED.car_name, license_plate=EXCLUDED.license_plate,
           car_type=EXCLUDED.car_type, brand=EXCLUDED.brand, model=EXCLUDED.model,
           sub_model=EXCLUDED.sub_model, year=EXCLUDED.year,
           created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        // $4=car_type uses brand (no separate car_type column in sheet), $5=brand, $6=model, $7=sub_model
        [id, carName, toText(r['license_plate']), toText(r['brand']),
         toText(r['brand']), toText(r['model']), toText(r['sub_model']),
         toInt(r['year']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncCars] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncStores() {
  const rows = await getSheet('store')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['store_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_stores (store_id, store_name, address, phone, location, zone, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
         ON CONFLICT (store_id) DO UPDATE SET
           store_name=EXCLUDED.store_name, address=EXCLUDED.address, phone=EXCLUDED.phone,
           location=EXCLUDED.location, zone=EXCLUDED.zone, created_at=EXCLUDED.created_at,
           synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['store_name']), toText(r['store_address']),
         toText(r['telephone_number']), toText(r['store_location']),
         toText(r['zone']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncStores] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncGroupStores() {
  const rows = await getSheet('name_car_release')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['name_car_release_id'] ?? r['group_store_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_group_stores (group_store_id, group_name, description, created_at, synced_at)
         VALUES ($1,$2,$3,$4,NOW())
         ON CONFLICT (group_store_id) DO UPDATE SET
           group_name=EXCLUDED.group_name, description=EXCLUDED.description,
           created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['name_car_release_name'] ?? r['name'] ?? r['group_name']), toText(r['description']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncGroupStores] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCarReleases() {
  const rows = await getSheet('car_release')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['car_release_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_car_releases
           (car_release_id, car_id, user_id, name_car_release_id, group_store_id,
            mileage, mileage_return, description, total_amount, total_number_of_bills,
            accounting_status, car_release_code, car_release_image, car_return_image,
            image_mileage, image_front, image_around_1, image_around_2, image_around_3, image_around_4,
            trip_date, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,NOW())
         ON CONFLICT (car_release_id) DO UPDATE SET
           car_id=EXCLUDED.car_id, user_id=EXCLUDED.user_id,
           name_car_release_id=EXCLUDED.name_car_release_id,
           group_store_id=EXCLUDED.group_store_id,
           mileage=EXCLUDED.mileage, mileage_return=EXCLUDED.mileage_return,
           description=EXCLUDED.description, total_amount=EXCLUDED.total_amount,
           total_number_of_bills=EXCLUDED.total_number_of_bills,
           accounting_status=EXCLUDED.accounting_status,
           car_release_code=EXCLUDED.car_release_code,
           car_release_image=EXCLUDED.car_release_image,
           car_return_image=EXCLUDED.car_return_image,
           image_mileage=EXCLUDED.image_mileage,
           image_front=EXCLUDED.image_front,
           image_around_1=EXCLUDED.image_around_1,
           image_around_2=EXCLUDED.image_around_2,
           image_around_3=EXCLUDED.image_around_3,
           image_around_4=EXCLUDED.image_around_4,
           trip_date=EXCLUDED.trip_date, created_at=EXCLUDED.created_at,
           synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['car_id']), toText(r['user_id']), toText(r['name_car_release_id']),
         toText(r['group_store_id']), toNum(r['mileage']), toNum(r['mileage_return']),
         toText(r['description']), toNum(r['total_amount']), toInt(r['total_number_of_bills']),
         toText(r['accounting_status']), toText(r['car_release_code']),
         toText(r['car_release_image'] ?? r['image_front'] ?? r['image_mileage'] ??
           r['image_around_1'] ?? r['image_around_2'] ?? r['image_around_3'] ??
           r['image_around_4'] ?? r['image_pda'] ?? r['image_car_release']),
         null,
         toText(r['image_mileage']), toText(r['image_front']), toText(r['image_around_1']),
         toText(r['image_around_2']), toText(r['image_around_3']), toText(r['image_around_4']),
         toDate(r['created_at']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncCarReleases] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCarReturns() {
  const rows = await getSheet('car_return')
  let synced = 0, failed = 0, skipped = 0
  for (const r of rows) {
    const id = toText(r['car_return_id'])
    const releaseId = toText(r['car_release_id'])
    if (!id || !releaseId) continue
    const images = {
      mileage: imagePathInFolder(r['image_mileage'], 'car_return_image'),
      front: imagePathInFolder(r['image_front'], 'car_return_image'),
      around1: imagePathInFolder(r['image_around_1'], 'car_return_image'),
      around2: imagePathInFolder(r['image_around_2'], 'car_return_image'),
      around3: imagePathInFolder(r['image_around_3'], 'car_return_image'),
      around4: imagePathInFolder(r['image_around_4'], 'car_return_image'),
    }
    try {
      const result = await crmDB.query(
        `UPDATE fleet_car_releases
         SET car_return_id=$2,
             car_return_image=COALESCE($4,$3,$5,$6,$7,$8),
             mileage_return=COALESCE($9, mileage_return),
             return_image_mileage=$3,
             return_image_front=$4,
             return_image_around_1=$5,
             return_image_around_2=$6,
             return_image_around_3=$7,
             return_image_around_4=$8,
             synced_at=NOW()
         WHERE car_release_id=$1 AND deleted_at IS NULL`,
        [releaseId, id, images.mileage, images.front, images.around1,
         images.around2, images.around3, images.around4, toNum(r['mileage'])]
      )
      if (result.rowCount) synced++
      else skipped++
    } catch (err) { failed++; console.error(`[syncCarReturns] row ${id}:`, err.message) }
  }
  return { synced, failed, skipped }
}

async function syncListStores() {
  const rows = await getSheet('list_store')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['list_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_list_stores
           (list_id, group_store_id, store_id, sequence_no,
            off_site, bypass, data_store_no, store_name_result, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW())
         ON CONFLICT (list_id) DO UPDATE SET
           group_store_id=EXCLUDED.group_store_id, store_id=EXCLUDED.store_id,
           sequence_no=EXCLUDED.sequence_no, off_site=EXCLUDED.off_site,
           bypass=EXCLUDED.bypass, data_store_no=EXCLUDED.data_store_no,
           store_name_result=EXCLUDED.store_name_result,
           created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        // store_id in sheet is in 'store_name' column (e.g. "OR-01466"), data_store_id is empty
        [id, toText(r['group_store_id']), toText(r['store_name']),
         toInt(r['row_order'] ?? r['sequence_no']),
         toBool(r['off_site']), toBool(r['bypass']),
         toText(r['data_store_no']), toText(r['store_name_result']),
         toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncListStores] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCheckIns() {
  const rows = await getSheet('check_in')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['check_in_id'])
    if (!id) continue
    let lat = null, lng = null
    if (r['location']) {
      const parts = String(r['location']).split(',')
      lat = toNum(parts[0])
      lng = toNum(parts[1])
    }
    try {
      await crmDB.query(
        `INSERT INTO fleet_check_ins
           (check_in_id, list_id, date_time_check_in, image_check_in, latitude, longitude, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
         ON CONFLICT (check_in_id) DO UPDATE SET
           list_id=EXCLUDED.list_id, date_time_check_in=EXCLUDED.date_time_check_in,
           image_check_in=EXCLUDED.image_check_in, latitude=EXCLUDED.latitude,
           longitude=EXCLUDED.longitude, created_at=EXCLUDED.created_at,
           synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['list_id']), toTs(r['date_time_check_in']),
         toText(r['check_in_image'] ?? r['image_check_in']), lat, lng, toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncCheckIns] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCheckOuts() {
  const rows = await getSheet('check_out')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['check_out_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_check_outs
           (check_out_id, list_id, date_time_check_out, image_bill, payment_id,
            cash, transfer, amount,
            transfer_according, visit_customer, visit, visit_note,
            created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
         ON CONFLICT (check_out_id) DO UPDATE SET
           list_id=EXCLUDED.list_id, date_time_check_out=EXCLUDED.date_time_check_out,
           image_bill=EXCLUDED.image_bill, payment_id=EXCLUDED.payment_id,
           cash=EXCLUDED.cash, transfer=EXCLUDED.transfer, amount=EXCLUDED.amount,
           transfer_according=EXCLUDED.transfer_according,
           visit_customer=EXCLUDED.visit_customer, visit=EXCLUDED.visit,
           visit_note=EXCLUDED.visit_note,
           created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['list_id']), toTs(r['date_time_check_out']),
         imagePathInFolder(r['image_check_out'] ?? r['check_out_image'] ?? r['image_bill'], 'check_out_image'),
         toText(r['payment_id']),
         toNum(r['cash']), toNum(r['transfer']), toNum(r['amount']),
         toNum(r['transfer_according']), toText(r['visit_customer']),
         toText(r['visit']), toText(r['visit_note']),
         toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncCheckOuts] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncCheckOutImages() {
  const rows = await getSheet('image_check_out')
  let synced = 0, failed = 0
  const records = []
  for (const r of rows) {
    const checkOutId = toText(r['check_out_id'])
    const imagePath = imagePathInFolder(
      r['image_check_out'] ?? r['check_out_image'] ?? r['image_path'] ?? r['image'] ?? r['image_bill'],
      'check_out_image'
    )
    const id = toText(
      r['image_check_out_id'] ?? r['check_out_image_id'] ?? r['id'] ?? r['_RowNumber']
    ) || (checkOutId && imagePath ? `${checkOutId}:${imagePath}` : null)
    if (!id || !checkOutId || !imagePath) continue
    records.push([
      id,
      checkOutId,
      imagePath,
      toText(r['note'] ?? r['remark'] ?? r['description']),
      toTs(r['created_at']),
    ])
  }

  const batchSize = 500
  for (let offset = 0; offset < records.length; offset += batchSize) {
    const chunk = records.slice(offset, offset + batchSize)
    const params = []
    const values = chunk.map((record, i) => {
      const base = i * 5
      params.push(...record)
      return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},NOW())`
    }).join(',')
    try {
      await crmDB.query(
        `INSERT INTO fleet_check_out_images
           (image_check_out_id, check_out_id, image_path, note, created_at, synced_at)
         VALUES ${values}
         ON CONFLICT (image_check_out_id) DO UPDATE SET
           check_out_id=EXCLUDED.check_out_id,
           image_path=EXCLUDED.image_path,
           note=EXCLUDED.note,
           created_at=EXCLUDED.created_at,
           synced_at=NOW(),
           deleted_at=NULL`,
        params
      )
      synced += chunk.length
    } catch (err) {
      failed += chunk.length
      console.error(`[syncCheckOutImages] batch ${offset}-${offset + chunk.length}:`, err.message)
    }
  }
  return { synced, failed }
}

async function syncProblems() {
  const rows = await getSheet('problem')
  let synced = 0, failed = 0
  for (const r of rows) {
    const id = toText(r['problem_id'])
    if (!id) continue
    try {
      await crmDB.query(
        `INSERT INTO fleet_problems
           (problem_id, list_id, problem_type, description, image_problem, is_resolved,
            normal_bill, normal_bill_note, edit_bill, edit_bill_note,
            product_swap, product_swap_note, out_of_stock, out_of_stock_note,
            overstock, overstock_note,
            created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW())
         ON CONFLICT (problem_id) DO UPDATE SET
           list_id=EXCLUDED.list_id, problem_type=EXCLUDED.problem_type,
           description=EXCLUDED.description, image_problem=EXCLUDED.image_problem,
           is_resolved=EXCLUDED.is_resolved,
           normal_bill=EXCLUDED.normal_bill, normal_bill_note=EXCLUDED.normal_bill_note,
           edit_bill=EXCLUDED.edit_bill, edit_bill_note=EXCLUDED.edit_bill_note,
           product_swap=EXCLUDED.product_swap, product_swap_note=EXCLUDED.product_swap_note,
           out_of_stock=EXCLUDED.out_of_stock, out_of_stock_note=EXCLUDED.out_of_stock_note,
           overstock=EXCLUDED.overstock, overstock_note=EXCLUDED.overstock_note,
           created_at=EXCLUDED.created_at, synced_at=NOW(), deleted_at=NULL`,
        [id, toText(r['list_id']), toText(r['problem_name'] ?? r['problem_type']),
         toText(r['description']), imagePathInFolder(r['problem_image'] ?? r['image_problem'], 'problem_image'),
         toBool(r['is_resolved'] ?? r['resolved']),
         toText(r['Normal_bill']), toText(r['Normal_bill_note']),
         toText(r['Edit_bill']), toText(r['Edit_bill_note']),
         toText(r['Product_swap']), toText(r['Product_swap_note']),
         toText(r['Out_of_stock']), toText(r['Out_of_stock_note']),
         toText(r['Overstock']), toText(r['Overstock_note']),
         toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncProblems] row ${id}:`, err.message) }
  }
  return { synced, failed }
}

async function syncProblemImages() {
  await ensureProblemImagesTable()
  const rows = await getSheet('image_problem')
  let synced = 0, failed = 0
  const records = []
  for (const r of rows) {
    const problemId = toText(r['problem_id'])
    const imagePath = imagePathInFolder(
      r['image_problem'] ?? r['problem_image'] ?? r['image_path'] ?? r['image'],
      'problem_image'
    )
    const id = toText(
      r['image_problem_id'] ?? r['problem_image_id'] ?? r['id'] ?? r['_RowNumber']
    ) || (problemId && imagePath ? `${problemId}:${imagePath}` : null)
    if (!id || !problemId || !imagePath) continue
    records.push([
      id,
      problemId,
      imagePath,
      toText(r['note'] ?? r['remark'] ?? r['description']),
      toTs(r['created_at']),
    ])
  }

  const batchSize = 500
  for (let offset = 0; offset < records.length; offset += batchSize) {
    const chunk = records.slice(offset, offset + batchSize)
    const params = []
    const values = chunk.map((record, i) => {
      const base = i * 5
      params.push(...record)
      return `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},NOW())`
    }).join(',')
    try {
      await crmDB.query(
        `INSERT INTO fleet_problem_images
           (image_problem_id, problem_id, image_path, note, created_at, synced_at)
         VALUES ${values}
         ON CONFLICT (image_problem_id) DO UPDATE SET
           problem_id=EXCLUDED.problem_id,
           image_path=EXCLUDED.image_path,
           note=EXCLUDED.note,
           created_at=EXCLUDED.created_at,
           synced_at=NOW(),
           deleted_at=NULL`,
        params
      )
      synced += chunk.length
    } catch (err) {
      failed += chunk.length
      console.error(`[syncProblemImages] batch ${offset}-${offset + chunk.length}:`, err.message)
    }
  }
  return { synced, failed }
}

// ---------------------------------------------------------------------------
// syncAllFleetSheets — orchestrator เรียกทุก sheet ตามลำดับ dependency
// ---------------------------------------------------------------------------
async function syncPayments() {
  await ensureStoreReportTables()
  const rows = await getSheet('payment')
  let synced = 0, failed = 0
  const activeIds = []
  for (const r of rows) {
    const id = toText(r['payment_id'])
    if (!id) continue
    activeIds.push(id)
    try {
      await crmDB.query(
        `INSERT INTO fleet_payments (payment_id, payment_name, created_at, synced_at)
         VALUES ($1,$2,$3,NOW())
         ON CONFLICT (payment_id) DO UPDATE SET
           payment_name=EXCLUDED.payment_name,
           created_at=EXCLUDED.created_at,
           synced_at=NOW(),
           deleted_at=NULL`,
        [id, toText(r['payment_name']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncPayments] row ${id}:`, err.message) }
  }
  await markMissingRows('fleet_payments', 'payment_id', activeIds)
  return { synced, failed }
}

async function syncVisits() {
  await ensureStoreReportTables()
  const rows = await getSheet('visit')
  let synced = 0, failed = 0
  const activeIds = []
  for (const r of rows) {
    const id = toText(r['visit_id'])
    if (!id) continue
    activeIds.push(id)
    try {
      await crmDB.query(
        `INSERT INTO fleet_visits (visit_id, visit_name, created_by, created_at, synced_at)
         VALUES ($1,$2,$3,$4,NOW())
         ON CONFLICT (visit_id) DO UPDATE SET
           visit_name=EXCLUDED.visit_name,
           created_by=EXCLUDED.created_by,
           created_at=EXCLUDED.created_at,
           synced_at=NOW(),
           deleted_at=NULL`,
        [id, toText(r['visit_name']), toText(r['created_by']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncVisits] row ${id}:`, err.message) }
  }
  await markMissingRows('fleet_visits', 'visit_id', activeIds)
  return { synced, failed }
}

async function syncReturnProducts() {
  await ensureStoreReportTables()
  const rows = await getSheet('return_product')
  let synced = 0, failed = 0
  const activeIds = []
  for (const r of rows) {
    const id = toText(r['return_product_id'])
    const checkOutId = toText(r['check_out_id'])
    if (!id || !checkOutId) continue
    activeIds.push(id)
    try {
      await crmDB.query(
        `INSERT INTO fleet_return_products
           (return_product_id, check_out_id, no, product_name, quantity, total, synced_at)
         VALUES ($1,$2,$3,$4,$5,$6,NOW())
         ON CONFLICT (return_product_id) DO UPDATE SET
           check_out_id=EXCLUDED.check_out_id,
           no=EXCLUDED.no,
           product_name=EXCLUDED.product_name,
           quantity=EXCLUDED.quantity,
           total=EXCLUDED.total,
           synced_at=NOW(),
           deleted_at=NULL`,
        [id, checkOutId, toInt(r['no']), toText(r['product_name']), toNum(r['quantity']), toNum(r['total'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncReturnProducts] row ${id}:`, err.message) }
  }
  await markMissingRows('fleet_return_products', 'return_product_id', activeIds)
  return { synced, failed }
}

async function syncReturnDocuments() {
  await ensureStoreReportTables()
  const rows = await getSheet('return_document')
  let synced = 0, failed = 0
  const activeIds = []
  for (const r of rows) {
    const id = toText(r['return_document_id'])
    const carReleaseId = toText(r['car_release_id'])
    if (!id || !carReleaseId) continue
    activeIds.push(id)
    try {
      await crmDB.query(
        `INSERT INTO fleet_return_documents
           (return_document_id, car_release_id, image_path, created_by, created_at, synced_at)
         VALUES ($1,$2,$3,$4,$5,NOW())
         ON CONFLICT (return_document_id) DO UPDATE SET
           car_release_id=EXCLUDED.car_release_id,
           image_path=EXCLUDED.image_path,
           created_by=EXCLUDED.created_by,
           created_at=EXCLUDED.created_at,
           synced_at=NOW(),
           deleted_at=NULL`,
        [id, carReleaseId, imagePathInFolder(r['image'] ?? r['image_path'], 'return_document_Images'),
         toText(r['created_by']), toTs(r['created_at'])]
      )
      synced++
    } catch (err) { failed++; console.error(`[syncReturnDocuments] row ${id}:`, err.message) }
  }
  await markMissingRows('fleet_return_documents', 'return_document_id', activeIds)
  return { synced, failed }
}

const SYNCS = [
  { name: 'user',             fn: syncUsers },
  { name: 'car',              fn: syncCars },
  { name: 'store',            fn: syncStores },
  { name: 'payment',          fn: syncPayments },
  { name: 'visit',            fn: syncVisits },
  { name: 'name_car_release', fn: syncGroupStores },
  { name: 'car_release',      fn: syncCarReleases },
  { name: 'car_return',       fn: syncCarReturns },
  { name: 'return_document',  fn: syncReturnDocuments },
  { name: 'list_store',       fn: syncListStores },
  { name: 'check_in',         fn: syncCheckIns },
  { name: 'check_out',        fn: syncCheckOuts },
  { name: 'image_check_out',  fn: syncCheckOutImages },
  { name: 'return_product',   fn: syncReturnProducts },
  { name: 'problem',          fn: syncProblems },
  { name: 'image_problem',    fn: syncProblemImages },
]

let syncAllRunning = false
const runningSheets = new Set()

async function syncAllFleetSheets() {
  if (!SPREADSHEET_ID || !KEY_PATH) {
    console.warn('[fleetSync] GOOGLE_SHEETS_SPREADSHEET_ID หรือ GOOGLE_SERVICE_ACCOUNT_KEY_PATH ยังไม่ได้ตั้งค่า — ข้าม sync')
    return
  }
  if (syncAllRunning) {
    console.warn('[fleetSync] skip syncAllFleetSheets: previous fleet sync is still running')
    return { skipped: true, reason: 'fleet sync already running' }
  }

  syncAllRunning = true
  try {
    console.log('[fleetSync] เริ่ม sync', new Date().toISOString())
    for (const { name, fn } of SYNCS) {
      if (runningSheets.has(name)) {
        console.warn(`[fleetSync] skip ${name}: sheet sync is already running`)
        continue
      }

      runningSheets.add(name)
      const logId = await startLog(name)
      const t0 = Date.now()
      try {
        const { synced, failed } = await fn()
        await finishLog(logId, 'success', synced, failed, Date.now() - t0, null)
        console.log(`[fleetSync] ${name}: synced=${synced} failed=${failed}`)
      } catch (err) {
        await finishLog(logId, 'failed', 0, 0, Date.now() - t0, err.message)
        console.error(`[fleetSync] ${name} ERROR:`, err.message)
      } finally {
        runningSheets.delete(name)
      }
    }
    console.log('[fleetSync] เสร็จสิ้น', new Date().toISOString())
  } finally {
    syncAllRunning = false
  }
}

async function syncSingleSheet(sheetName) {
  const entry = SYNCS.find(s => s.name === sheetName)
  if (!entry) throw new Error(`ไม่พบ sync function สำหรับ sheet: ${sheetName}`)
  if (runningSheets.has(sheetName)) {
    console.warn(`[fleetSync] skip ${sheetName}: sheet sync is already running`)
    return { synced: 0, failed: 0, skipped: true, reason: 'sheet sync already running' }
  }

  runningSheets.add(sheetName)
  const logId = await startLog(sheetName)
  const t0 = Date.now()
  try {
    const { synced, failed } = await entry.fn()
    await finishLog(logId, 'success', synced, failed, Date.now() - t0, null)
    return { synced, failed }
  } catch (err) {
    await finishLog(logId, 'failed', 0, 0, Date.now() - t0, err.message)
    throw err
  } finally {
    runningSheets.delete(sheetName)
  }
}

module.exports = { syncAllFleetSheets, syncSingleSheet, SYNCS }
