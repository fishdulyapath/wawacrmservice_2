const express = require('express')
const crypto = require('crypto')
const { posDB, posImagesDB } = require('../db')
const { authMiddleware, requireRole } = require('../middleware/auth')

const router = express.Router()

const PRODUCT_CODE_PATTERN = /^[A-Z0-9_-]+$/
const PRODUCT_IMAGE_CACHE_CONTROL = 'private, max-age=0, must-revalidate'

// Public: รูปภาพสินค้า (GET /images/primary, GET /images/:guid) ใช้แสดงผลใน <img>
// ซึ่ง browser ส่ง Authorization header ไม่ได้ จึงยกเว้น auth/role เฉพาะทางนี้
// ส่วน /images/list (JSON metadata) และทุก write route ยังต้องมีสิทธิ์ admin เหมือนเดิม
const PUBLIC_IMAGE_GET_RE = /^\/images\/(?!list$|order$)[^/]+$/
router.use((req, res, next) => {
  if (req.method === 'GET' && PUBLIC_IMAGE_GET_RE.test(req.path)) return next()
  authMiddleware(req, res, next)
}, (req, res, next) => {
  if (req.method === 'GET' && PUBLIC_IMAGE_GET_RE.test(req.path)) return next()
  requireRole('admin')(req, res, next)
})

function activeProductCondition(alias = 'd') {
  return `COALESCE(${alias}.is_hold_sale,0) <> 1 AND COALESCE(${alias}.is_hold_purchase,0) <> 1`
}

function normalizeText(value) {
  return String(value || '').trim()
}

function normalizeQty(value) {
  const raw = String(value ?? '').replace(/,/g, '').trim()
  if (!raw) return 0
  const num = Number(raw)
  return Number.isFinite(num) ? num : NaN
}

function detectImageMime(buffer) {
  if (!buffer || buffer.length < 4) return 'application/octet-stream'
  if (
    buffer.length >= 8 &&
    buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47 &&
    buffer[4] === 0x0d && buffer[5] === 0x0a && buffer[6] === 0x1a && buffer[7] === 0x0a
  ) return 'image/png'
  if (buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) return 'image/jpeg'
  if (buffer.length >= 6 && buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x38) return 'image/gif'
  if (buffer.length >= 2 && buffer[0] === 0x42 && buffer[1] === 0x4d) return 'image/bmp'
  if (
    buffer.length >= 12 &&
    buffer[0] === 0x52 && buffer[1] === 0x49 && buffer[2] === 0x46 && buffer[3] === 0x46 &&
    buffer[8] === 0x57 && buffer[9] === 0x45 && buffer[10] === 0x42 && buffer[11] === 0x50
  ) return 'image/webp'
  return 'application/octet-stream'
}

function decodeBase64Image(imageFile) {
  const raw = String(imageFile || '').replace(/^data:[^;]+;base64,/, '')
  return Buffer.from(raw, 'base64')
}

function httpError(message, statusCode = 400) {
  const err = new Error(message)
  err.statusCode = statusCode
  return err
}

async function withPosTransaction(fn) {
  const client = await posDB.connect()
  try {
    await client.query('BEGIN')
    const result = await fn(client)
    await client.query('COMMIT')
    return result
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {})
    throw err
  } finally {
    client.release()
  }
}

async function withPosImageTransaction(fn) {
  const clientMain = await posDB.connect()
  const clientImages = await posImagesDB.connect()
  try {
    await clientMain.query('BEGIN')
    await clientImages.query('BEGIN')
    const result = await fn(clientMain, clientImages)
    await clientMain.query('COMMIT')
    await clientImages.query('COMMIT')
    return result
  } catch (err) {
    await clientMain.query('ROLLBACK').catch(() => {})
    await clientImages.query('ROLLBACK').catch(() => {})
    throw err
  } finally {
    clientMain.release()
    clientImages.release()
  }
}

async function ensureProductExists(client, itemCode) {
  const code = normalizeText(itemCode)
  if (!code) throw httpError('กรุณาระบุรหัสสินค้า')
  const result = await client.query('SELECT 1 FROM ic_inventory WHERE code=$1::text LIMIT 1', [code])
  if (!result.rows.length) throw httpError('ไม่พบสินค้า', 404)
}

async function ensureWarehouseShelfExists(client, whCode, shelfCode) {
  const wh = normalizeText(whCode)
  const shelf = normalizeText(shelfCode)
  if (!wh) throw httpError('กรุณาเลือกคลัง')
  if (!shelf) throw httpError('กรุณาเลือกที่เก็บ')

  const result = await client.query(
    'SELECT 1 FROM ic_shelf WHERE whcode=$1::text AND code=$2::text LIMIT 1',
    [wh, shelf],
  )
  if (!result.rows.length) throw httpError('คลัง/ที่เก็บไม่ถูกต้อง')
}

async function ensureProductUnitUse(client, itemCode, unitCode) {
  const code = normalizeText(itemCode)
  const unit = normalizeText(unitCode)
  if (!code || !unit) return

  await client.query(
    `INSERT INTO ic_unit_use (ic_code, code, stand_value, divide_value, ratio, row_order)
     SELECT $1::text, $2::text, 1, 1, 1, 0
     WHERE NOT EXISTS (
       SELECT 1 FROM ic_unit_use WHERE ic_code=$1::text AND code=$2::text
     )`,
    [code, unit],
  )
}

async function syncProductUnitType(client, itemCode) {
  const code = normalizeText(itemCode)
  if (!code) return

  const result = await client.query(
    `SELECT COUNT(DISTINCT NULLIF(TRIM(code::text), ''))::int AS unit_count
     FROM ic_unit_use
     WHERE ic_code=$1::text`,
    [code],
  )
  const unitCount = Number(result.rows[0]?.unit_count || 0)
  await client.query('UPDATE ic_inventory SET unit_type=$1::integer WHERE code=$2::text', [
    unitCount > 1 ? 1 : 0,
    code,
  ])
}

async function ensureMainWarehouseShelf(client, itemCode, whCode, shelfCode) {
  const code = normalizeText(itemCode)
  const wh = normalizeText(whCode)
  const shelf = normalizeText(shelfCode)

  await client.query(
    `INSERT INTO ic_wh_shelf (ic_code, wh_code, shelf_code, shelf_list, min_point, max_point, status)
     SELECT $1::text, $2::text, $3::text, '', 0, 0, 1
     WHERE NOT EXISTS (
       SELECT 1 FROM ic_wh_shelf
       WHERE ic_code=$1::text AND wh_code=$2::text AND shelf_code=$3::text
     )`,
    [code, wh, shelf],
  )
  await client.query(
    `UPDATE ic_wh_shelf
     SET status=1
     WHERE ic_code=$1::text AND wh_code=$2::text AND shelf_code=$3::text`,
    [code, wh, shelf],
  )
}

function productPayload(body) {
  return {
    code: normalizeText(body.code).toUpperCase(),
    name_1: normalizeText(body.name_1),
    name_2: normalizeText(body.name_2),
    name_eng_1: normalizeText(body.name_eng_1),
    name_eng_2: normalizeText(body.name_eng_2),
    unit_standard: normalizeText(body.unit_standard),
    unit_cost: normalizeText(body.unit_cost || body.unit_standard),
    item_category: normalizeText(body.item_category),
    item_brand: normalizeText(body.item_brand),
    group_main: normalizeText(body.group_main),
    group_sub: normalizeText(body.group_sub),
    group_sub2: normalizeText(body.group_sub2),
    item_design: normalizeText(body.item_design),
    item_model: normalizeText(body.item_model),
    wh_code: normalizeText(body.wh_code || body.start_sale_wh),
    shelf_code: normalizeText(body.shelf_code || body.start_sale_shelf),
    purchase_point: normalizeQty(body.purchase_point),
    minimum_qty: normalizeQty(body.minimum_qty),
    maximum_qty: normalizeQty(body.maximum_qty),
  }
}

function validateProductPayload(payload, { requireCode = true } = {}) {
  if (requireCode && !payload.code) throw httpError('กรุณาระบุรหัสสินค้า')
  if (requireCode && !PRODUCT_CODE_PATTERN.test(payload.code)) {
    throw httpError('รูปแบบรหัสสินค้าไม่ถูกต้อง (อนุญาต A-Z, 0-9, -, _)')
  }
  if (!payload.name_1) throw httpError('กรุณาระบุชื่อสินค้า')
  if (!payload.unit_standard) throw httpError('กรุณาเลือกหน่วยมาตรฐาน')
  if (!payload.wh_code) throw httpError('กรุณาเลือกคลัง')
  if (!payload.shelf_code) throw httpError('กรุณาเลือกที่เก็บ')

  const qtyFields = [
    ['purchase_point', 'จุดสั่งซื้อ'],
    ['minimum_qty', 'จำนวนสั่งซื้อต่ำสุด'],
    ['maximum_qty', 'จำนวนสั่งซื้อสูงสุด'],
  ]
  for (const [field, label] of qtyFields) {
    if (!Number.isFinite(payload[field])) throw httpError(`${label}ไม่ถูกต้อง`)
    if (payload[field] < 0) throw httpError(`${label}ต้องไม่ติดลบ`)
  }
  if (payload.minimum_qty > 0 && payload.maximum_qty > 0 && payload.minimum_qty > payload.maximum_qty) {
    throw httpError('จำนวนสั่งซื้อต่ำสุดต้องไม่มากกว่าจำนวนสั่งซื้อสูงสุด')
  }
}

router.get('/masters/units', async (req, res) => {
  const search = normalizeText(req.query.search)
  const like = `%${search}%`
  try {
    const result = await posDB.query(
      `SELECT code, COALESCE(name_1,'') AS name_1, COALESCE(name_2,'') AS name_2
       FROM ic_unit
       WHERE ($1 = '' OR code ILIKE $2 OR name_1 ILIKE $2 OR name_2 ILIKE $2)
       ORDER BY code
       LIMIT 200`,
      [search, like],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/masters/categories', async (req, res) => {
  const search = normalizeText(req.query.search)
  const like = `%${search}%`
  try {
    const result = await posDB.query(
      `SELECT code, COALESCE(name_1,'') AS name_1
       FROM ic_category
       WHERE ($1 = '' OR code ILIKE $2 OR name_1 ILIKE $2)
       ORDER BY code
       LIMIT 200`,
      [search, like],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/masters/warehouses', async (_req, res) => {
  try {
    const result = await posDB.query(
      `SELECT code, COALESCE(name_1,'') AS name_1
       FROM ic_warehouse
       ORDER BY code`,
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/masters/shelves', async (req, res) => {
  const whCode = normalizeText(req.query.wh_code)
  try {
    const result = await posDB.query(
      `SELECT code, whcode, COALESCE(name_1,'') AS name_1
       FROM ic_shelf
       WHERE ($1 = '' OR whcode = $1::text)
       ORDER BY whcode, code`,
      [whCode],
    )
    res.json({ data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/', async (req, res) => {
  const search = normalizeText(req.query.search)
  const page = Math.max(1, parseInt(req.query.page, 10) || 1)
  const limit = Math.min(100, Math.max(10, parseInt(req.query.limit, 10) || 20))
  const offset = (page - 1) * limit
  const sortBy = normalizeText(req.query.sort_by) || 'code'
  const sortDir = normalizeText(req.query.sort_dir).toLowerCase() === 'desc' ? 'DESC' : 'ASC'

  const sortWhitelist = {
    code: 'i.code',
    name_1: 'i.name_1',
    unit_standard: 'i.unit_standard',
    balance_qty: 'COALESCE(i.balance_qty,0)',
    purchase_point: 'COALESCE(d.purchase_point,0)',
    minimum_qty: 'COALESCE(d.minimum_qty,0)',
    maximum_qty: 'COALESCE(d.maximum_qty,0)',
  }
  const orderBy = `${sortWhitelist[sortBy] || sortWhitelist.code} ${sortDir}`

  const params = []
  const whereParts = [activeProductCondition('d')]
  const keywords = search.split(/\s+/).filter(Boolean)
  for (const keyword of keywords) {
    params.push(`%${keyword}%`)
    const p = `$${params.length}`
    whereParts.push(`(
      i.code ILIKE ${p}
      OR i.name_1 ILIKE ${p}
      OR COALESCE(i.name_eng_1,'') ILIKE ${p}
      OR EXISTS (
        SELECT 1 FROM ic_inventory_barcode b
        WHERE b.ic_code = i.code AND b.barcode ILIKE ${p}
      )
    )`)
  }

  const whereSql = `WHERE ${whereParts.join(' AND ')}`

  try {
    const countResult = await posDB.query(
      `SELECT COUNT(*)::int AS total
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       ${whereSql}`,
      params,
    )

    const dataResult = await posDB.query(
      `SELECT i.code,
              COALESCE(i.name_1,'') AS name_1,
              COALESCE(i.name_eng_1,'') AS name_eng_1,
              COALESCE(i.unit_standard,'') AS unit_standard,
              COALESCE(i.balance_qty,0) AS balance_qty,
              COALESCE(i.book_out_qty,0) AS book_out_qty,
              COALESCE(i.accrued_out_qty,0) AS accrued_out_qty,
              COALESCE(i.accrued_in_qty,0) AS accrued_in_qty,
              COALESCE(d.purchase_point,0) AS purchase_point,
              COALESCE(d.minimum_qty,0) AS minimum_qty,
              COALESCE(d.maximum_qty,0) AS maximum_qty
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       ${whereSql}
       ORDER BY ${orderBy}
       OFFSET $${params.length + 1} LIMIT $${params.length + 2}`,
      [...params, offset, limit],
    )

    res.json({
      data: dataResult.rows,
      total: countResult.rows[0]?.total || 0,
      page,
      limit,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/images/list', async (req, res) => {
  const itemCode = normalizeText(req.query.item_code)
  if (!itemCode) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้า' })

  try {
    const result = await posImagesDB.query(
      `SELECT image_id, guid_code, COALESCE(image_order,0) AS image_order
       FROM images
       WHERE image_id=$1::text
       ORDER BY image_order ASC`,
      [itemCode],
    )
    res.json({ success: true, data: result.rows })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/images/primary', async (req, res) => {
  const itemCode = normalizeText(req.query.item_code)
  if (!itemCode) return res.status(400).type('text/plain').send('ERROR: item_code is required')

  try {
    const result = await posImagesDB.query(
      `SELECT image_file
       FROM images
       WHERE image_id=$1::text
       ORDER BY image_order ASC
       LIMIT 1`,
      [itemCode],
    )

    if (!result.rows.length || !result.rows[0].image_file) {
      return res.status(404).type('text/plain').set('Cache-Control', 'no-store').send('ERROR: image not found')
    }

    const imageBytes = result.rows[0].image_file
    const etag = crypto.createHash('md5').update(imageBytes).digest('hex')
    if (req.headers['if-none-match'] === `"${etag}"`) {
      return res.status(304).set('Cache-Control', PRODUCT_IMAGE_CACHE_CONTROL).set('ETag', `"${etag}"`).end()
    }

    return res
      .status(200)
      .type(detectImageMime(imageBytes))
      .set('Cache-Control', PRODUCT_IMAGE_CACHE_CONTROL)
      .set('ETag', `"${etag}"`)
      .set('Content-Length', String(imageBytes.length))
      .send(imageBytes)
  } catch (err) {
    res.status(500).type('text/plain').send(`ERROR: ${err.message}`)
  }
})

router.post('/images', async (req, res) => {
  const itemCode = normalizeText(req.body?.item_code)
  const imageBytes = decodeBase64Image(req.body?.image_file)
  if (!itemCode || !imageBytes.length) return res.status(400).json({ error: 'กรุณาระบุรหัสสินค้าและรูปภาพ' })

  const mime = detectImageMime(imageBytes)
  if (!mime.startsWith('image/')) return res.status(400).json({ error: 'ไฟล์รูปภาพไม่ถูกต้อง' })

  try {
    const guid = crypto.randomUUID()
    const result = await withPosImageTransaction(async (clientMain, clientImages) => {
      await ensureProductExists(clientMain, itemCode)
      const orderRes = await clientMain.query(
        'SELECT COALESCE(MAX(image_order), -1) + 1 AS next_order FROM images WHERE image_id=$1::text',
        [itemCode],
      )
      const nextOrder = parseInt(orderRes.rows[0]?.next_order, 10) || 0

      await clientImages.query(
        'INSERT INTO images (image_id, image_file, guid_code, image_order) VALUES ($1::text,$2,$3::text,$4::integer)',
        [itemCode, imageBytes, guid, nextOrder],
      )
      await clientMain.query(
        'INSERT INTO images (image_id, guid_code, image_order) VALUES ($1::text,$2::text,$3::integer)',
        [itemCode, guid, nextOrder],
      )
      return { guid_code: guid, image_order: nextOrder }
    })

    res.status(201).json({ success: true, ...result })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

router.put('/images/order', async (req, res) => {
  const itemCode = normalizeText(req.body?.item_code)
  const orders = Array.isArray(req.body?.orders) ? req.body.orders : []
  if (!itemCode || !orders.length) return res.status(400).json({ error: 'ข้อมูลไม่ครบ' })

  try {
    await withPosImageTransaction(async (clientMain, clientImages) => {
      await ensureProductExists(clientMain, itemCode)
      for (const row of orders) {
        const guid = normalizeText(row.guid_code)
        const order = Math.max(0, parseInt(row.image_order, 10) || 0)
        if (!guid) continue
        await clientImages.query(
          'UPDATE images SET image_order=$1::integer WHERE guid_code=$2::text AND image_id=$3::text',
          [order, guid, itemCode],
        )
        await clientMain.query(
          'UPDATE images SET image_order=$1::integer WHERE guid_code=$2::text AND image_id=$3::text',
          [order, guid, itemCode],
        )
      }
    })

    res.json({ success: true })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

router.get('/images/:guid', async (req, res) => {
  const guid = normalizeText(req.params.guid)
  if (!guid) return res.status(400).type('text/plain').send('ERROR: guid_code is required')

  try {
    const result = await posImagesDB.query('SELECT image_file FROM images WHERE guid_code=$1::text LIMIT 1', [guid])
    if (!result.rows.length || !result.rows[0].image_file) {
      return res.status(404).type('text/plain').set('Cache-Control', 'no-store').send('ERROR: image not found')
    }

    const imageBytes = result.rows[0].image_file
    const etag = crypto.createHash('md5').update(imageBytes).digest('hex')
    if (req.headers['if-none-match'] === `"${etag}"`) {
      return res.status(304).set('Cache-Control', PRODUCT_IMAGE_CACHE_CONTROL).set('ETag', `"${etag}"`).end()
    }

    return res
      .status(200)
      .type(detectImageMime(imageBytes))
      .set('Cache-Control', PRODUCT_IMAGE_CACHE_CONTROL)
      .set('ETag', `"${etag}"`)
      .set('Content-Length', String(imageBytes.length))
      .send(imageBytes)
  } catch (err) {
    res.status(500).type('text/plain').send(`ERROR: ${err.message}`)
  }
})

router.delete('/images/:guid', async (req, res) => {
  const guid = normalizeText(req.params.guid)
  if (!guid) return res.status(400).json({ error: 'กรุณาระบุ guid_code' })

  try {
    await withPosImageTransaction(async (clientMain, clientImages) => {
      await clientImages.query('DELETE FROM images WHERE guid_code=$1::text', [guid])
      await clientMain.query('DELETE FROM images WHERE guid_code=$1::text', [guid])
    })
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.get('/:code', async (req, res) => {
  const code = normalizeText(req.params.code)
  try {
    const result = await posDB.query(
      `SELECT i.code,
              COALESCE(i.name_1,'') AS name_1,
              COALESCE(i.name_2,'') AS name_2,
              COALESCE(i.name_eng_1,'') AS name_eng_1,
              COALESCE(i.name_eng_2,'') AS name_eng_2,
              COALESCE(i.unit_standard,'') AS unit_standard,
              COALESCE(i.unit_cost,'') AS unit_cost,
              COALESCE(i.item_category,'') AS item_category,
              COALESCE(i.item_brand,'') AS item_brand,
              COALESCE(i.group_main,'') AS group_main,
              COALESCE(i.group_sub,'') AS group_sub,
              COALESCE(i.group_sub2,'') AS group_sub2,
              COALESCE(i.item_design,'') AS item_design,
              COALESCE(i.item_model,'') AS item_model,
              COALESCE(d.purchase_point,0) AS purchase_point,
              COALESCE(d.minimum_qty,0) AS minimum_qty,
              COALESCE(d.maximum_qty,0) AS maximum_qty,
              COALESCE(d.start_sale_wh,'') AS wh_code,
              COALESCE(d.start_sale_shelf,'') AS shelf_code
       FROM ic_inventory i
       LEFT JOIN ic_inventory_detail d ON d.ic_code = i.code
       WHERE i.code = $1::text AND ${activeProductCondition('d')}`,
      [code],
    )

    if (!result.rows.length) return res.status(404).json({ error: 'ไม่พบสินค้า' })
    res.json(result.rows[0])
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

router.post('/', async (req, res) => {
  const payload = productPayload(req.body || {})

  try {
    validateProductPayload(payload)

    await withPosTransaction(async (client) => {
      const exists = await client.query('SELECT 1 FROM ic_inventory WHERE code=$1::text LIMIT 1', [payload.code])
      if (exists.rows.length) throw httpError('รหัสสินค้านี้มีอยู่แล้ว')

      await ensureWarehouseShelfExists(client, payload.wh_code, payload.shelf_code)

      await client.query(
        `INSERT INTO ic_inventory (
           code, name_1, name_2, name_eng_1, name_eng_2,
           unit_standard, unit_cost, item_category, item_brand,
           group_main, group_sub, group_sub2, item_design, item_model
         ) VALUES (
           $1::text,$2::text,$3::text,$4::text,$5::text,
           $6::text,$7::text,$8::text,$9::text,
           $10::text,$11::text,$12::text,$13::text,$14::text
         )`,
        [
          payload.code,
          payload.name_1,
          payload.name_2,
          payload.name_eng_1,
          payload.name_eng_2,
          payload.unit_standard,
          payload.unit_cost || payload.unit_standard,
          payload.item_category,
          payload.item_brand,
          payload.group_main,
          payload.group_sub,
          payload.group_sub2,
          payload.item_design,
          payload.item_model,
        ],
      )

      await client.query(
        `INSERT INTO ic_inventory_detail
           (ic_code, purchase_point, minimum_qty, maximum_qty, start_sale_wh, start_sale_shelf)
         VALUES ($1::text,$2::numeric,$3::numeric,$4::numeric,$5::text,$6::text)
         ON CONFLICT (ic_code) DO UPDATE SET
           purchase_point = EXCLUDED.purchase_point,
           minimum_qty = EXCLUDED.minimum_qty,
           maximum_qty = EXCLUDED.maximum_qty,
           start_sale_wh = EXCLUDED.start_sale_wh,
           start_sale_shelf = EXCLUDED.start_sale_shelf`,
        [
          payload.code,
          payload.purchase_point,
          payload.minimum_qty,
          payload.maximum_qty,
          payload.wh_code,
          payload.shelf_code,
        ],
      )

      await ensureProductUnitUse(client, payload.code, payload.unit_standard)
      await ensureMainWarehouseShelf(client, payload.code, payload.wh_code, payload.shelf_code)
      await syncProductUnitType(client, payload.code)
    })

    res.status(201).json({ success: true, code: payload.code })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

router.put('/:code', async (req, res) => {
  const payload = productPayload({ ...(req.body || {}), code: req.params.code })

  try {
    validateProductPayload(payload)

    await withPosTransaction(async (client) => {
      const updateResult = await client.query(
        `UPDATE ic_inventory SET
           name_1=$1, name_2=$2, name_eng_1=$3, name_eng_2=$4,
           unit_standard=$5, unit_cost=$6, item_category=$7, item_brand=$8,
           group_main=$9, group_sub=$10, group_sub2=$11, item_design=$12, item_model=$13
         WHERE code=$14::text`,
        [
          payload.name_1,
          payload.name_2,
          payload.name_eng_1,
          payload.name_eng_2,
          payload.unit_standard,
          payload.unit_cost || payload.unit_standard,
          payload.item_category,
          payload.item_brand,
          payload.group_main,
          payload.group_sub,
          payload.group_sub2,
          payload.item_design,
          payload.item_model,
          payload.code,
        ],
      )
      if (updateResult.rowCount === 0) throw httpError('ไม่พบสินค้า', 404)

      await ensureWarehouseShelfExists(client, payload.wh_code, payload.shelf_code)

      await client.query(
        `INSERT INTO ic_inventory_detail
           (ic_code, purchase_point, minimum_qty, maximum_qty, start_sale_wh, start_sale_shelf)
         VALUES ($1::text,$2::numeric,$3::numeric,$4::numeric,$5::text,$6::text)
         ON CONFLICT (ic_code) DO UPDATE SET
           purchase_point = EXCLUDED.purchase_point,
           minimum_qty = EXCLUDED.minimum_qty,
           maximum_qty = EXCLUDED.maximum_qty,
           start_sale_wh = EXCLUDED.start_sale_wh,
           start_sale_shelf = EXCLUDED.start_sale_shelf`,
        [
          payload.code,
          payload.purchase_point,
          payload.minimum_qty,
          payload.maximum_qty,
          payload.wh_code,
          payload.shelf_code,
        ],
      )

      await ensureProductUnitUse(client, payload.code, payload.unit_standard)
      await ensureMainWarehouseShelf(client, payload.code, payload.wh_code, payload.shelf_code)
      await syncProductUnitType(client, payload.code)
    })

    res.json({ success: true, code: payload.code })
  } catch (err) {
    res.status(err.statusCode || 500).json({ error: err.message })
  }
})

module.exports = router
