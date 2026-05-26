-- Fleet Delivery Dashboard Tables
-- ใช้ CRM PostgreSQL (wawa.iszai.com:6543/crm)
-- READ-ONLY mirror จาก Google Sheets — ห้ามแก้ไขข้อมูลในนี้จาก AppSheet side

-- ----------------------------------------------------------------
-- fleet_users — จาก sheet "user"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_users (
  user_id       TEXT PRIMARY KEY,
  username      TEXT,
  name          TEXT,
  level_user_id TEXT,
  phone_number  TEXT,
  image_profile TEXT,
  location_now  TEXT,
  language      TEXT,
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ,
  synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at    TIMESTAMPTZ
);

-- ----------------------------------------------------------------
-- fleet_cars — จาก sheet "car"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_cars (
  car_id        TEXT PRIMARY KEY,
  car_name      TEXT,
  license_plate TEXT,
  car_type      TEXT,
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ,
  synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at    TIMESTAMPTZ
);

-- ----------------------------------------------------------------
-- fleet_stores — จาก sheet "store"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_stores (
  store_id      TEXT PRIMARY KEY,
  store_name    TEXT,
  address       TEXT,
  phone         TEXT,
  location      TEXT,
  zone          TEXT,
  is_active     BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ,
  synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at    TIMESTAMPTZ
);

-- ----------------------------------------------------------------
-- fleet_group_stores — จาก sheet "name_car_release" (เส้นทาง/กลุ่มร้าน)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_group_stores (
  group_store_id  TEXT PRIMARY KEY,
  group_name      TEXT,
  description     TEXT,
  created_at      TIMESTAMPTZ,
  synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at      TIMESTAMPTZ
);

-- ----------------------------------------------------------------
-- fleet_car_releases — จาก sheet "car_release" (trip หลัก)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_car_releases (
  car_release_id        TEXT PRIMARY KEY,
  car_id                TEXT,
  user_id               TEXT,
  name_car_release_id   TEXT,
  group_store_id        TEXT,   -- join key! ใช้นี้ ไม่ใช่ car_release_id ใน list_store
  mileage               NUMERIC,
  mileage_return        NUMERIC,
  description           TEXT,
  total_amount          NUMERIC,
  total_number_of_bills INTEGER,
  accounting_status     TEXT,
  car_release_image     TEXT,
  car_return_image      TEXT,
  image_mileage         TEXT,
  image_front           TEXT,
  image_around_1        TEXT,
  image_around_2        TEXT,
  image_around_3        TEXT,
  image_around_4        TEXT,
  car_return_id         TEXT,
  return_image_mileage  TEXT,
  return_image_front    TEXT,
  return_image_around_1 TEXT,
  return_image_around_2 TEXT,
  return_image_around_3 TEXT,
  return_image_around_4 TEXT,
  trip_date             DATE,
  created_at            TIMESTAMPTZ,
  synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at            TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_user_id       ON fleet_car_releases(user_id);
CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_car_id        ON fleet_car_releases(car_id);
CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_group_store   ON fleet_car_releases(group_store_id);
CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_created_at    ON fleet_car_releases(created_at);
CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_trip_date     ON fleet_car_releases(trip_date);

-- ----------------------------------------------------------------
-- fleet_list_stores — จาก sheet "list_store" (junction: trip ↔ store)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_list_stores (
  list_id         TEXT PRIMARY KEY,
  group_store_id  TEXT,   -- FK ไป fleet_car_releases.group_store_id
  store_id        TEXT,
  sequence_no     INTEGER,
  created_at      TIMESTAMPTZ,
  synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_list_stores_group_store ON fleet_list_stores(group_store_id);
CREATE INDEX IF NOT EXISTS idx_fleet_list_stores_store_id    ON fleet_list_stores(store_id);

-- ----------------------------------------------------------------
-- fleet_check_ins — จาก sheet "check_in"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_check_ins (
  check_in_id         TEXT PRIMARY KEY,
  list_id             TEXT,
  date_time_check_in  TIMESTAMPTZ,
  image_check_in      TEXT,
  latitude            NUMERIC,
  longitude           NUMERIC,
  created_at          TIMESTAMPTZ,
  synced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_check_ins_list_id          ON fleet_check_ins(list_id);
CREATE INDEX IF NOT EXISTS idx_fleet_check_ins_date_time        ON fleet_check_ins(date_time_check_in);

-- ----------------------------------------------------------------
-- fleet_check_outs — จาก sheet "check_out"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_check_outs (
  check_out_id          TEXT PRIMARY KEY,
  list_id               TEXT,   -- join ไป fleet_list_stores.list_id
  date_time_check_out   TIMESTAMPTZ,
  image_bill            TEXT,
  payment_id            TEXT,
  cash                  NUMERIC,
  transfer              NUMERIC,
  amount                NUMERIC,
  created_at            TIMESTAMPTZ,
  synced_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at            TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_check_outs_list_id         ON fleet_check_outs(list_id);
CREATE INDEX IF NOT EXISTS idx_fleet_check_outs_date_time       ON fleet_check_outs(date_time_check_out);

-- ----------------------------------------------------------------
-- fleet_check_out_images — จาก sheet "image_check_out" (หลายรูปต่อ check_out)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_check_out_images (
  image_check_out_id TEXT PRIMARY KEY,
  check_out_id       TEXT NOT NULL,
  image_path         TEXT,
  note               TEXT,
  created_at         TIMESTAMPTZ,
  synced_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_check_out_images_check_out_id ON fleet_check_out_images(check_out_id);
CREATE INDEX IF NOT EXISTS idx_fleet_check_out_images_created_at   ON fleet_check_out_images(created_at);

-- ----------------------------------------------------------------
-- fleet_problems — จาก sheet "problem"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_problems (
  problem_id    TEXT PRIMARY KEY,
  list_id       TEXT,
  problem_type  TEXT,
  description   TEXT,
  image_problem TEXT,
  is_resolved   BOOLEAN DEFAULT FALSE,
  created_at    TIMESTAMPTZ,
  synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_problems_list_id    ON fleet_problems(list_id);
CREATE INDEX IF NOT EXISTS idx_fleet_problems_created_at ON fleet_problems(created_at);

-- ----------------------------------------------------------------
-- fleet_problem_images — จาก sheet "image_problem" (หลายรูปต่อ problem)
-- ----------------------------------------------------------------
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

-- ----------------------------------------------------------------
-- fleet_payments — from sheet "payment"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_payments (
  payment_id   TEXT PRIMARY KEY,
  payment_name TEXT,
  created_at   TIMESTAMPTZ,
  synced_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_payments_name ON fleet_payments(payment_name);

-- ----------------------------------------------------------------
-- fleet_visits — from sheet "visit"
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_visits (
  visit_id   TEXT PRIMARY KEY,
  visit_name TEXT,
  created_by TEXT,
  created_at TIMESTAMPTZ,
  synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_visits_name ON fleet_visits(visit_name);

-- ----------------------------------------------------------------
-- fleet_return_products — from sheet "return_product"
-- ----------------------------------------------------------------
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

-- ----------------------------------------------------------------
-- fleet_return_documents — from sheet "return_document"
-- ----------------------------------------------------------------
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
CREATE INDEX IF NOT EXISTS idx_fleet_return_documents_created_at     ON fleet_return_documents(created_at);

-- ----------------------------------------------------------------
-- fleet_sync_logs — ติดตามสถานะการ sync ทุกครั้ง
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_sync_logs (
  id            SERIAL PRIMARY KEY,
  sheet_name    TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  rows_synced   INTEGER,
  rows_failed   INTEGER,
  duration_ms   INTEGER,
  error_message TEXT,
  started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fleet_sync_logs_sheet_name  ON fleet_sync_logs(sheet_name);
CREATE INDEX IF NOT EXISTS idx_fleet_sync_logs_started_at  ON fleet_sync_logs(started_at DESC);

-- ----------------------------------------------------------------
-- Migration: เพิ่ม columns ที่ขาดจาก Google Sheet
-- ----------------------------------------------------------------

-- fleet_car_releases: รหัส trip ที่อ่านได้
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS car_release_code TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS car_release_image TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS car_return_image TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_mileage TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_front TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_around_1 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_around_2 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_around_3 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS image_around_4 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS car_return_id TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_mileage TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_front TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_around_1 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_around_2 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_around_3 TEXT;
ALTER TABLE fleet_car_releases ADD COLUMN IF NOT EXISTS return_image_around_4 TEXT;

-- fleet_check_outs: ข้อมูล visit และ payment reconcile
ALTER TABLE fleet_check_outs ADD COLUMN IF NOT EXISTS transfer_according NUMERIC;
ALTER TABLE fleet_check_outs ADD COLUMN IF NOT EXISTS visit_customer TEXT;
ALTER TABLE fleet_check_outs ADD COLUMN IF NOT EXISTS visit TEXT;
ALTER TABLE fleet_check_outs ADD COLUMN IF NOT EXISTS visit_note TEXT;

-- fleet_list_stores: สถานะการส่งและเลข order
ALTER TABLE fleet_list_stores ADD COLUMN IF NOT EXISTS off_site BOOLEAN;
ALTER TABLE fleet_list_stores ADD COLUMN IF NOT EXISTS bypass BOOLEAN;
ALTER TABLE fleet_list_stores ADD COLUMN IF NOT EXISTS data_store_no TEXT;
ALTER TABLE fleet_list_stores ADD COLUMN IF NOT EXISTS store_name_result TEXT;

-- fleet_cars: ข้อมูลรถแยกชัดเจน
ALTER TABLE fleet_cars ADD COLUMN IF NOT EXISTS brand TEXT;
ALTER TABLE fleet_cars ADD COLUMN IF NOT EXISTS model TEXT;
ALTER TABLE fleet_cars ADD COLUMN IF NOT EXISTS sub_model TEXT;
ALTER TABLE fleet_cars ADD COLUMN IF NOT EXISTS year INTEGER;

-- fleet_problems: categories จาก AppSheet
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS normal_bill TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS normal_bill_note TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS edit_bill TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS edit_bill_note TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS product_swap TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS product_swap_note TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS out_of_stock TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS out_of_stock_note TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS overstock TEXT;
ALTER TABLE fleet_problems ADD COLUMN IF NOT EXISTS overstock_note TEXT;

-- fleet_problem_images: รูปปัญหาแยก sheet ที่ผูกด้วย problem_id
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

-- ----------------------------------------------------------------
-- Store report performance indexes
-- ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fleet_list_stores_store_group ON fleet_list_stores(store_id, group_store_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_list_stores_store_created ON fleet_list_stores(store_id, created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_list_stores_store_data_no ON fleet_list_stores(store_id, data_store_no) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_check_outs_list_date ON fleet_check_outs(list_id, date_time_check_out DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_check_ins_list_date ON fleet_check_ins(list_id, date_time_check_in DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_problems_list_created ON fleet_problems(list_id, created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_check_out_images_check_out_created ON fleet_check_out_images(check_out_id, created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_problem_images_problem_created ON fleet_problem_images(problem_id, created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_fleet_car_releases_group_date ON fleet_car_releases(group_store_id, trip_date DESC) WHERE deleted_at IS NULL;
