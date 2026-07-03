-- Purchase planning user permissions and shared cart state.
-- Stored in CRM DB because ownership and colors are CRM-user concepts.

CREATE TABLE IF NOT EXISTS public.crm_purchase_planning_user_setting (
  user_id integer PRIMARY KEY REFERENCES public.crm_users(id) ON DELETE CASCADE,
  can_access smallint NOT NULL DEFAULT 0,
  cart_color varchar(20) NOT NULL DEFAULT '#2563eb',
  remark varchar(255) NOT NULL DEFAULT '',
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT '',
  CONSTRAINT crm_purchase_planning_user_setting_access_chk CHECK (can_access IN (0, 1)),
  CONSTRAINT crm_purchase_planning_user_setting_color_chk CHECK (cart_color ~ '^#[0-9A-Fa-f]{6}$')
);

CREATE TABLE IF NOT EXISTS public.crm_purchase_planning_cart (
  id bigserial PRIMARY KEY,
  user_id integer NOT NULL REFERENCES public.crm_users(id) ON DELETE CASCADE,
  item_code varchar(25) NOT NULL,
  item_name varchar(255) NOT NULL DEFAULT '',
  ap_code varchar(25) NOT NULL,
  ap_name varchar(255) NOT NULL DEFAULT '',
  unit_code varchar(25) NOT NULL DEFAULT '',
  selected_unit varchar(25) NOT NULL DEFAULT '',
  unit_ratio numeric NOT NULL DEFAULT 1,
  unit_stand_value numeric NOT NULL DEFAULT 1,
  unit_divide_value numeric NOT NULL DEFAULT 1,
  base_qty numeric NOT NULL DEFAULT 0,
  qty numeric NOT NULL DEFAULT 0,
  price numeric NOT NULL DEFAULT 0,
  suggest_qty numeric NOT NULL DEFAULT 0,
  reference_unit_code varchar(25) NOT NULL DEFAULT '',
  reference_price numeric NOT NULL DEFAULT 0,
  status varchar(20) NOT NULL DEFAULT 'active',
  pr_doc_no varchar(50) NOT NULL DEFAULT '',
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT '',
  CONSTRAINT crm_purchase_planning_cart_status_chk CHECK (status IN ('active', 'created_pr', 'removed')),
  CONSTRAINT crm_purchase_planning_cart_qty_chk CHECK (qty >= 0 AND base_qty >= 0 AND price >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crm_purchase_planning_cart_active_item_ap
  ON public.crm_purchase_planning_cart (item_code, ap_code)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_crm_purchase_planning_cart_user_status
  ON public.crm_purchase_planning_cart (user_id, status);

CREATE INDEX IF NOT EXISTS idx_crm_purchase_planning_cart_item_ap_status
  ON public.crm_purchase_planning_cart (item_code, ap_code, status);

CREATE TABLE IF NOT EXISTS public.crm_purchase_planning_report_snapshot (
  query_key varchar(80) PRIMARY KEY,
  query_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  snapshot_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT ''
);
