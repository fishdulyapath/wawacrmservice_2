-- Purchase planning master settings for POSDB.
-- Safe to re-run. These tables extend SML master tables without altering core master tables.

CREATE TABLE IF NOT EXISTS public.purchase_planning_supplier_setting (
  ap_code varchar(25) NOT NULL,
  lead_time_days integer,
  late_buffer_days integer,
  wholesale_buffer_days integer,
  order_cycle_days integer,
  planning_enabled smallint NOT NULL DEFAULT 1,
  remark varchar(255) NOT NULL DEFAULT '',
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT '',
  CONSTRAINT purchase_planning_supplier_setting_pk PRIMARY KEY (ap_code),
  CONSTRAINT purchase_planning_supplier_setting_ap_fk FOREIGN KEY (ap_code) REFERENCES public.ap_supplier(code),
  CONSTRAINT purchase_planning_supplier_setting_days_chk CHECK (
    (lead_time_days IS NULL OR lead_time_days >= 0)
    AND (late_buffer_days IS NULL OR late_buffer_days >= 0)
    AND (wholesale_buffer_days IS NULL OR wholesale_buffer_days >= 0)
    AND (order_cycle_days IS NULL OR order_cycle_days >= 0)
  ),
  CONSTRAINT purchase_planning_supplier_setting_enabled_chk CHECK (planning_enabled IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_purchase_planning_supplier_setting_enabled
  ON public.purchase_planning_supplier_setting (planning_enabled);

CREATE TABLE IF NOT EXISTS public.purchase_planning_item_setting (
  ic_code varchar(25) NOT NULL,
  lead_time_days integer,
  late_buffer_days integer,
  wholesale_buffer_days integer,
  order_cycle_days integer,
  planning_enabled smallint NOT NULL DEFAULT 1,
  remark varchar(255) NOT NULL DEFAULT '',
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT '',
  CONSTRAINT purchase_planning_item_setting_pk PRIMARY KEY (ic_code),
  CONSTRAINT purchase_planning_item_setting_ic_fk FOREIGN KEY (ic_code) REFERENCES public.ic_inventory(code),
  CONSTRAINT purchase_planning_item_setting_days_chk CHECK (
    (lead_time_days IS NULL OR lead_time_days >= 0)
    AND (late_buffer_days IS NULL OR late_buffer_days >= 0)
    AND (wholesale_buffer_days IS NULL OR wholesale_buffer_days >= 0)
    AND (order_cycle_days IS NULL OR order_cycle_days >= 0)
  ),
  CONSTRAINT purchase_planning_item_setting_enabled_chk CHECK (planning_enabled IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_purchase_planning_item_setting_enabled
  ON public.purchase_planning_item_setting (planning_enabled);

CREATE TABLE IF NOT EXISTS public.purchase_planning_item_supplier_setting (
  ic_code varchar(25) NOT NULL,
  ap_code varchar(25) NOT NULL,
  lead_time_days integer,
  late_buffer_days integer,
  wholesale_buffer_days integer,
  order_cycle_days integer,
  min_order_qty numeric NOT NULL DEFAULT 0,
  pack_size numeric NOT NULL DEFAULT 1,
  purchase_unit_code varchar(25) NOT NULL DEFAULT '',
  planning_enabled smallint NOT NULL DEFAULT 1,
  is_preferred smallint NOT NULL DEFAULT 0,
  remark varchar(255) NOT NULL DEFAULT '',
  create_datetime timestamp without time zone NOT NULL DEFAULT now(),
  last_update_date_time timestamp without time zone NOT NULL DEFAULT now(),
  create_code varchar(50) NOT NULL DEFAULT '',
  last_update_code varchar(50) NOT NULL DEFAULT '',
  CONSTRAINT purchase_planning_item_supplier_setting_pk PRIMARY KEY (ic_code, ap_code),
  CONSTRAINT purchase_planning_item_supplier_setting_ic_fk FOREIGN KEY (ic_code) REFERENCES public.ic_inventory(code),
  CONSTRAINT purchase_planning_item_supplier_setting_ap_fk FOREIGN KEY (ap_code) REFERENCES public.ap_supplier(code),
  CONSTRAINT purchase_planning_item_supplier_setting_days_chk CHECK (
    (lead_time_days IS NULL OR lead_time_days >= 0)
    AND (late_buffer_days IS NULL OR late_buffer_days >= 0)
    AND (wholesale_buffer_days IS NULL OR wholesale_buffer_days >= 0)
    AND (order_cycle_days IS NULL OR order_cycle_days >= 0)
  ),
  CONSTRAINT purchase_planning_item_supplier_setting_qty_chk CHECK (min_order_qty >= 0 AND pack_size > 0),
  CONSTRAINT purchase_planning_item_supplier_setting_flags_chk CHECK (planning_enabled IN (0, 1) AND is_preferred IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_purchase_planning_item_supplier_setting_ap
  ON public.purchase_planning_item_supplier_setting (ap_code);

CREATE INDEX IF NOT EXISTS idx_purchase_planning_item_supplier_setting_enabled
  ON public.purchase_planning_item_supplier_setting (planning_enabled);

CREATE OR REPLACE VIEW public.purchase_planning_item_supplier_resolved AS
SELECT
  link.ic_code,
  link.ap_code,
  COALESCE(NULLIF(link_setting.lead_time_days, 0), NULLIF(item_setting.lead_time_days, 0), NULLIF(supplier_setting.lead_time_days, 0), 0) AS lead_time_days,
  COALESCE(NULLIF(link_setting.late_buffer_days, 0), NULLIF(item_setting.late_buffer_days, 0), NULLIF(supplier_setting.late_buffer_days, 0), 0) AS late_buffer_days,
  COALESCE(NULLIF(link_setting.wholesale_buffer_days, 0), NULLIF(item_setting.wholesale_buffer_days, 0), NULLIF(supplier_setting.wholesale_buffer_days, 0), 0) AS wholesale_buffer_days,
  COALESCE(NULLIF(link_setting.order_cycle_days, 0), NULLIF(item_setting.order_cycle_days, 0), NULLIF(supplier_setting.order_cycle_days, 0), 0) AS order_cycle_days,
  COALESCE(link_setting.min_order_qty, 0) AS min_order_qty,
  COALESCE(NULLIF(link_setting.pack_size, 0), 1) AS pack_size,
  COALESCE(link_setting.purchase_unit_code, '') AS purchase_unit_code,
  COALESCE(link_setting.is_preferred, 0) AS is_preferred,
  COALESCE(link_setting.planning_enabled, item_setting.planning_enabled, supplier_setting.planning_enabled, 1) AS planning_enabled
FROM public.ap_item_by_supplier link
LEFT JOIN public.purchase_planning_item_supplier_setting link_setting
  ON link_setting.ic_code = link.ic_code
 AND link_setting.ap_code = link.ap_code
LEFT JOIN public.purchase_planning_item_setting item_setting
  ON item_setting.ic_code = link.ic_code
LEFT JOIN public.purchase_planning_supplier_setting supplier_setting
  ON supplier_setting.ap_code = link.ap_code;
