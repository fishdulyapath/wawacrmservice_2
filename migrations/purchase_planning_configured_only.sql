-- Purchase planning resolved settings: zero means "not configured".
-- Run on POSDB. Safe to re-run.

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
