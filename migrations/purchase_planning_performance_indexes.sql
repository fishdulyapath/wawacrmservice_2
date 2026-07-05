-- Performance indexes for purchase planning master/report screens.
-- Prefer running the companion script because it uses CREATE INDEX CONCURRENTLY.

CREATE INDEX IF NOT EXISTS idx_ic_trans_detail_pp_latest_receipt
  ON public.ic_trans_detail (item_code, cust_code, unit_code, doc_date DESC, doc_time DESC, doc_no DESC)
  WHERE trans_flag = 310 AND COALESCE(status, 0) = 0;

CREATE INDEX IF NOT EXISTS idx_ap_item_by_supplier_pp_ic_ap
  ON public.ap_item_by_supplier (ic_code, ap_code);

CREATE INDEX IF NOT EXISTS idx_ap_item_by_supplier_pp_ap_ic
  ON public.ap_item_by_supplier (ap_code, ic_code);
