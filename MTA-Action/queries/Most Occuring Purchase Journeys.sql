SELECT
  row_rank,
  channel_path,
  journeys AS path_count,
  orders AS path_orders,
  revenue AS path_revenue
FROM `action-dwh.attribution_model_ecom.mta_purchase_journey_reporting_long_20260528_20260610`
WHERE report_section = 'top_purchase_journey_paths'
ORDER BY row_rank;
