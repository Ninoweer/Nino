SELECT
  channel,
  metric_value AS journey_position_index_0_to_100,
  avg_value AS avg_raw_position,
  journeys,
  touch_occurrences,
  orders,
  revenue
FROM `action-dwh.attribution_model_ecom.mta_purchase_journey_reporting_long_20260528_20260610`
WHERE report_section = 'channel_position_index'
ORDER BY journeys DESC;
