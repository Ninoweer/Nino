SELECT
  channel_a,
  channel_b,
  journeys,
  orders,
  revenue,
  lift_value AS revenue_lift_vs_independence
FROM `action-dwh.attribution_model_ecom.mta_purchase_journey_reporting_long_20260528_20260610`
WHERE report_section = 'channel_interaction_lift'
  AND metric_name = 'revenue_lift_vs_independence'
ORDER BY revenue DESC;
