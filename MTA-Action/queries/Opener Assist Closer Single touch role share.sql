SELECT
  channel,
  role_type,
  pct_value AS pct_role_share,
  touch_occurrences
FROM `action-dwh.attribution_model_ecom.mta_purchase_journey_reporting_long_20260528_20260610`
WHERE report_section = 'channel_role_share'
ORDER BY channel, role_type;
