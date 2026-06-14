-- Sawiday MTA daily spend input.
-- Uses the source table's DATE field named `date` and aggregates spend per day/channel.
-- This intentionally does NOT output a monthly spend table.

DECLARE start_date DATE DEFAULT DATE '2026-05-01';
DECLARE end_date   DATE DEFAULT DATE '2026-06-10';

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_spend_daily_20260501_20260610`
PARTITION BY date
CLUSTER BY channel_group
AS
SELECT
  date,
  COALESCE(default_channel_grouping, 'Unattributed') AS channel_group,
  SUM(COALESCE(costs, 0.0)) AS cost
FROM `action-dwh.mart_ga4.ga_marketing_reporting`
WHERE date BETWEEN start_date AND end_date
GROUP BY
  date,
  channel_group;
