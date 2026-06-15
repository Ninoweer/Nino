-- Sawiday journey complexity distribution table.
--
-- Single table for two distributions:
--   1. unique_channels_per_journey
--   2. sessions_per_journey
--
-- Looker setup:
--   X-axis: x_value
--   Breakdown/series: distribution_metric
--   Y-axis: journeys
--   Optional density/share: pct_journeys_percent
--
-- Important:
--   Do NOT add ORDER BY to the final CTAS SELECT. This table is clustered.
--   Use ORDER BY when querying the completed table.

DECLARE reporting_start_date DATE DEFAULT DATE '2026-05-28';
DECLARE reporting_end_date   DATE DEFAULT DATE '2026-06-10';
DECLARE lookback_days        INT64 DEFAULT 7;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_journey_complexity_distribution_20260528_20260610`
CLUSTER BY distribution_metric, x_value
AS

WITH sessions AS (
  SELECT
    session_date,
    user_pseudo_id,
    ga_session_id,
    traffic_channelgroup,
    COALESCE(transactions, 0) AS transactions,
    COALESCE(ecommerce_purchase_revenue, 0.0) AS ecommerce_purchase_revenue,
    SAFE_CAST(ga_session_id AS INT64) AS ga_session_id_int
  FROM `action-dwh.attribution_model_ecom.mta_sessions_20260501_20260610`
  WHERE session_date BETWEEN DATE_SUB(reporting_start_date, INTERVAL lookback_days DAY)
                         AND reporting_end_date
    AND user_pseudo_id IS NOT NULL
    AND ga_session_id IS NOT NULL
    AND traffic_channelgroup IS NOT NULL
),

conversions AS (
  SELECT
    user_pseudo_id,
    session_date AS conversion_date,
    ga_session_id AS conversion_session_id,
    ga_session_id_int AS conversion_session_id_int,
    CONCAT(user_pseudo_id, '#', ga_session_id, '#', CAST(session_date AS STRING)) AS journey_id,
    transactions AS conversion_orders,
    ecommerce_purchase_revenue AS conversion_revenue
  FROM sessions
  WHERE session_date BETWEEN reporting_start_date AND reporting_end_date
    AND transactions > 0
),

touch_raw AS (
  SELECT
    c.journey_id,
    c.conversion_orders,
    c.conversion_revenue,
    s.session_date,
    s.ga_session_id,
    s.ga_session_id_int,
    s.traffic_channelgroup AS channel
  FROM conversions c
  JOIN sessions s
    ON s.user_pseudo_id = c.user_pseudo_id
   AND s.session_date BETWEEN DATE_SUB(c.conversion_date, INTERVAL lookback_days DAY)
                          AND c.conversion_date
   AND (
     s.session_date < c.conversion_date
     OR (
       s.session_date = c.conversion_date
       AND COALESCE(s.ga_session_id_int, -1)
           <= COALESCE(c.conversion_session_id_int, 9223372036854775807)
     )
   )
),

journey_metrics AS (
  SELECT
    journey_id,
    COUNT(*) AS session_count,
    COUNT(DISTINCT channel) AS unique_channel_count,
    MAX(conversion_orders) AS conversion_orders,
    MAX(conversion_revenue) AS conversion_revenue
  FROM touch_raw
  GROUP BY journey_id
),

unique_channel_distribution AS (
  SELECT
    'unique_channels_per_journey' AS distribution_metric,
    unique_channel_count AS x_value,
    COUNT(*) AS journeys,
    SUM(conversion_orders) AS orders,
    SUM(conversion_revenue) AS revenue
  FROM journey_metrics
  GROUP BY x_value
),

session_count_distribution AS (
  SELECT
    'sessions_per_journey' AS distribution_metric,
    session_count AS x_value,
    COUNT(*) AS journeys,
    SUM(conversion_orders) AS orders,
    SUM(conversion_revenue) AS revenue
  FROM journey_metrics
  GROUP BY x_value
),

combined AS (
  SELECT * FROM unique_channel_distribution
  UNION ALL
  SELECT * FROM session_count_distribution
)

SELECT
  distribution_metric,
  x_value,
  journeys,
  SAFE_DIVIDE(
    journeys,
    SUM(journeys) OVER (PARTITION BY distribution_metric)
  ) AS pct_journeys,
  ROUND(
    100 * SAFE_DIVIDE(
      journeys,
      SUM(journeys) OVER (PARTITION BY distribution_metric)
    ),
    2
  ) AS pct_journeys_percent,
  orders,
  revenue
FROM combined;
