-- Sawiday MTA sessions input: all sessions in the selected recent window.
-- No new-user / first-seen / lookback logic is applied here.
-- Last session day: 2026-06-10.
--
-- IMPORTANT:
-- Do not put ORDER BY in this CTAS. This table is partitioned by session_date.
-- Use ORDER BY only in downstream SELECT/export/validation queries.

DECLARE start_date DATE DEFAULT DATE '2026-05-01';
DECLARE end_date   DATE DEFAULT DATE '2026-06-10';

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_sessions_20260501_20260610`
PARTITION BY session_date
CLUSTER BY user_pseudo_id, ga_session_id
AS

WITH source_base AS (
  SELECT
    r.*
  FROM `action-dwh.mart_ga4.ga_ecom_reporting` r
  WHERE r.date BETWEEN start_date AND end_date
    AND r.user_pseudo_id IS NOT NULL
    AND r.session_id IS NOT NULL
),

session_level AS (
  SELECT
    r.date AS session_date,
    r.user_pseudo_id,
    CAST(r.session_id AS STRING) AS ga_session_id,

    -- If the source table is already session-grain, these preserve the session value.
    -- If duplicate rows exist for the same user/session/date, MAX avoids double-counting
    -- repeated session-level totals.
    MAX(COALESCE(r.total_transactions, 0)) AS transactions,
    MAX(COALESCE(r.revenue, 0.0)) AS ecommerce_purchase_revenue,

    ARRAY_AGG(r.default_channel_grouping IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS default_channel_grouping,
    ARRAY_AGG(r.session_campaign IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS session_campaign

  FROM source_base r
  GROUP BY
    session_date,
    user_pseudo_id,
    ga_session_id
)

SELECT
  s.session_date,
  s.user_pseudo_id,
  s.ga_session_id,

  CASE
    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'performance max'
      THEN 'Performance Max - Generic'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid search'
      AND (
        LOWER(COALESCE(s.session_campaign, '')) LIKE '%-brd%'
        OR LOWER(COALESCE(s.session_campaign, '')) = 'brd'
      )
      THEN 'Paid Search - Branded'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid search'
      AND (
        LOWER(COALESCE(s.session_campaign, '')) LIKE '%-gen%'
        OR LOWER(COALESCE(s.session_campaign, '')) = 'gen'
      )
      THEN 'Paid Search - Generic'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid search'
      THEN 'Paid Search - Unclassified'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid shopping'
      AND LOWER(COALESCE(s.session_campaign, '')) LIKE '%-brd%'
      THEN 'Paid Shopping - Branded'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid shopping'
      AND LOWER(COALESCE(s.session_campaign, '')) LIKE '%-gen%'
      THEN 'Paid Shopping - Generic'

    WHEN LOWER(COALESCE(s.default_channel_grouping, '')) = 'paid shopping'
      THEN 'Paid Shopping - Unclassified'

    ELSE COALESCE(s.default_channel_grouping, 'Unattributed')
  END AS traffic_channelgroup,

  CONCAT(s.user_pseudo_id, '#', s.ga_session_id) AS user_pseudo_session_id,

  s.transactions,
  s.ecommerce_purchase_revenue,

  CAST(NULL AS FLOAT64) AS ecommerce_tax_value,
  CAST(NULL AS INT64)   AS ecommerce_total_item_quantity,
  CAST(NULL AS FLOAT64) AS sum_of_revenue_exc_BTW,
  CAST(NULL AS FLOAT64) AS sum_of_revenue_inc_BTW

FROM session_level s;
