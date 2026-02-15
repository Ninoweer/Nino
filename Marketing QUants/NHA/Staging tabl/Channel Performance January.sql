-- NHA | Session-level channel performance | January 2026
-- 1 row = 1 session
-- Date range is set MANUALLY via _TABLE_SUFFIX bounds (no DECLARE, no CURRENT_DATE()).

WITH sessions_raw AS (
  -- NL property (analytics_265106443)
  SELECT
    'NL' AS country,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_time,
    DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Europe/Amsterdam')) AS session_date,

    NULLIF(
      COALESCE(
        session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_default_channel_grouping')
      ),
      ''
    ) AS raw_channel_grouping,

    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_source'),
      traffic_source.source
    ) AS source,

    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_medium'),
      traffic_source.medium
    ) AS medium,

    device.category AS device,

    event_timestamp
  FROM `nha-analytics.analytics_265106443.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
    AND event_name = 'session_start'

  UNION ALL

  -- BE property (analytics_320053540)
  SELECT
    'BE' AS country,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_time,
    DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Europe/Amsterdam')) AS session_date,

    NULLIF(
      COALESCE(
        session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_default_channel_grouping')
      ),
      ''
    ) AS raw_channel_grouping,

    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_source'),
      traffic_source.source
    ) AS source,

    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_medium'),
      traffic_source.medium
    ) AS medium,

    device.category AS device,

    event_timestamp
  FROM `nha-analytics.analytics_320053540.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
    AND event_name = 'session_start'
),

sessions AS (
  -- Defensive dedupe: keep earliest session_start per (country, user_pseudo_id, session_id)
  SELECT
    country,
    user_pseudo_id,
    session_id,
    session_start_time,
    session_date,
    raw_channel_grouping,
    source,
    medium,
    device
  FROM sessions_raw
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, user_pseudo_id, session_id
    ORDER BY event_timestamp ASC
  ) = 1
),

purchases_raw AS (
  SELECT
    'NL' AS country,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Europe/Amsterdam')) AS purchase_date,
    ecommerce.transaction_id AS transaction_id,
    COALESCE(
      ecommerce.purchase_revenue,
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'),
      (SELECT value.float_value  FROM UNNEST(event_params) WHERE key = 'value'),
      SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS FLOAT64)
    ) AS revenue_value
  FROM `nha-analytics.analytics_265106443.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
    AND event_name = 'purchase'

  UNION ALL

  SELECT
    'BE' AS country,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    DATE(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Europe/Amsterdam')) AS purchase_date,
    ecommerce.transaction_id AS transaction_id,
    COALESCE(
      ecommerce.purchase_revenue,
      (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'),
      (SELECT value.float_value  FROM UNNEST(event_params) WHERE key = 'value'),
      SAFE_CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS FLOAT64)
    ) AS revenue_value
  FROM `nha-analytics.analytics_320053540.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260131'
    AND event_name = 'purchase'
),

purchase_agg AS (
  SELECT
    country,
    user_pseudo_id,
    session_id,
    MIN(purchase_date) AS purchase_date,
    COUNT(1) AS purchase_events,
    COUNT(DISTINCT transaction_id) AS distinct_transaction_ids,
    SUM(SAFE_CAST(revenue_value AS FLOAT64)) AS sum_revenue
  FROM purchases_raw
  GROUP BY 1, 2, 3
),

final AS (
  SELECT
    s.country,
    s.user_pseudo_id,
    s.session_id,

    -- Extra safe unique key for downstream pipelines (prevents NL/BE collisions)
    CONCAT(s.country, '|', s.user_pseudo_id, '|', CAST(s.session_id AS STRING)) AS session_key,

    s.session_start_time,
    s.session_date,
    s.device,
    s.source,
    s.medium,

    -- Final channel grouping (based on your existing logic)
    CASE
      WHEN s.raw_channel_grouping IS NOT NULL
           AND LOWER(s.raw_channel_grouping) NOT IN ('affiliates')
        THEN s.raw_channel_grouping

      -- Paid Search
      WHEN LOWER(s.source) IN ('google', 'bing') AND LOWER(s.medium) IN ('cpc', 'ppc') THEN 'Paid Search'

      -- Organic Search
      WHEN LOWER(s.source) IN ('google', 'bing') AND LOWER(s.medium) = 'organic' THEN 'Organic Search'

      -- Direct
      WHEN LOWER(s.source) IN ('(direct)', 'direct') AND LOWER(s.medium) IN ('(none)', 'not set', '') THEN 'Direct'

      -- Email
      WHEN LOWER(s.medium) = 'email' OR LOWER(s.source) = 'canopydeploy' THEN 'Email'

      -- Referral
      WHEN LOWER(s.medium) = 'referral' OR LOWER(s.source) IN ('chatgpt.com') THEN 'Referral'

      -- Organic Social
      WHEN LOWER(s.medium) IN ('meta', 'social') OR LOWER(s.source) IN ('facebook', 'instagram', 'social') THEN 'Organic Social'

      -- Affiliates (strict match)
      WHEN LOWER(s.medium) = 'affiliate'
           AND LOWER(s.source) IN ('daisycon', 'tradetracker', 'awin', 'roc') THEN 'Affiliates'

      -- Fallback
      ELSE 'Unassigned'
    END AS channel_grouping,

    -- Purchase / value signals (session-level)
    p.purchase_date,
    IFNULL(p.purchase_events, 0) AS purchase_events,

    -- Transactions: prefer distinct transaction_id when available; otherwise fall back to purchase_events
    IF(
      IFNULL(p.distinct_transaction_ids, 0) > 0,
      p.distinct_transaction_ids,
      IFNULL(p.purchase_events, 0)
    ) AS transactions,

    IFNULL(p.sum_revenue, 0.0) AS sum_revenue

  FROM sessions s
  LEFT JOIN purchase_agg p
    ON s.country        = p.country
   AND s.user_pseudo_id = p.user_pseudo_id
   AND s.session_id     = p.session_id
)

SELECT *
FROM final
ORDER BY session_date, session_start_time;
