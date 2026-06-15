-- Sawiday NBA / next-channel probability table.
--
-- Question answered:
--   For each current channel, what % of observed next-channel moves go to each
--   other channel in converting journeys?
--
-- Important:
--   - PURCHASE is excluded.
--   - START is excluded.
--   - Same-channel transitions are excluded.
--   - Percentages are calculated in BigQuery, not Looker Studio.
--   - Percentages sum to 100% per current_channel, for current_channels that
--     have at least one observed next-channel transition.
--
-- Source:
--   action-dwh.attribution_model_ecom.mta_sessions_20260501_20260610
--
-- Reporting period:
--   2026-05-28 through 2026-06-10
--
-- Lookback:
--   7 days

DECLARE reporting_start_date DATE DEFAULT DATE '2026-05-28';
DECLARE reporting_end_date   DATE DEFAULT DATE '2026-06-10';
DECLARE lookback_days        INT64 DEFAULT 7;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_next_channel_probability_20260528_20260610`
CLUSTER BY current_channel, next_channel
AS

WITH sessions AS (
  SELECT
    session_date,
    user_pseudo_id,
    CAST(ga_session_id AS STRING) AS ga_session_id,
    user_pseudo_session_id,
    COALESCE(traffic_channelgroup, 'Unattributed') AS channel,
    COALESCE(transactions, 0) AS transactions,
    COALESCE(ecommerce_purchase_revenue, 0.0) AS revenue,
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
    ga_session_id AS conversion_ga_session_id,
    ga_session_id_int AS conversion_ga_session_id_int,
    user_pseudo_session_id AS conversion_user_pseudo_session_id,
    CONCAT(
      user_pseudo_id,
      '#',
      ga_session_id,
      '#',
      CAST(session_date AS STRING),
      '#',
      user_pseudo_session_id
    ) AS journey_id,
    transactions AS conversion_orders,
    revenue AS conversion_revenue
  FROM sessions
  WHERE session_date BETWEEN reporting_start_date AND reporting_end_date
    AND transactions > 0
),

journey_sessions AS (
  SELECT
    c.journey_id,
    c.user_pseudo_id,
    c.conversion_date,
    c.conversion_ga_session_id,
    c.conversion_ga_session_id_int,
    c.conversion_user_pseudo_session_id,
    c.conversion_orders,
    c.conversion_revenue,

    s.session_date,
    s.ga_session_id,
    s.ga_session_id_int,
    s.user_pseudo_session_id,
    s.channel
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
           <= COALESCE(c.conversion_ga_session_id_int, 9223372036854775807)
     )
   )
),

ordered_touchpoints AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY journey_id
      ORDER BY
        session_date,
        COALESCE(ga_session_id_int, 9223372036854775807),
        ga_session_id,
        user_pseudo_session_id
    ) AS raw_touchpoint_position
  FROM journey_sessions
),

-- Collapse consecutive identical channels.
-- Example:
--   Direct > Direct > Paid Search > Paid Search > Email
-- becomes:
--   Direct > Paid Search > Email
--
-- This avoids inflated self-channel repetition and makes "next channel" mean
-- the next different channel.
lagged_touchpoints AS (
  SELECT
    *,
    LAG(channel) OVER (
      PARTITION BY journey_id
      ORDER BY raw_touchpoint_position
    ) AS previous_channel
  FROM ordered_touchpoints
),

collapsed_touchpoints AS (
  SELECT
    *
  FROM lagged_touchpoints
  WHERE previous_channel IS NULL
     OR channel != previous_channel
),

positioned_touchpoints AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY journey_id
      ORDER BY raw_touchpoint_position
    ) AS channel_position
  FROM collapsed_touchpoints
),

channel_to_channel_edges AS (
  SELECT
    journey_id,
    channel AS current_channel,
    LEAD(channel) OVER (
      PARTITION BY journey_id
      ORDER BY channel_position
    ) AS next_channel,
    conversion_orders,
    conversion_revenue
  FROM positioned_touchpoints
),

valid_channel_to_channel_edges AS (
  SELECT
    journey_id,
    current_channel,
    next_channel,
    conversion_orders,
    conversion_revenue
  FROM channel_to_channel_edges
  WHERE current_channel IS NOT NULL
    AND next_channel IS NOT NULL

    -- Exclude synthetic nodes entirely.
    AND current_channel NOT IN ('START', 'PURCHASE')
    AND next_channel NOT IN ('START', 'PURCHASE')

    -- Exclude same-channel transitions.
    -- Consecutive repeats were already collapsed, but this keeps the rule explicit.
    AND current_channel != next_channel
),

transition_counts AS (
  SELECT
    current_channel,
    next_channel,
    COUNT(*) AS transition_count,
    COUNT(DISTINCT journey_id) AS journeys,
    SUM(conversion_orders) AS orders,
    SUM(conversion_revenue) AS revenue
  FROM valid_channel_to_channel_edges
  GROUP BY
    current_channel,
    next_channel
),

transition_percentages AS (
  SELECT
    current_channel,
    next_channel,
    transition_count,
    journeys,
    orders,
    revenue,

    SUM(transition_count) OVER (
      PARTITION BY current_channel
    ) AS total_next_channel_transitions_from_current,

    SAFE_DIVIDE(
      transition_count,
      SUM(transition_count) OVER (PARTITION BY current_channel)
    ) AS pct_next_channel,

    ROUND(
      100 * SAFE_DIVIDE(
        transition_count,
        SUM(transition_count) OVER (PARTITION BY current_channel)
      ),
      2
    ) AS pct_next_channel_percent
  FROM transition_counts
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY current_channel
      ORDER BY
        transition_count DESC,
        revenue DESC,
        orders DESC,
        next_channel ASC
    ) AS next_channel_rank
  FROM transition_percentages
),

final AS (
  SELECT
    current_channel,
    next_channel,

    MAX(IF(next_channel_rank = 1, next_channel, NULL)) OVER (
      PARTITION BY current_channel
    ) AS most_likely_next_channel,

    next_channel_rank,
    next_channel_rank = 1 AS is_most_likely_next_channel,

    transition_count,
    total_next_channel_transitions_from_current,
    pct_next_channel,
    pct_next_channel_percent,

    journeys,
    orders,
    revenue,

    CONCAT(
      'After ',
      current_channel,
      ', ',
      CAST(pct_next_channel_percent AS STRING),
      '% of observed next-channel moves go to ',
      next_channel,
      '.'
    ) AS interpretation_text
  FROM ranked
)

SELECT *
FROM final;
