-- Sawiday observed next-channel probability table, excluding PURCHASE.
--
-- Question answered:
--   If the current channel is X, what other channel is most likely to come next
--   in converting journeys?
--
-- Output shape:
--   One row per current_channel x next_channel pair, excluding same-channel and PURCHASE.
--   Includes zero-probability pairs so Looker can show a complete channel-by-channel matrix.
--
-- Interpretation:
--   Descriptive observed next-channel behavior in converting journeys.
--   Not causal optimization and not budget advice.

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

touch_ordered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY journey_id
      ORDER BY session_date, COALESCE(ga_session_id_int, 9223372036854775807), ga_session_id
    ) AS raw_position
  FROM touch_raw
),

-- Collapse consecutive identical channels so repeated same-channel sessions do not
-- inflate transition probabilities.
lagged AS (
  SELECT
    *,
    LAG(channel) OVER (PARTITION BY journey_id ORDER BY raw_position) AS previous_channel
  FROM touch_ordered
),

collapsed AS (
  SELECT *
  FROM lagged
  WHERE previous_channel IS NULL OR channel != previous_channel
),

positioned AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY journey_id ORDER BY raw_position) AS step_position
  FROM collapsed
),

all_channels AS (
  SELECT DISTINCT channel
  FROM positioned
),

all_channel_pairs AS (
  SELECT
    c1.channel AS current_channel,
    c2.channel AS next_channel
  FROM all_channels c1
  CROSS JOIN all_channels c2
  WHERE c1.channel != c2.channel
),

observed_edges AS (
  SELECT
    journey_id,
    channel AS current_channel,
    LEAD(channel) OVER (PARTITION BY journey_id ORDER BY step_position) AS next_channel,
    conversion_orders,
    conversion_revenue
  FROM positioned
),

valid_observed_edges AS (
  SELECT
    journey_id,
    current_channel,
    next_channel,
    conversion_orders,
    conversion_revenue
  FROM observed_edges
  WHERE next_channel IS NOT NULL
    AND current_channel IS NOT NULL
    AND next_channel IS NOT NULL
    AND current_channel != next_channel
    AND current_channel NOT IN ('START', 'PURCHASE')
    AND next_channel NOT IN ('START', 'PURCHASE')
),

transition_counts AS (
  SELECT
    current_channel,
    next_channel,
    COUNT(*) AS transition_count,
    COUNT(DISTINCT journey_id) AS journeys,
    SUM(conversion_orders) AS orders,
    SUM(conversion_revenue) AS revenue
  FROM valid_observed_edges
  GROUP BY current_channel, next_channel
),

complete_matrix AS (
  SELECT
    p.current_channel,
    p.next_channel,
    COALESCE(t.transition_count, 0) AS transition_count,
    COALESCE(t.journeys, 0) AS journeys,
    COALESCE(t.orders, 0) AS orders,
    COALESCE(t.revenue, 0.0) AS revenue
  FROM all_channel_pairs p
  LEFT JOIN transition_counts t
    ON t.current_channel = p.current_channel
   AND t.next_channel = p.next_channel
),

scored AS (
  SELECT
    *,
    SUM(transition_count) OVER (PARTITION BY current_channel) AS total_next_channel_transitions_from_current,
    COALESCE(
      SAFE_DIVIDE(
        transition_count,
        SUM(transition_count) OVER (PARTITION BY current_channel)
      ),
      0
    ) AS pct_next_channel,
    ROW_NUMBER() OVER (
      PARTITION BY current_channel
      ORDER BY transition_count DESC, revenue DESC, orders DESC, next_channel ASC
    ) AS raw_next_channel_rank
  FROM complete_matrix
),

ranked AS (
  SELECT
    *,
    IF(total_next_channel_transitions_from_current > 0, raw_next_channel_rank, NULL) AS next_channel_rank
  FROM scored
)

SELECT
  current_channel,
  next_channel,

  -- Repeated on every row for the same current channel so Looker tables can show it.
  MAX(IF(next_channel_rank = 1, next_channel, NULL)) OVER (PARTITION BY current_channel)
    AS most_likely_next_channel,

  next_channel_rank,
  next_channel_rank = 1 AS is_most_likely_next_channel,

  transition_count,
  total_next_channel_transitions_from_current,
  pct_next_channel,
  ROUND(100 * pct_next_channel, 2) AS pct_next_channel_percent,

  journeys,
  orders,
  revenue
FROM ranked
ORDER BY current_channel, next_channel_rank, next_channel;
