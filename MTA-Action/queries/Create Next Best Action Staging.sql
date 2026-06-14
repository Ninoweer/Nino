-- Next Best Action for converting journeys:
-- "If the current channel is X, what is most likely to come next?"
-- Creates both a full transition matrix and the top next action per current channel.

DECLARE reporting_start_date DATE DEFAULT DATE '2026-05-28';
DECLARE reporting_end_date   DATE DEFAULT DATE '2026-06-10';
DECLARE lookback_days INT64 DEFAULT 7;

CREATE TEMP TABLE next_action_edges AS
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
       AND COALESCE(s.ga_session_id_int, -1) <= COALESCE(c.conversion_session_id_int, 9223372036854775807)
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

channel_edges AS (
  SELECT
    journey_id,
    channel AS current_channel,
    LEAD(channel) OVER (PARTITION BY journey_id ORDER BY step_position) AS next_action,
    conversion_orders,
    conversion_revenue
  FROM positioned
),

purchase_edges AS (
  SELECT
    journey_id,
    channel AS current_channel,
    'PURCHASE' AS next_action,
    conversion_orders,
    conversion_revenue
  FROM positioned
  QUALIFY ROW_NUMBER() OVER (PARTITION BY journey_id ORDER BY step_position DESC) = 1
)

SELECT *
FROM channel_edges
WHERE next_action IS NOT NULL
UNION ALL
SELECT *
FROM purchase_edges;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_next_best_action_matrix_20260528_20260610`
CLUSTER BY current_channel, next_action
AS
SELECT
  current_channel,
  next_action,
  COUNT(*) AS transition_count,
  COUNT(DISTINCT journey_id) AS journeys,
  SUM(conversion_orders) AS orders,
  SUM(conversion_revenue) AS revenue,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY current_channel)) AS pct_of_next_actions_from_current_channel,
  ROW_NUMBER() OVER (
    PARTITION BY current_channel
    ORDER BY COUNT(*) DESC, SUM(conversion_revenue) DESC, next_action
  ) AS next_action_rank
FROM next_action_edges
GROUP BY
  current_channel,
  next_action;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_next_best_action_top_20260528_20260610`
CLUSTER BY current_channel
AS
SELECT
  current_channel,
  next_action AS most_likely_next_action,
  transition_count,
  journeys,
  orders,
  revenue,
  pct_of_next_actions_from_current_channel
FROM `action-dwh.attribution_model_ecom.mta_next_best_action_matrix_20260528_20260610`
WHERE next_action_rank = 1;
