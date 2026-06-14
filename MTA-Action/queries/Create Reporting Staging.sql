-- Sawiday MTA Journey Reporting Tables
-- Target/reporting period: 2026-05-28 through 2026-06-10
-- Input/lookback period: 2026-05-21 through 2026-06-10
-- Source table expected to already contain standardized traffic_channelgroup values.
--
-- This script creates BI-ready journey diagnostic tables for:
--   - top purchase paths
--   - Sankey edges
--   - next-best-action / transition matrix long form
--   - conversion journey KPIs
--   - channel journey complexity distributions
--   - role/stage/position summaries
--   - channel interaction lift by revenue and orders
--
-- Interpretation guardrail:
--   These are descriptive observed-journey diagnostics. They are not causal lift,
--   not budget optimization, and not Markov credit replacement tables.

DECLARE target_start_date DATE DEFAULT DATE '2026-05-28';
DECLARE target_end_date   DATE DEFAULT DATE '2026-06-10';
DECLARE lookback_start_date DATE DEFAULT DATE '2026-05-21';
DECLARE lookback_days INT64 DEFAULT 7;
DECLARE top_path_limit INT64 DEFAULT 100;

-- -----------------------------------------------------------------------------
-- 0) Base sessions in the available lookback/input window
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE sessions_input AS
SELECT
  session_date,
  user_pseudo_id,
  CAST(ga_session_id AS STRING) AS ga_session_id,
  COALESCE(traffic_channelgroup, 'Unattributed') AS channel,
  user_pseudo_session_id,
  COALESCE(transactions, 0) AS transactions,
  COALESCE(ecommerce_purchase_revenue, 0.0) AS revenue,
  SAFE_CAST(ga_session_id AS INT64) AS ga_session_id_num
FROM `action-dwh.attribution_model_ecom.mta_sessions_20260501_20260610`
WHERE session_date BETWEEN lookback_start_date AND target_end_date
  AND user_pseudo_id IS NOT NULL
  AND ga_session_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 1) Target-period converting sessions. Each converting session is one journey.
--    If a converting session has transactions > 1, the journey carries that
--    order count in journey_orders.
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE conversion_sessions AS
SELECT
  CONCAT(
    user_pseudo_id,
    '#', FORMAT_DATE('%Y%m%d', session_date),
    '#', ga_session_id,
    '#', CAST(user_conversion_number AS STRING)
  ) AS journey_id,
  user_pseudo_id,
  session_date AS conversion_date,
  ga_session_id AS conversion_session_id,
  ga_session_id_num AS conversion_session_id_num,
  transactions AS journey_orders,
  revenue AS journey_revenue,
  user_conversion_number
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id
      ORDER BY session_date, COALESCE(ga_session_id_num, 9223372036854775807), ga_session_id
    ) AS user_conversion_number
  FROM sessions_input s
  WHERE s.session_date BETWEEN target_start_date AND target_end_date
    AND s.transactions > 0
);

-- -----------------------------------------------------------------------------
-- 2) Touchpoints feeding each conversion.
--    The touchpoint window is max(lookback_start_date, conversion_date - 7 days)
--    through the conversion session.
--
--    If ga_session_id is not numeric, same-day ordering cannot be perfectly known.
--    In that fallback case, same-day sessions are included conservatively.
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE journey_touches_raw AS
SELECT
  c.journey_id,
  c.user_pseudo_id,
  c.conversion_date,
  c.conversion_session_id,
  c.conversion_session_id_num,
  c.journey_orders,
  c.journey_revenue,
  s.session_date,
  s.ga_session_id,
  s.ga_session_id_num,
  s.user_pseudo_session_id,
  s.channel,
  s.transactions AS session_transactions,
  s.revenue AS session_revenue
FROM conversion_sessions c
JOIN sessions_input s
  ON s.user_pseudo_id = c.user_pseudo_id
WHERE s.session_date BETWEEN GREATEST(DATE_SUB(c.conversion_date, INTERVAL lookback_days DAY), lookback_start_date)
                         AND c.conversion_date
  AND (
    s.session_date < c.conversion_date
    OR c.conversion_session_id_num IS NULL
    OR s.ga_session_id_num IS NULL
    OR s.ga_session_id_num <= c.conversion_session_id_num
  );

CREATE TEMP TABLE journey_touches AS
SELECT
  r.*,
  ROW_NUMBER() OVER (
    PARTITION BY journey_id
    ORDER BY session_date, COALESCE(ga_session_id_num, 9223372036854775807), ga_session_id
  ) AS touch_position,
  COUNT(*) OVER (PARTITION BY journey_id) AS session_count
FROM journey_touches_raw r;

CREATE TEMP TABLE journey_touches_enriched AS
SELECT
  t.*,
  CASE
    WHEN session_count = 1 THEN 'single_touch'
    WHEN touch_position <= CEIL(session_count / 3.0) THEN 'early'
    WHEN touch_position <= CEIL(2 * session_count / 3.0) THEN 'mid'
    ELSE 'late'
  END AS journey_stage,
  CASE
    WHEN session_count = 1 THEN 'single_touch'
    WHEN touch_position = 1 THEN 'opener'
    WHEN touch_position = session_count THEN 'closer'
    ELSE 'assist'
  END AS role_type,
  SAFE_DIVIDE(touch_position - 1, NULLIF(session_count - 1, 0)) AS position_index_0_to_1
FROM journey_touches t;

-- -----------------------------------------------------------------------------
-- 3) Consecutive-channel-compressed journey steps for cleaner paths/Sankey.
--    Example: Paid Search > Paid Search > Direct becomes Paid Search > Direct.
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE journey_channel_steps AS
SELECT
  f.* EXCEPT(prev_channel),
  ROW_NUMBER() OVER (
    PARTITION BY journey_id
    ORDER BY touch_position
  ) AS channel_step_position
FROM (
  SELECT
    t.*,
    LAG(channel) OVER (PARTITION BY journey_id ORDER BY touch_position) AS prev_channel
  FROM journey_touches_enriched t
) f
WHERE prev_channel IS NULL OR channel != prev_channel;

CREATE TEMP TABLE journey_summary_raw AS
SELECT
  journey_id,
  ANY_VALUE(user_pseudo_id) AS user_pseudo_id,
  ANY_VALUE(conversion_date) AS conversion_date,
  ANY_VALUE(conversion_session_id) AS conversion_session_id,
  ANY_VALUE(journey_orders) AS journey_orders,
  ANY_VALUE(journey_revenue) AS journey_revenue,
  MIN(session_date) AS first_touch_date,
  MAX(session_date) AS last_touch_date,
  COUNT(*) AS session_count,
  COUNT(DISTINCT channel) AS unique_channel_count,
  DATE_DIFF(ANY_VALUE(conversion_date), MIN(session_date), DAY) AS days_to_conversion,
  STRING_AGG(channel, ' > ' ORDER BY touch_position) AS session_channel_path,
  ARRAY_AGG(channel ORDER BY touch_position LIMIT 1)[OFFSET(0)] AS first_channel,
  ARRAY_AGG(channel ORDER BY touch_position DESC LIMIT 1)[OFFSET(0)] AS last_channel
FROM journey_touches_enriched
GROUP BY journey_id;

CREATE TEMP TABLE journey_summary AS
SELECT
  r.*,
  c.compressed_channel_path,
  c.compressed_channel_count,
  target_start_date AS reporting_start_date,
  target_end_date AS reporting_end_date,
  lookback_start_date AS lookback_input_start_date,
  lookback_days AS lookback_days
FROM journey_summary_raw r
JOIN (
  SELECT
    journey_id,
    STRING_AGG(channel, ' > ' ORDER BY channel_step_position) AS compressed_channel_path,
    COUNT(*) AS compressed_channel_count
  FROM journey_channel_steps
  GROUP BY journey_id
) c USING (journey_id);

CREATE TEMP TABLE first_conversion_per_user AS
SELECT *
FROM journey_summary
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY user_pseudo_id
  ORDER BY conversion_date, conversion_session_id
) = 1;

CREATE TEMP TABLE path_rank AS
SELECT
  compressed_channel_path,
  ROW_NUMBER() OVER (
    ORDER BY COUNT(*) DESC, SUM(journey_revenue) DESC, compressed_channel_path
  ) AS path_rank
FROM journey_summary
GROUP BY compressed_channel_path;

-- -----------------------------------------------------------------------------
-- 4) KPI summary table
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_purchase_journey_kpis_20260528_20260610` AS
SELECT
  target_start_date AS reporting_start_date,
  target_end_date AS reporting_end_date,
  lookback_start_date AS lookback_input_start_date,
  target_end_date AS lookback_input_end_date,
  lookback_days AS lookback_days,
  COUNT(*) AS total_converting_journeys,
  COUNT(DISTINCT user_pseudo_id) AS total_converting_users,
  SUM(journey_orders) AS total_conversion_orders,
  SUM(journey_revenue) AS total_conversion_revenue,
  AVG(unique_channel_count) AS avg_unique_channels_per_conversion_journey,
  AVG(session_count) AS avg_sessions_per_conversion_journey,
  AVG(days_to_conversion) AS avg_days_to_conversion_per_journey,
  (SELECT AVG(days_to_conversion) FROM first_conversion_per_user) AS avg_days_to_conversion_per_converting_user_first_conversion,
  SAFE_MULTIPLY(100, SAFE_DIVIDE(COUNTIF(unique_channel_count > 1), COUNT(*))) AS pct_converting_journeys_more_than_1_channel,
  SAFE_MULTIPLY(100, SAFE_DIVIDE(COUNTIF(session_count > 1), COUNT(*))) AS pct_converting_journeys_more_than_1_session,
  SAFE_MULTIPLY(100, SAFE_DIVIDE(COUNTIF(compressed_channel_count > 1), COUNT(*))) AS pct_converting_journeys_more_than_1_consecutive_channel_step
FROM journey_summary;

-- -----------------------------------------------------------------------------
-- 5) Most occurring purchase customer journeys / path summary
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_purchase_journey_paths_20260528_20260610`
CLUSTER BY path_rank
AS
SELECT
  p.path_rank,
  js.compressed_channel_path AS channel_path,
  js.session_channel_path AS raw_session_channel_path_example,
  COUNT(*) AS path_count,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()) AS path_share,
  SUM(js.journey_orders) AS path_orders,
  SUM(js.journey_revenue) AS path_revenue,
  AVG(js.session_count) AS avg_sessions_per_journey,
  AVG(js.unique_channel_count) AS avg_unique_channels_per_journey,
  AVG(js.days_to_conversion) AS avg_days_to_conversion,
  MIN(js.first_touch_date) AS earliest_first_touch_date,
  MAX(js.conversion_date) AS latest_conversion_date,
  COUNTIF(js.session_count > 1) AS multi_session_journey_count,
  COUNTIF(js.unique_channel_count > 1) AS multi_channel_journey_count,
  SAFE_DIVIDE(COUNTIF(js.session_count > 1), COUNT(*)) AS multi_session_share,
  SAFE_DIVIDE(COUNTIF(js.unique_channel_count > 1), COUNT(*)) AS multi_channel_share
FROM journey_summary js
JOIN path_rank p USING (compressed_channel_path)
GROUP BY
  p.path_rank,
  channel_path,
  raw_session_channel_path_example;

-- -----------------------------------------------------------------------------
-- 6) Sankey edges for top purchase paths.
--    Uses step-prefixed node IDs to keep Sankey acyclic even when a channel
--    appears at multiple path positions.
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE top_path_journeys AS
SELECT js.journey_id
FROM journey_summary js
JOIN path_rank p USING (compressed_channel_path)
WHERE p.path_rank <= top_path_limit;

CREATE TEMP TABLE sankey_edge_events AS
SELECT
  js.journey_id,
  0 AS edge_step,
  'START' AS source_channel,
  js.first_channel AS target_channel,
  '00_START' AS source_node,
  FORMAT('01_%s', js.first_channel) AS target_node,
  js.journey_orders,
  js.journey_revenue,
  js.compressed_channel_path AS channel_path
FROM journey_summary js
JOIN top_path_journeys tp USING (journey_id)

UNION ALL

SELECT
  a.journey_id,
  a.channel_step_position AS edge_step,
  a.channel AS source_channel,
  b.channel AS target_channel,
  FORMAT('%02d_%s', a.channel_step_position, a.channel) AS source_node,
  FORMAT('%02d_%s', b.channel_step_position, b.channel) AS target_node,
  ANY_VALUE(a.journey_orders) AS journey_orders,
  ANY_VALUE(a.journey_revenue) AS journey_revenue,
  ANY_VALUE(js.compressed_channel_path) AS channel_path
FROM journey_channel_steps a
JOIN journey_channel_steps b
  ON a.journey_id = b.journey_id
 AND b.channel_step_position = a.channel_step_position + 1
JOIN top_path_journeys tp
  ON a.journey_id = tp.journey_id
JOIN journey_summary js
  ON a.journey_id = js.journey_id
GROUP BY
  a.journey_id,
  edge_step,
  source_channel,
  target_channel,
  source_node,
  target_node

UNION ALL

SELECT
  js.journey_id,
  js.compressed_channel_count AS edge_step,
  js.last_channel AS source_channel,
  'PURCHASE' AS target_channel,
  FORMAT('%02d_%s', js.compressed_channel_count, js.last_channel) AS source_node,
  FORMAT('%02d_PURCHASE', js.compressed_channel_count + 1) AS target_node,
  js.journey_orders,
  js.journey_revenue,
  js.compressed_channel_path AS channel_path
FROM journey_summary js
JOIN top_path_journeys tp USING (journey_id);

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_purchase_journey_sankey_edges_20260528_20260610`
CLUSTER BY edge_step, source_channel, target_channel
AS
SELECT
  edge_step,
  source_channel,
  target_channel,
  source_node,
  target_node,
  COUNT(DISTINCT journey_id) AS transition_count,
  SUM(journey_orders) AS transition_orders,
  SUM(journey_revenue) AS transition_revenue,
  COUNT(DISTINCT channel_path) AS supporting_top_paths,
  top_path_limit AS top_path_limit_used
FROM sankey_edge_events
GROUP BY
  edge_step,
  source_channel,
  target_channel,
  source_node,
  target_node;

-- -----------------------------------------------------------------------------
-- 7) Next best action / transition matrix long form
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE action_edge_events AS
SELECT
  a.journey_id,
  a.channel AS current_channel,
  b.channel AS next_channel_or_action,
  ANY_VALUE(a.journey_orders) AS journey_orders,
  ANY_VALUE(a.journey_revenue) AS journey_revenue
FROM journey_channel_steps a
JOIN journey_channel_steps b
  ON a.journey_id = b.journey_id
 AND b.channel_step_position = a.channel_step_position + 1
GROUP BY
  a.journey_id,
  current_channel,
  next_channel_or_action

UNION ALL

SELECT
  js.journey_id,
  js.last_channel AS current_channel,
  'PURCHASE' AS next_channel_or_action,
  js.journey_orders,
  js.journey_revenue
FROM journey_summary js;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_next_best_action_matrix_long_20260528_20260610`
CLUSTER BY current_channel, next_channel_or_action
AS
WITH agg AS (
  SELECT
    current_channel,
    next_channel_or_action,
    COUNT(*) AS transition_count,
    SUM(journey_orders) AS transition_orders,
    SUM(journey_revenue) AS transition_revenue
  FROM action_edge_events
  GROUP BY
    current_channel,
    next_channel_or_action
), totals AS (
  SELECT
    current_channel,
    SUM(transition_count) AS total_outgoing_transitions,
    SUM(transition_orders) AS total_outgoing_orders,
    SUM(transition_revenue) AS total_outgoing_revenue
  FROM agg
  GROUP BY current_channel
)
SELECT
  a.current_channel,
  a.next_channel_or_action,
  a.transition_count,
  SAFE_DIVIDE(a.transition_count, t.total_outgoing_transitions) AS transition_probability,
  a.transition_orders,
  SAFE_DIVIDE(a.transition_orders, t.total_outgoing_orders) AS transition_order_share,
  a.transition_revenue,
  SAFE_DIVIDE(a.transition_revenue, t.total_outgoing_revenue) AS transition_revenue_share,
  ROW_NUMBER() OVER (
    PARTITION BY a.current_channel
    ORDER BY SAFE_DIVIDE(a.transition_count, t.total_outgoing_transitions) DESC,
             a.transition_count DESC,
             a.transition_revenue DESC,
             a.next_channel_or_action
  ) AS next_action_rank,
  ROW_NUMBER() OVER (
    PARTITION BY a.current_channel
    ORDER BY SAFE_DIVIDE(a.transition_count, t.total_outgoing_transitions) DESC,
             a.transition_count DESC,
             a.transition_revenue DESC,
             a.next_channel_or_action
  ) = 1 AS is_next_best_action
FROM agg a
JOIN totals t USING (current_channel);

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_next_best_action_20260528_20260610`
CLUSTER BY current_channel
AS
SELECT
  current_channel,
  next_channel_or_action AS recommended_next_channel_or_action,
  CASE
    WHEN next_channel_or_action = 'PURCHASE'
      THEN 'Likely next observed action is purchase/close; use closing and friction-reduction messaging.'
    ELSE CONCAT('Likely next observed channel is ', next_channel_or_action, '; use this for journey sequencing and campaign handoff diagnostics.')
  END AS recommended_action_text,
  transition_probability,
  transition_count,
  transition_orders,
  transition_revenue
FROM `action-dwh.attribution_model_ecom.mta_next_best_action_matrix_long_20260528_20260610`
WHERE is_next_best_action;

-- -----------------------------------------------------------------------------
-- 8) Channel journey complexity summary and distributions
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE journey_channel_membership AS
SELECT DISTINCT
  journey_id,
  channel
FROM journey_touches_enriched;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_channel_journey_complexity_summary_20260528_20260610`
CLUSTER BY channel
AS
SELECT
  m.channel,
  COUNT(*) AS journeys_with_channel,
  AVG(js.unique_channel_count) AS avg_unique_channels_per_journey_with_channel,
  AVG(js.session_count) AS avg_sessions_per_journey_with_channel,
  APPROX_QUANTILES(js.unique_channel_count, 4)[OFFSET(1)] AS p25_unique_channels,
  APPROX_QUANTILES(js.unique_channel_count, 4)[OFFSET(2)] AS median_unique_channels,
  APPROX_QUANTILES(js.unique_channel_count, 4)[OFFSET(3)] AS p75_unique_channels,
  APPROX_QUANTILES(js.session_count, 4)[OFFSET(1)] AS p25_sessions,
  APPROX_QUANTILES(js.session_count, 4)[OFFSET(2)] AS median_sessions,
  APPROX_QUANTILES(js.session_count, 4)[OFFSET(3)] AS p75_sessions,
  SUM(js.journey_orders) AS orders_in_journeys_with_channel,
  SUM(js.journey_revenue) AS revenue_in_journeys_with_channel
FROM journey_channel_membership m
JOIN journey_summary js USING (journey_id)
GROUP BY m.channel;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_channel_journey_complexity_distribution_20260528_20260610`
CLUSTER BY channel, metric_name
AS
SELECT
  m.channel,
  'unique_channels_per_journey' AS metric_name,
  CAST(js.unique_channel_count AS INT64) AS metric_value,
  COUNT(*) AS journey_count,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY m.channel, 'unique_channels_per_journey')) AS journey_share_within_channel,
  SUM(js.journey_orders) AS orders,
  SUM(js.journey_revenue) AS revenue
FROM journey_channel_membership m
JOIN journey_summary js USING (journey_id)
GROUP BY
  m.channel,
  metric_value

UNION ALL

SELECT
  m.channel,
  'sessions_per_journey' AS metric_name,
  CAST(js.session_count AS INT64) AS metric_value,
  COUNT(*) AS journey_count,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY m.channel, 'sessions_per_journey')) AS journey_share_within_channel,
  SUM(js.journey_orders) AS orders,
  SUM(js.journey_revenue) AS revenue
FROM journey_channel_membership m
JOIN journey_summary js USING (journey_id)
GROUP BY
  m.channel,
  metric_value;

-- -----------------------------------------------------------------------------
-- 9) Channel roles, journey stages and position index
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_channel_role_stage_position_20260528_20260610`
CLUSTER BY channel
AS
SELECT
  channel,
  COUNT(*) AS touch_occurrences,
  COUNT(DISTINCT journey_id) AS journeys_with_channel,
  COUNTIF(journey_stage = 'early') AS early_touches,
  COUNTIF(journey_stage = 'mid') AS mid_touches,
  COUNTIF(journey_stage = 'late') AS late_touches,
  COUNTIF(journey_stage = 'single_touch') AS single_touch_stage_touches,
  SAFE_DIVIDE(COUNTIF(journey_stage = 'early'), COUNT(*)) AS pct_early_in_journey,
  SAFE_DIVIDE(COUNTIF(journey_stage = 'mid'), COUNT(*)) AS pct_mid_in_journey,
  SAFE_DIVIDE(COUNTIF(journey_stage = 'late'), COUNT(*)) AS pct_late_in_journey,
  SAFE_DIVIDE(COUNTIF(journey_stage = 'single_touch'), COUNT(*)) AS pct_single_touch_stage,
  COUNTIF(role_type = 'opener') AS opener_touches,
  COUNTIF(role_type = 'assist') AS assist_touches,
  COUNTIF(role_type = 'closer') AS closer_touches,
  COUNTIF(role_type = 'single_touch') AS single_touch_role_touches,
  SAFE_DIVIDE(COUNTIF(role_type = 'opener'), COUNT(*)) AS pct_role_opener,
  SAFE_DIVIDE(COUNTIF(role_type = 'assist'), COUNT(*)) AS pct_role_assist,
  SAFE_DIVIDE(COUNTIF(role_type = 'closer'), COUNT(*)) AS pct_role_closer,
  SAFE_DIVIDE(COUNTIF(role_type = 'single_touch'), COUNT(*)) AS pct_role_single_touch,
  AVG(touch_position) AS avg_position_number,
  AVG(position_index_0_to_1) AS journey_position_index_0_to_1,
  SAFE_MULTIPLY(100, AVG(position_index_0_to_1)) AS journey_position_index_0_to_100,
  AVG(session_count) AS avg_journey_session_count_when_channel_appears,
  AVG(journey_orders) AS avg_orders_when_channel_appears,
  AVG(journey_revenue) AS avg_revenue_when_channel_appears
FROM journey_touches_enriched
GROUP BY channel;

-- Long table version for stacked role/stage visuals.
CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_channel_role_stage_long_20260528_20260610`
CLUSTER BY channel, metric_family
AS
SELECT
  channel,
  'role_type' AS metric_family,
  role_type AS metric_value,
  COUNT(*) AS touch_occurrences,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY channel, 'role_type')) AS share_within_channel
FROM journey_touches_enriched
GROUP BY channel, metric_value

UNION ALL

SELECT
  channel,
  'journey_stage' AS metric_family,
  journey_stage AS metric_value,
  COUNT(*) AS touch_occurrences,
  SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY channel, 'journey_stage')) AS share_within_channel
FROM journey_touches_enriched
GROUP BY channel, metric_value;

-- -----------------------------------------------------------------------------
-- 10) Channel interaction lift heatmaps: descriptive co-occurrence lift by
--     journey count, revenue and order volume.
-- -----------------------------------------------------------------------------

CREATE TEMP TABLE channel_pairs_by_journey AS
SELECT
  a.journey_id,
  a.channel AS channel_a,
  b.channel AS channel_b
FROM journey_channel_membership a
JOIN journey_channel_membership b
  ON a.journey_id = b.journey_id
 AND a.channel < b.channel;

CREATE TEMP TABLE total_journey_stats AS
SELECT
  COUNT(*) AS total_journeys,
  SUM(journey_revenue) AS total_revenue,
  SUM(journey_orders) AS total_orders
FROM journey_summary;

CREATE TEMP TABLE channel_presence_stats AS
SELECT
  m.channel,
  COUNT(*) AS journeys_with_channel,
  SUM(js.journey_revenue) AS revenue_with_channel,
  SUM(js.journey_orders) AS orders_with_channel
FROM journey_channel_membership m
JOIN journey_summary js USING (journey_id)
GROUP BY m.channel;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_channel_interaction_lift_20260528_20260610`
CLUSTER BY channel_a, channel_b
AS
WITH pair_stats AS (
  SELECT
    p.channel_a,
    p.channel_b,
    COUNT(*) AS journeys_with_pair,
    SUM(js.journey_revenue) AS pair_revenue,
    SUM(js.journey_orders) AS pair_orders
  FROM channel_pairs_by_journey p
  JOIN journey_summary js USING (journey_id)
  GROUP BY
    p.channel_a,
    p.channel_b
)
SELECT
  ps.channel_a,
  ps.channel_b,
  ps.journeys_with_pair,
  ps.pair_revenue,
  ps.pair_orders AS pair_attributed_orders_descriptive,
  SAFE_DIVIDE(ps.journeys_with_pair, t.total_journeys) AS pair_journey_share,
  SAFE_DIVIDE(
    SAFE_DIVIDE(ps.journeys_with_pair, t.total_journeys),
    SAFE_DIVIDE(a.journeys_with_channel, t.total_journeys) * SAFE_DIVIDE(b.journeys_with_channel, t.total_journeys)
  ) AS journey_count_lift_vs_independence,
  SAFE_DIVIDE(
    SAFE_DIVIDE(ps.pair_revenue, t.total_revenue),
    SAFE_DIVIDE(a.revenue_with_channel, t.total_revenue) * SAFE_DIVIDE(b.revenue_with_channel, t.total_revenue)
  ) AS revenue_lift_vs_independence,
  SAFE_DIVIDE(
    SAFE_DIVIDE(ps.pair_orders, t.total_orders),
    SAFE_DIVIDE(a.orders_with_channel, t.total_orders) * SAFE_DIVIDE(b.orders_with_channel, t.total_orders)
  ) AS attributed_orders_lift_vs_independence_descriptive,
  SAFE_DIVIDE(
    ps.journeys_with_pair,
    a.journeys_with_channel + b.journeys_with_channel - ps.journeys_with_pair
  ) AS jaccard_index,
  a.journeys_with_channel AS channel_a_journeys,
  b.journeys_with_channel AS channel_b_journeys,
  a.revenue_with_channel AS channel_a_revenue,
  b.revenue_with_channel AS channel_b_revenue,
  a.orders_with_channel AS channel_a_orders,
  b.orders_with_channel AS channel_b_orders,
  t.total_journeys,
  t.total_revenue,
  t.total_orders
FROM pair_stats ps
JOIN channel_presence_stats a
  ON ps.channel_a = a.channel
JOIN channel_presence_stats b
  ON ps.channel_b = b.channel
CROSS JOIN total_journey_stats t;

-- -----------------------------------------------------------------------------
-- 11) Validation / coverage helper table
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_journey_reporting_table_manifest_20260528_20260610` AS
SELECT 'mta_purchase_journey_kpis_20260528_20260610' AS table_name, 'one row: KPI summary for converting journeys' AS purpose UNION ALL
SELECT 'mta_purchase_journey_paths_20260528_20260610', 'path frequency/revenue/order summary with >-separated channels' UNION ALL
SELECT 'mta_purchase_journey_sankey_edges_20260528_20260610', 'Sankey-ready START/channel/PURCHASE edges from top paths' UNION ALL
SELECT 'mta_next_best_action_matrix_long_20260528_20260610', 'long-form transition matrix: current channel x next channel/action' UNION ALL
SELECT 'mta_next_best_action_20260528_20260610', 'one recommended observed next action per current channel' UNION ALL
SELECT 'mta_channel_journey_complexity_summary_20260528_20260610', 'per-channel average journey complexity' UNION ALL
SELECT 'mta_channel_journey_complexity_distribution_20260528_20260610', 'per-channel distributions of unique channels and sessions per journey' UNION ALL
SELECT 'mta_channel_role_stage_position_20260528_20260610', 'per-channel role/stage shares and journey position index' UNION ALL
SELECT 'mta_channel_role_stage_long_20260528_20260610', 'long-form role/stage table for stacked visuals' UNION ALL
SELECT 'mta_channel_interaction_lift_20260528_20260610', 'pair co-occurrence lift by journey count, revenue and descriptive orders';
