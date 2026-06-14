DECLARE top_path_limit INT64 DEFAULT 100;

CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_purchase_journey_sankey_looker_20260528_20260610`
CLUSTER BY source_node, target_node
AS

WITH base AS (
  SELECT *
  FROM `action-dwh.attribution_model_ecom.mta_purchase_journey_base_20260528_20260610`
),

top_paths AS (
  SELECT
    channel_path,
    COUNT(*) AS path_journeys,
    SUM(conversion_orders) AS path_orders,
    SUM(conversion_revenue) AS path_revenue,
    ROW_NUMBER() OVER (
      ORDER BY COUNT(*) DESC, SUM(conversion_revenue) DESC, channel_path
    ) AS path_rank
  FROM base
  GROUP BY channel_path
),

top_path_journeys AS (
  SELECT b.*
  FROM base b
  JOIN top_paths p USING (channel_path)
  WHERE p.path_rank <= top_path_limit
),

edge_rows AS (
  -- START to first channel.
  SELECT
    '00 | START' AS source_node,
    FORMAT('%02d | %s', first_tp.touchpoint_position, first_tp.channel) AS target_node,
    'START' AS source_channel,
    first_tp.channel AS target_channel,
    0 AS source_step,
    first_tp.touchpoint_position AS target_step,
    b.conversion_journey_id,
    b.conversion_orders,
    b.conversion_revenue,
    b.channel_path,
    b.session_count,
    b.unique_channel_count,
    b.days_to_conversion
  FROM top_path_journeys b,
  UNNEST(b.touchpoints) AS first_tp
  WHERE first_tp.touchpoint_position = 1

  UNION ALL

  -- Stage-aware channel to next stage-aware channel.
  SELECT
    FORMAT('%02d | %s', cur_tp.touchpoint_position, cur_tp.channel) AS source_node,
    FORMAT('%02d | %s', next_tp.touchpoint_position, next_tp.channel) AS target_node,
    cur_tp.channel AS source_channel,
    next_tp.channel AS target_channel,
    cur_tp.touchpoint_position AS source_step,
    next_tp.touchpoint_position AS target_step,
    b.conversion_journey_id,
    b.conversion_orders,
    b.conversion_revenue,
    b.channel_path,
    b.session_count,
    b.unique_channel_count,
    b.days_to_conversion
  FROM top_path_journeys b,
  UNNEST(b.touchpoints) AS cur_tp
  JOIN UNNEST(b.touchpoints) AS next_tp
    ON next_tp.touchpoint_position = cur_tp.touchpoint_position + 1

  UNION ALL

  -- Last channel to PURCHASE. PURCHASE is a sink and is never a source.
  SELECT
    FORMAT('%02d | %s', last_tp.touchpoint_position, last_tp.channel) AS source_node,
    '99 | PURCHASE' AS target_node,
    last_tp.channel AS source_channel,
    'PURCHASE' AS target_channel,
    last_tp.touchpoint_position AS source_step,
    99 AS target_step,
    b.conversion_journey_id,
    b.conversion_orders,
    b.conversion_revenue,
    b.channel_path,
    b.session_count,
    b.unique_channel_count,
    b.days_to_conversion
  FROM top_path_journeys b,
  UNNEST(b.touchpoints) AS last_tp
  WHERE last_tp.touchpoint_position = b.session_count
)

SELECT
  source_node,
  target_node,

  -- Clean labels for detail tables/tooltips.
  -- Do not use these as Looker Sankey nodes.
  source_channel,
  target_channel,

  source_step,
  target_step,

  COUNT(*) AS transition_count,
  COUNT(DISTINCT conversion_journey_id) AS journeys,
  SUM(conversion_orders) AS orders,
  SUM(conversion_revenue) AS revenue,
  AVG(session_count) AS avg_sessions_in_journey,
  AVG(unique_channel_count) AS avg_unique_channels_in_journey,
  AVG(days_to_conversion) AS avg_days_to_conversion,
  COUNT(DISTINCT channel_path) AS distinct_paths_using_edge

FROM edge_rows
GROUP BY
  source_node,
  target_node,
  source_channel,
  target_channel,
  source_step,
  target_step;
