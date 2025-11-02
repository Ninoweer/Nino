-- SESSION-LEVEL FEATURE USAGE BY DEFAULT_CHANNEL_GROUPING (ENGAGED ONLY)
-- Use WHERE is_first_session_only = TRUE to zoom into first sessions only.

WITH engaged AS (
  SELECT
    DATE(date) AS date,
    user_pseudo_id,
    SAFE_CAST(session_id AS INT64) AS session_id,
    COALESCE(
      SAFE_CAST(engaged_session AS BOOL),
      SAFE_CAST(engaged_sessions AS BOOL),
      SAFE_CAST(engaged_sessions AS INT64) > 0,
      FALSE
    ) AS engaged_session,
    COALESCE(
      SAFE_CAST(identified_session AS BOOL),
      SAFE_CAST(identified_session AS INT64) > 0,
      FALSE
    ) AS identified_session,
    COALESCE(
      SAFE_CAST(login AS BOOL),
      SAFE_CAST(login AS INT64) > 0,
      FALSE
    ) AS login,
    COALESCE(default_channel_grouping,'(unknown)') AS default_channel_grouping,

    COALESCE(promo_views,0)                     AS promo_views,
    COALESCE(category_screens,0)                AS category_screens,
    COALESCE(weekdeals_screens,0)               AS weekdeals_screens,
    COALESCE(nextweekdeals_screens,0)           AS nextweekdeals_screens,
    COALESCE(searches,0)                        AS searches,
    COALESCE(plp_product_impressions,0)         AS plp_product_impressions,
    COALESCE(pdp_views,0)                       AS pdp_views,
    COALESCE(main_page_teasers,0)               AS main_page_teasers,
    COALESCE(category_carousel_selected,0)      AS category_carousel_selected,
    COALESCE(products_screens,0)                AS products_screens,
    COALESCE(moment_screens,0)                  AS moment_screens,
    COALESCE(shoppinglist_screens,0)            AS shoppinglist_screens,
    COALESCE(myaction_screen,0)                 AS myaction_screen,
    COALESCE(barcodescanner_open,0)             AS barcodescanner_open,
    COALESCE(loyalty_card_show,0)               AS loyalty_card_show,
    COALESCE(digital_receipt_views,0)           AS digital_receipt_views,
    COALESCE(loyalty_game_entries,0)            AS loyalty_game_entries,
    COALESCE(folder_opens,0)                    AS folder_opens,
    COALESCE(add_to_shoppinglists,0)            AS add_to_shoppinglists,
    COALESCE(navigated,0)                       AS navigated,
    COALESCE(webshop_products_viewed,0)         AS webshop_products_viewed
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE COALESCE(
          SAFE_CAST(engaged_session AS BOOL),
          SAFE_CAST(engaged_sessions AS BOOL),
          SAFE_CAST(engaged_sessions AS INT64) > 0,
          FALSE
        ) = TRUE
),

user_stats AS (
  SELECT
    user_pseudo_id,
    MIN(date) AS first_seen_date,
    COUNT(DISTINCT session_id) AS total_engaged_sessions_per_user,
    COUNTIF(loyalty_card_show > 0) AS loyalty_show_engaged_sessions_per_user
  FROM engaged
  GROUP BY user_pseudo_id
),

with_ord AS (
  SELECT
    e.*,
    us.first_seen_date,
    us.total_engaged_sessions_per_user,
    us.loyalty_show_engaged_sessions_per_user,
    ROW_NUMBER() OVER (PARTITION BY e.user_pseudo_id ORDER BY e.date, e.session_id) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY e.user_pseudo_id ORDER BY e.date DESC, e.session_id DESC) AS rn_desc
  FROM engaged e
  JOIN user_stats us USING (user_pseudo_id)
),

with_flags AS (
  SELECT
    *,
    (identified_session OR login) AS is_logged_in,
    (loyalty_card_show > 0) AS is_loyalty_session,
    CASE WHEN total_engaged_sessions_per_user = 1 THEN 'one_session' ELSE 'multi_session' END AS session_count_bucket_1vMore,
    CASE WHEN loyalty_show_engaged_sessions_per_user = 1 THEN 'one_loyalty_session' ELSE 'multi_loyalty_session' END AS loyalty_session_bucket_1vMore,
    DATE_DIFF(date, first_seen_date, DAY) AS days_since_first_seen,
    DATE_DIFF(date, first_seen_date, DAY) < 15 AS is_new_user_15d,
    CASE WHEN rn_asc = 1 THEN 'first_session' ELSE 'not_first' END AS first_session_flag
  FROM with_ord
),

session_long AS (
  SELECT
    s.user_pseudo_id,
    s.session_id,
    s.default_channel_grouping,
    (s.first_session_flag = 'first_session') AS is_first_session_only,
    s.is_logged_in,
    s.is_loyalty_session,
    s.session_count_bucket_1vMore,
    s.loyalty_session_bucket_1vMore,
    s.is_new_user_15d,
    m.metric_name,
    m.metric_value
  FROM with_flags AS s,
  UNNEST([
    STRUCT('promo_views'                AS metric_name, CAST(promo_views                AS INT64) AS metric_value),
    STRUCT('category_screens'           AS metric_name, CAST(category_screens           AS INT64) AS metric_value),
    STRUCT('weekdeals_screens'          AS metric_name, CAST(weekdeals_screens          AS INT64) AS metric_value),
    STRUCT('nextweekdeals_screens'      AS metric_name, CAST(nextweekdeals_screens      AS INT64) AS metric_value),
    STRUCT('searches'                   AS metric_name, CAST(searches                   AS INT64) AS metric_value),
    STRUCT('plp_product_impressions'    AS metric_name, CAST(plp_product_impressions    AS INT64) AS metric_value),
    STRUCT('pdp_views'                  AS metric_name, CAST(pdp_views                  AS INT64) AS metric_value),
    STRUCT('main_page_teasers'          AS metric_name, CAST(main_page_teasers          AS INT64) AS metric_value),
    STRUCT('category_carousel_selected' AS metric_name, CAST(category_carousel_selected AS INT64) AS metric_value),
    STRUCT('products_screens'           AS metric_name, CAST(products_screens           AS INT64) AS metric_value),
    STRUCT('moment_screens'             AS metric_name, CAST(moment_screens             AS INT64) AS metric_value),
    STRUCT('shoppinglist_screens'       AS metric_name, CAST(shoppinglist_screens       AS INT64) AS metric_value),
    STRUCT('myaction_screen'            AS metric_name, CAST(myaction_screen            AS INT64) AS metric_value),
    STRUCT('barcodescanner_open'        AS metric_name, CAST(barcodescanner_open        AS INT64) AS metric_value),
    STRUCT('loyalty_card_show'          AS metric_name, CAST(loyalty_card_show          AS INT64) AS metric_value),
    STRUCT('digital_receipt_views'      AS metric_name, CAST(digital_receipt_views      AS INT64) AS metric_value),
    STRUCT('loyalty_game_entries'       AS metric_name, CAST(loyalty_game_entries       AS INT64) AS metric_value),
    STRUCT('folder_opens'               AS metric_name, CAST(folder_opens               AS INT64) AS metric_value),
    STRUCT('add_to_shoppinglists'       AS metric_name, CAST(add_to_shoppinglists       AS INT64) AS metric_value),
    STRUCT('navigated'                  AS metric_name, CAST(navigated                  AS INT64) AS metric_value),
    STRUCT('webshop_products_viewed'    AS metric_name, CAST(webshop_products_viewed    AS INT64) AS metric_value)
  ]) AS m
),

agg AS (
  -- Aggregate first, producing stable named columns for derived metrics.
  SELECT
    default_channel_grouping,
    metric_name,
    is_first_session_only,
    is_logged_in,
    is_loyalty_session,
    session_count_bucket_1vMore,
    loyalty_session_bucket_1vMore,
    is_new_user_15d,

    COUNT(DISTINCT session_id) AS sessions_total,
    COUNT(DISTINCT IF(metric_value > 0, session_id, NULL)) AS sessions_with_feature,
    COUNT(DISTINCT user_pseudo_id) AS users_total,
    COUNT(DISTINCT IF(metric_value > 0, user_pseudo_id, NULL)) AS users_with_feature,
    SUM(metric_value) AS feature_events_total,

    SAFE_DIVIDE(SUM(metric_value), NULLIF(COUNT(DISTINCT session_id),0)) AS avg_feature_events_per_session,
    SAFE_DIVIDE(SUM(metric_value), NULLIF(COUNT(DISTINCT IF(metric_value > 0, session_id, NULL)),0)) AS avg_feature_events_per_used_session

  FROM session_long
  GROUP BY
    default_channel_grouping, metric_name,
    is_first_session_only, is_logged_in, is_loyalty_session,
    session_count_bucket_1vMore, loyalty_session_bucket_1vMore, is_new_user_15d
)

SELECT
  default_channel_grouping,
  metric_name,

  -- Slice-able flags
  is_first_session_only,
  is_logged_in,
  is_loyalty_session,
  session_count_bucket_1vMore,
  loyalty_session_bucket_1vMore,
  is_new_user_15d,

  -- Core volumes
  sessions_total,
  sessions_with_feature,
  users_total,
  users_with_feature,
  feature_events_total,

  -- Adoption & intensity
  SAFE_DIVIDE(sessions_with_feature, NULLIF(sessions_total,0)) AS pct_sessions_with_feature,
  SAFE_DIVIDE(users_with_feature, NULLIF(users_total,0))       AS pct_users_with_feature,
  avg_feature_events_per_session,
  avg_feature_events_per_used_session,

  -- Shares:

  -- 1) Across-channels distribution for a given metric & slice (sums to 1 over channels)
  SAFE_DIVIDE(
    feature_events_total,
    NULLIF(SUM(feature_events_total) OVER (
      PARTITION BY metric_name,
                   is_first_session_only, is_logged_in, is_loyalty_session,
                   session_count_bucket_1vMore, loyalty_session_bucket_1vMore, is_new_user_15d
    ), 0)
  ) AS share_metric_events_by_channel,

  -- 2) Within-channel metric composition for a given slice (100% stacked across metrics)
  SAFE_DIVIDE(
    feature_events_total,
    NULLIF(SUM(feature_events_total) OVER (
      PARTITION BY default_channel_grouping,
                   is_first_session_only, is_logged_in, is_loyalty_session,
                   session_count_bucket_1vMore, loyalty_session_bucket_1vMore, is_new_user_15d
    ), 0)
  ) AS within_channel_metric_mix

FROM agg
ORDER BY default_channel_grouping, metric_name;
