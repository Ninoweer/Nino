/* =============================================================================
   APP FEATURE USAGE — Engaged Sessions Only (BigQuery Standard SQL)
   - 30 bins: 0..30 (30 groups everything >=30 as '30+')
   OUTPUTS:
     - SESSION_vs_USER_DISTRIBUTIONS_30BINS  (first FINAL SELECT)
     - TOP_FEATURE_TOTALS                     (second FINAL SELECT)
   - Includes:
     * session-level distributions (per-session counts)
     * user-level totals distributions (per-user total across engaged sessions)
     * user-level average-per-session distributions (per-user average across engaged sessions, rounded)
     * per-bin total metric counts and relative shares
     * per-bin aggregated averages and ratio of metric / total user event load
================================================================================ */

/* -------------------------------
 0) Engaged sessions only
-------------------------------- */
WITH engaged AS (
  SELECT
    date,
    user_pseudo_id,
    CAST(session_id AS INT64) AS session_id,
    COALESCE(engaged_session, engaged_sessions) AS engaged_session,
    identified_session,
    login,
    loyalty_card_show,

    -- feature counters (NULLs left as-is; we will IFNULL to 0 downstream)
    promo_views, 
    category_screens, 
    weekdeals_screens, 
    nextweekdeals_screens, 
    searches,
    plp_product_impressions, 
    pdp_views, 
    main_page_teasers, 
    category_carousel_selected,
    products_screens, 
    moment_screens, 
    shoppinglist_screens, 
    myaction_screen,
    barcodescanner_open, 
    digital_receipt_views, 
    loyalty_game_entries,
    folder_opens,
    add_to_shoppinglists, 
    navigated, 
    webshop_products_viewed
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE COALESCE(engaged_session, engaged_sessions) = TRUE
),

/* -------------------------------
 1) User stats (tenure & counts)
-------------------------------- */
user_stats AS (
  SELECT
    user_pseudo_id,
    MIN(date) AS first_seen_date,
    COUNT(DISTINCT session_id) AS total_engaged_sessions_per_user,
    COUNTIF(IFNULL(loyalty_card_show,0) > 0) AS loyalty_show_engaged_sessions_per_user
  FROM engaged
  GROUP BY user_pseudo_id
),

/* -------------------------------------------------------------
 2) Number sessions ordered per user to detect first/last
-------------------------------------------------------------- */
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

/* -------------------------------------------------------------
 3) Cohort flags + friendly labels
-------------------------------------------------------------- */
with_flags AS (
  SELECT
    *,
    -- logged-in
    (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE)) AS is_logged_in,
    CASE WHEN (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE)) THEN 'Logged In' ELSE 'Guest' END AS login_status,

    -- loyalty
    IF(IFNULL(loyalty_card_show,0) > 0, TRUE, FALSE) AS is_loyalty_session,
    CASE WHEN IFNULL(loyalty_card_show,0) > 0 THEN 'Loyalty Card Shown' ELSE 'No Card Shown' END AS is_loyalty_session_label,

    -- new vs regular
    DATE_DIFF(date, first_seen_date, DAY) AS days_since_first_seen,
    CASE WHEN DATE_DIFF(date, first_seen_date, DAY) < 15 THEN TRUE ELSE FALSE END AS is_new_user_15d,
    CASE WHEN DATE_DIFF(date, first_seen_date, DAY) < 15 THEN 'New User (<15d)' ELSE 'Regular User (>15d)' END AS is_new_user_15d_label,

    -- 4-way tenure label
    CASE
      WHEN DATE_DIFF(date, first_seen_date, DAY) < 15                            THEN 'New users'
      WHEN total_engaged_sessions_per_user = 1                                    THEN 'Regular Single Session Users'
      WHEN total_engaged_sessions_per_user BETWEEN 2 AND 3                        THEN 'Regular 2/3 Sessions Users'
      WHEN total_engaged_sessions_per_user > 3                                    THEN 'Regular Frequent Sessions (>3) Users'
    END AS user_new_low_high_bucket_label,

    -- entry/exit/average sessions
    CASE
      WHEN total_engaged_sessions_per_user > 1 AND rn_asc  = 1 THEN 'First Sessions'
      WHEN total_engaged_sessions_per_user > 1 AND rn_desc = 1 THEN 'Last Sessions'
      ELSE 'Average Sessions'
    END AS entry_exit_3lvl_label
  FROM with_ord
),

/* -------------------------------------------------------------
 4) Long shape: one row per session × metric_name
    - UNNEST of structs gives metric_name, metric_value in a single scan
-------------------------------------------------------------- */
session_long AS (
  SELECT
    s.date,
    s.user_pseudo_id,
    s.session_id,

    -- cohort labels
    s.login_status,
    s.is_loyalty_session,
    s.is_loyalty_session_label,
    s.is_new_user_15d,
    s.is_new_user_15d_label,
    s.user_new_low_high_bucket_label,
    s.entry_exit_3lvl_label,

    m.metric_name,
    CAST(IFNULL(m.metric_value,0) AS INT64) AS metric_value   -- NULL-safe
  FROM with_flags s,
  UNNEST([
    STRUCT('promo_views'                AS metric_name, promo_views                AS metric_value),
    STRUCT('category_screens'           AS metric_name, category_screens           AS metric_value),
    STRUCT('weekdeals_screens'          AS metric_name, weekdeals_screens          AS metric_value),
    STRUCT('nextweekdeals_screens'      AS metric_name, nextweekdeals_screens      AS metric_value),
    STRUCT('searches'                   AS metric_name, searches                   AS metric_value),
    STRUCT('plp_product_impressions'    AS metric_name, plp_product_impressions    AS metric_value),
    STRUCT('pdp_views'                  AS metric_name, pdp_views                  AS metric_value),
    STRUCT('main_page_teasers'          AS metric_name, main_page_teasers          AS metric_value),
    STRUCT('category_carousel_selected' AS metric_name, category_carousel_selected AS metric_value),
    STRUCT('products_screens'           AS metric_name, products_screens           AS metric_value),
    STRUCT('moment_screens'             AS metric_name, moment_screens             AS metric_value),
    STRUCT('shoppinglist_screens'       AS metric_name, shoppinglist_screens       AS metric_value),
    STRUCT('myaction_screen'            AS metric_name, myaction_screen            AS metric_value),
    STRUCT('barcodescanner_open'        AS metric_name, barcodescanner_open        AS metric_value),
    STRUCT('loyalty_card_show'          AS metric_name, loyalty_card_show          AS metric_value),
    STRUCT('digital_receipt_views'      AS metric_name, digital_receipt_views      AS metric_value),
    STRUCT('loyalty_game_entries'       AS metric_name, loyalty_game_entries       AS metric_value),
    STRUCT('folder_opens'               AS metric_name, folder_opens               AS metric_value),
    STRUCT('add_to_shoppinglists'       AS metric_name, add_to_shoppinglists       AS metric_value),
    STRUCT('navigated'                  AS metric_name, navigated                  AS metric_value),
    STRUCT('webshop_products_viewed'    AS metric_name, webshop_products_viewed    AS metric_value)
  ]) AS m
),

/* -------------------------------------------------------------
 5) Global denominators used for percentages
-------------------------------------------------------------- */
global_denoms AS (
  SELECT
    COUNT(DISTINCT session_id) AS all_unique_sessions,
    COUNT(DISTINCT user_pseudo_id) AS all_unique_users,
    SUM(metric_value) AS all_metrics_event_count,
    COUNT(DISTINCT IF(metric_value > 0, user_pseudo_id, NULL)) AS users_clicked_any_metric
  FROM session_long
),

/* -------------------------------------------------------------
 6) Session-level binning (0..30, 30+)
    include metric_value so we can derive per-bin totals
-------------------------------------------------------------- */
session_binned AS (
  SELECT
    metric_name,
    CAST(LEAST(ROUND(CAST(metric_value AS FLOAT64)), 30) AS INT64) AS bin_numeric_0_30,
    IF(ROUND(CAST(metric_value AS FLOAT64)) >= 30,'30+',CAST(ROUND(CAST(metric_value AS FLOAT64)) AS STRING)) AS bin_label_0_30,
    session_id,
    metric_value,

    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label,
    entry_exit_3lvl_label
  FROM session_long
),

/* -------------------------------------------------------------
 7) User-level totals per metric (sum across engaged sessions)
   - We'll use these for user_total distributions
   - ANY_VALUE(labels) used for convenience; if you need deterministic per-user
     labels we can compute them by timestamp or mode.
-------------------------------------------------------------- */
user_metric_totals AS (
  SELECT
    metric_name,
    user_pseudo_id,
    SUM(metric_value) AS user_total_for_metric,
    ANY_VALUE(login_status)                   AS login_status,
    ANY_VALUE(is_loyalty_session_label)       AS is_loyalty_session_label,
    ANY_VALUE(is_new_user_15d_label)          AS is_new_user_15d_label,
    ANY_VALUE(user_new_low_high_bucket_label) AS user_new_low_high_bucket_label
  FROM session_long
  GROUP BY metric_name, user_pseudo_id
),

/* -------------------------------------------------------------
 7b) user_total_events: the total count of ALL metrics for each user
    (used to compute the proportion of a user's event load that this metric occupies)
-------------------------------------------------------------- */
user_total_events AS (
  SELECT
    user_pseudo_id,
    SUM(metric_value) AS user_total_events -- total across all metrics & sessions
  FROM session_long
  GROUP BY user_pseudo_id
),

/* -------------------------------------------------------------
 7c) Per-user, per-metric statistics including averages:
    - user_avg_metric_per_session_float  = user_total_for_metric / total_engaged_sessions_per_user
    - user_avg_metric_per_session_rounded = rounded integer for binning & display
    - user_avg_total_events_per_session_float = user_total_events / total_engaged_sessions_per_user
    - user_avg_metric_pct_of_user_totalavg = user_avg_metric_per_session_float / user_avg_total_events_per_session_float
-------------------------------------------------------------- */
user_metric_stats AS (
  SELECT
    umt.metric_name,
    umt.user_pseudo_id,
    umt.user_total_for_metric,
    us.total_engaged_sessions_per_user,

    -- per-user average of this metric per engaged session (float)
    SAFE_DIVIDE(umt.user_total_for_metric, us.total_engaged_sessions_per_user) AS user_avg_metric_per_session_float,

    -- rounded average for binning/display (whole numbers)
    CAST(ROUND(SAFE_DIVIDE(umt.user_total_for_metric, us.total_engaged_sessions_per_user)) AS INT64) AS user_avg_metric_per_session_rounded,

    -- per-user average total events per session (float)
    SAFE_DIVIDE(utev.user_total_events, us.total_engaged_sessions_per_user) AS user_avg_total_events_per_session_float,

    -- ratio (not rounded): metric avg / total-events avg (use SAFE_DIVIDE)
    SAFE_DIVIDE(
      SAFE_DIVIDE(umt.user_total_for_metric, us.total_engaged_sessions_per_user),
      SAFE_DIVIDE(utev.user_total_events, us.total_engaged_sessions_per_user)
    ) AS user_avg_metric_pct_of_user_totalavg,

    -- labels (any_value)
    umt.login_status,
    umt.is_loyalty_session_label,
    umt.is_new_user_15d_label,
    umt.user_new_low_high_bucket_label
  FROM user_metric_totals umt
  JOIN user_stats us USING (user_pseudo_id)
  LEFT JOIN user_total_events utev USING (user_pseudo_id)
),

/* -------------------------------------------------------------
 8) User-level binned by TOTAL (user_total_for_metric) — this preserves previous user_total bins
-------------------------------------------------------------- */
user_binned_total AS (
  SELECT
    metric_name,
    CAST(LEAST(ROUND(CAST(user_total_for_metric AS FLOAT64)), 30) AS INT64) AS bin_numeric_0_30,
    IF(ROUND(CAST(user_total_for_metric AS FLOAT64)) >= 30, '30+', CAST(ROUND(CAST(user_total_for_metric AS FLOAT64)) AS STRING)) AS bin_label_0_30,
    user_pseudo_id,
    user_total_for_metric,

    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label
  FROM user_metric_totals
  WHERE user_total_for_metric >= 0  -- keep users including zero if desired; if you want exclude zeros, change to >0
),

/* -------------------------------------------------------------
 9) User-level binned by AVG PER SESSION (rounded) — the new "user_avg" distribution
-------------------------------------------------------------- */
user_binned_avg AS (
  SELECT
    metric_name,
    CAST(LEAST(ROUND(CAST(user_avg_metric_per_session_float AS FLOAT64)), 30) AS INT64) AS bin_numeric_0_30,
    IF(ROUND(CAST(user_avg_metric_per_session_float AS FLOAT64)) >= 30, '30+', CAST(ROUND(CAST(user_avg_metric_per_session_float AS FLOAT64)) AS STRING)) AS bin_label_0_30,
    user_pseudo_id,
    user_total_for_metric,

    -- per-user values retained for aggregation
    user_avg_metric_per_session_float,
    user_avg_metric_per_session_rounded,
    user_avg_total_events_per_session_float,
    user_avg_metric_pct_of_user_totalavg,

    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label
  FROM user_metric_stats
  WHERE total_engaged_sessions_per_user > 0
),

/* -------------------------------------------------------------
 10) Metric totals (needed to compute percentages of metric-value by bin)
-------------------------------------------------------------- */
metric_totals AS (
  SELECT
    sl.metric_name,
    SUM(sl.metric_value) AS total_usage_count,
    COUNT(DISTINCT IF(sl.metric_value > 0, sl.user_pseudo_id, NULL)) AS users_clicking_metric
  FROM session_long sl
  GROUP BY sl.metric_name
),
metric_totals_with_rel AS (
  SELECT
    mt.metric_name,
    mt.total_usage_count,
    SAFE_DIVIDE(mt.total_usage_count, gd.all_metrics_event_count) AS relative_usage_count_pct,
    mt.users_clicking_metric,
    SAFE_DIVIDE(mt.users_clicking_metric, gd.all_unique_users) AS relative_users_clicking_pct_vs_all_users,
    SAFE_DIVIDE(mt.users_clicking_metric, gd.users_clicked_any_metric) AS relative_users_clicking_pct_vs_users_clicked_any_metric
  FROM metric_totals mt
  CROSS JOIN global_denoms gd
)

/* =====================================================================
   FINAL SELECT #1 — SESSION_vs_USER_DISTRIBUTIONS_30BINS
   This output contains three distribution types in one long table:
     - distribution_level = 'session'   : per-session bins
     - distribution_level = 'user_total' : per-user total bins
     - distribution_level = 'user_avg'   : per-user average-per-session bins (rounded)
   Use filters/controls in Looker Studio to display the distribution you want.
===================================================================== */

-- 1) SESSION rows
SELECT
  'session' AS distribution_level,
  b.metric_name,
  b.bin_numeric_0_30,
  b.bin_label_0_30,

  b.login_status,
  b.is_loyalty_session_label,
  b.is_new_user_15d_label,
  b.user_new_low_high_bucket_label,
  b.entry_exit_3lvl_label,

  -- counts
  COUNT(DISTINCT b.session_id) AS count_in_bin,
  SAFE_DIVIDE(COUNT(DISTINCT b.session_id), gd.all_unique_sessions) AS pct_of_all_unique_sessions,
  SAFE_DIVIDE(
    COUNT(DISTINCT b.session_id),
    SUM(COUNT(DISTINCT b.session_id)) OVER (PARTITION BY b.metric_name)
  ) AS pct_of_sessions_within_feature,

  -- totals of metric values for this bin (session-level sum)
  SUM(b.metric_value) AS total_metric_value_in_bin,
  -- percent of the metric's total usage that this bin accounts for
  SAFE_DIVIDE(SUM(b.metric_value), mt.total_usage_count) AS pct_of_metric_value_within_feature,

  -- Nulls for user-average fields (not applicable)
  CAST(NULL AS INT64) AS avg_user_avg_metric_per_session_rounded,
  CAST(NULL AS FLOAT64) AS avg_user_avg_metric_per_session_float,
  CAST(NULL AS FLOAT64) AS avg_user_avg_total_events_per_session_float,
  CAST(NULL AS FLOAT64) AS avg_user_avg_metric_pct_of_user_totalavg

FROM session_binned b
LEFT JOIN metric_totals mt ON mt.metric_name = b.metric_name
CROSS JOIN global_denoms gd
GROUP BY
  b.metric_name, b.bin_numeric_0_30, b.bin_label_0_30,
  b.login_status, b.is_loyalty_session_label, b.is_new_user_15d_label,
  b.user_new_low_high_bucket_label, b.entry_exit_3lvl_label, mt.total_usage_count, gd.all_unique_sessions

UNION ALL

-- 2) USER_TOTAL rows (per-user TOTALs binned)
SELECT
  'user_total' AS distribution_level,
  u.metric_name,
  u.bin_numeric_0_30,
  u.bin_label_0_30,

  u.login_status,
  u.is_loyalty_session_label,
  u.is_new_user_15d_label,
  u.user_new_low_high_bucket_label,
  CAST(NULL AS STRING) AS entry_exit_3lvl_label,  -- not meaningful at user grain

  COUNT(DISTINCT u.user_pseudo_id) AS count_in_bin,
  SAFE_DIVIDE(COUNT(DISTINCT u.user_pseudo_id), gd.all_unique_users) AS pct_of_all_unique_users,
  SAFE_DIVIDE(
    COUNT(DISTINCT u.user_pseudo_id),
    SUM(COUNT(DISTINCT u.user_pseudo_id)) OVER (PARTITION BY u.metric_name)
  ) AS pct_of_users_within_feature,

  -- total metric value contributed by users in this bin (sum of their user_totals)
  SUM(u.user_total_for_metric) AS total_metric_value_in_bin,
  SAFE_DIVIDE(SUM(u.user_total_for_metric), mt.total_usage_count) AS pct_of_metric_value_within_feature,

  -- Nulls for user-average fields (not applicable for this distribution)
  CAST(NULL AS INT64) AS avg_user_avg_metric_per_session_rounded,
  CAST(NULL AS FLOAT64) AS avg_user_avg_metric_per_session_float,
  CAST(NULL AS FLOAT64) AS avg_user_avg_total_events_per_session_float,
  CAST(NULL AS FLOAT64) AS avg_user_avg_metric_pct_of_user_totalavg

FROM user_binned_total u
LEFT JOIN metric_totals mt ON mt.metric_name = u.metric_name
CROSS JOIN global_denoms gd
GROUP BY
  u.metric_name, u.bin_numeric_0_30, u.bin_label_0_30,
  u.login_status, u.is_loyalty_session_label, u.is_new_user_15d_label,
  u.user_new_low_high_bucket_label, mt.total_usage_count, gd.all_unique_users

UNION ALL

-- 3) USER_AVG rows (per-user AVERAGE-per-session binned, rounded)
SELECT
  'user_avg' AS distribution_level,
  a.metric_name,
  a.bin_numeric_0_30,
  a.bin_label_0_30,

  a.login_status,
  a.is_loyalty_session_label,
  a.is_new_user_15d_label,
  a.user_new_low_high_bucket_label,
  CAST(NULL AS STRING) AS entry_exit_3lvl_label,  -- not meaningful at user grain

  COUNT(DISTINCT a.user_pseudo_id) AS count_in_bin,
  SAFE_DIVIDE(COUNT(DISTINCT a.user_pseudo_id), gd.all_unique_users) AS pct_of_all_unique_users,
  SAFE_DIVIDE(
    COUNT(DISTINCT a.user_pseudo_id),
    SUM(COUNT(DISTINCT a.user_pseudo_id)) OVER (PARTITION BY a.metric_name)
  ) AS pct_of_users_within_feature,

  -- total metric value contributed by the users in this avg-bin (sum of their user totals)
  SUM(a.user_total_for_metric) AS total_metric_value_in_bin,
  SAFE_DIVIDE(SUM(a.user_total_for_metric), mt.total_usage_count) AS pct_of_metric_value_within_feature,

  -- aggregated user-average measures for users in the bin:
  -- average of their rounded per-user averages
  AVG(a.user_avg_metric_per_session_rounded) AS avg_user_avg_metric_per_session_rounded,
  -- average of their unrounded per-user averages
  AVG(a.user_avg_metric_per_session_float) AS avg_user_avg_metric_per_session_float,
  -- average of per-user average total events/session
  AVG(a.user_avg_total_events_per_session_float) AS avg_user_avg_total_events_per_session_float,
  -- average of per-user ratios (metric avg / total-events avg) - NOT rounded
  AVG(a.user_avg_metric_pct_of_user_totalavg) AS avg_user_avg_metric_pct_of_user_totalavg

FROM user_binned_avg a
LEFT JOIN metric_totals mt ON mt.metric_name = a.metric_name
CROSS JOIN global_denoms gd
GROUP BY
  a.metric_name, a.bin_numeric_0_30, a.bin_label_0_30,
  a.login_status, a.is_loyalty_session_label, a.is_new_user_15d_label,
  a.user_new_low_high_bucket_label, mt.total_usage_count, gd.all_unique_users

ORDER BY metric_name, distribution_level, bin_numeric_0_30
;

/* =====================================================================
   FINAL SELECT #2 — TOP_FEATURE_TOTALS
   Per-metric totals for the "all-metrics" page (one row per metric).
===================================================================== */
SELECT
  mtr.metric_name,

  mtr.total_usage_count,
  mtr.relative_usage_count_pct,         -- usage vs all events across all metrics

  mtr.users_clicking_metric,
  mtr.relative_users_clicking_pct_vs_all_users,               -- users clicking metric / all unique engaged users
  mtr.relative_users_clicking_pct_vs_users_clicked_any_metric -- users clicking metric / users who clicked any metric
FROM metric_totals_with_rel mtr
ORDER BY mtr.metric_name
;
