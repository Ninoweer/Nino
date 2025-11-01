/* =============================================================================
   APP FEATURE USAGE — Engaged Sessions Only (BigQuery Standard SQL)

   OUTPUTS
   1) SESSION_vs_USER_DISTRIBUTIONS_25BINS  (run the final SELECT #1)
   2) TOP_FEATURE_TOTALS                     (run the final SELECT #2)

   PURPOSE (short):
   - Session-level histogram (0..25, 25+) per metric + % vs ALL engaged sessions.
   - User-level histogram (per-user total usage binned, 0..25, 25+) per metric
     + % vs ALL engaged users.
   - Per-metric totals & user reach for the “all-metrics” page (incl. relatives).
   - Cohort labels you can filter/split on in Looker Studio.

   COHORT / COMPARISON LABELS:
     - login_status:            'Logged In' | 'Guest'
     - is_loyalty_session_label:'Loyalty Card Shown' | 'No Card Shown'
     - is_new_user_15d_label:   'New User (<15d)' | 'Regular User (>15d)'
     - user_new_low_high_bucket_label:
         'New users' | 'Regular Single Session Users' |
         'Regular 2/3 Sessions Users' | 'Regular Frequent Sessions (>3) Users'
     - entry_exit_3lvl_label:   'Average Sessions' | 'First Sessions' | 'Last Sessions'

   BINNING
   - Integer bins 0..25; 25+ is grouped.
   - Session distribution uses per-session counts.
   - User distribution uses per-user TOTAL for that metric (sum across engaged sessions).

   SAFE MATH
   - Uses SAFE_DIVIDE to avoid division by zero.
   ============================================================================= */

/* -------------------------------
 0) Select engaged sessions only
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

    -- feature counters
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
 1) User stats to enable cohorts
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
 2) Order sessions to flag first / last (among engaged sessions)
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
 3) Cohort flags + human-friendly labels for Looker Studio
-------------------------------------------------------------- */
with_flags AS (
  SELECT
    *,
    -- Logged in?
    (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE)) AS is_logged_in,

    -- Loyalty in session (boolean + label)
    IF(IFNULL(loyalty_card_show,0) > 0, TRUE, FALSE) AS is_loyalty_session,
    CASE WHEN IFNULL(loyalty_card_show,0) > 0
         THEN 'Loyalty Card Shown' ELSE 'No Card Shown' END AS is_loyalty_session_label,

    -- New vs regular (by first_seen_date)
    DATE_DIFF(date, first_seen_date, DAY) AS days_since_first_seen,
    CASE WHEN DATE_DIFF(date, first_seen_date, DAY) < 15 THEN TRUE ELSE FALSE END AS is_new_user_15d,
    CASE WHEN DATE_DIFF(date, first_seen_date, DAY) < 15
         THEN 'New User (<15d)' ELSE 'Regular User (>15d)' END AS is_new_user_15d_label,

    -- 4-way tenure bucket
    CASE
      WHEN DATE_DIFF(date, first_seen_date, DAY) < 15                            THEN 'New users'
      WHEN total_engaged_sessions_per_user = 1                                    THEN 'Regular Single Session Users'
      WHEN total_engaged_sessions_per_user BETWEEN 2 AND 3                        THEN 'Regular 2/3 Sessions Users'
      WHEN total_engaged_sessions_per_user > 3                                    THEN 'Regular Frequent Sessions (>3) Users'
    END AS user_new_low_high_bucket_label,

    -- Entry / Exit / Average Sessions (chart label)
    CASE
      WHEN total_engaged_sessions_per_user > 1 AND rn_asc  = 1 THEN 'First Sessions'
      WHEN total_engaged_sessions_per_user > 1 AND rn_desc = 1 THEN 'Last Sessions'
      ELSE 'Average Sessions'
    END AS entry_exit_3lvl_label,

    -- Login status label
    CASE WHEN (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE))
         THEN 'Logged In' ELSE 'Guest' END AS login_status
  FROM with_ord
),

/* -------------------------------------------------------------
 4) Long shape (session × metric_name, metric_value)
    — UNNEST keeps one scan and is easy to maintain
-------------------------------------------------------------- */
session_long AS (
  SELECT
    s.date,
    s.user_pseudo_id,
    s.session_id,

    -- labels usable as breakdown/filters
    s.login_status,
    s.is_loyalty_session,
    s.is_loyalty_session_label,
    s.is_new_user_15d,
    s.is_new_user_15d_label,
    s.user_new_low_high_bucket_label,
    s.entry_exit_3lvl_label,

    m.metric_name,
    m.metric_value
  FROM with_flags s,
  UNNEST([
    STRUCT('promo_views'                AS metric_name, CAST(IFNULL(promo_views,0)                AS INT64) AS metric_value),
    STRUCT('category_screens'           AS metric_name, CAST(IFNULL(category_screens,0)           AS INT64) AS metric_value),
    STRUCT('weekdeals_screens'          AS metric_name, CAST(IFNULL(weekdeals_screens,0)          AS INT64) AS metric_value),
    STRUCT('nextweekdeals_screens'      AS metric_name, CAST(IFNULL(nextweekdeals_screens,0)      AS INT64) AS metric_value),
    STRUCT('searches'                   AS metric_name, CAST(IFNULL(searches,0)                   AS INT64) AS metric_value),
    STRUCT('plp_product_impressions'    AS metric_name, CAST(IFNULL(plp_product_impressions,0)    AS INT64) AS metric_value),
    STRUCT('pdp_views'                  AS metric_name, CAST(IFNULL(pdp_views,0)                  AS INT64) AS metric_value),
    STRUCT('main_page_teasers'          AS metric_name, CAST(IFNULL(main_page_teasers,0)          AS INT64) AS metric_value),
    STRUCT('category_carousel_selected' AS metric_name, CAST(IFNULL(category_carousel_selected,0) AS INT64) AS metric_value),
    STRUCT('products_screens'           AS metric_name, CAST(IFNULL(products_screens,0)           AS INT64) AS metric_value),
    STRUCT('moment_screens'             AS metric_name, CAST(IFNULL(moment_screens,0)             AS INT64) AS metric_value),
    STRUCT('shoppinglist_screens'       AS metric_name, CAST(IFNULL(shoppinglist_screens,0)       AS INT64) AS metric_value),
    STRUCT('myaction_screen'            AS metric_name, CAST(IFNULL(myaction_screen,0)            AS INT64) AS metric_value),
    STRUCT('barcodescanner_open'        AS metric_name, CAST(IFNULL(barcodescanner_open,0)        AS INT64) AS metric_value),
    STRUCT('loyalty_card_show'          AS metric_name, CAST(IFNULL(loyalty_card_show,0)          AS INT64) AS metric_value),
    STRUCT('digital_receipt_views'      AS metric_name, CAST(IFNULL(digital_receipt_views,0)      AS INT64) AS metric_value),
    STRUCT('loyalty_game_entries'       AS metric_name, CAST(IFNULL(loyalty_game_entries,0)       AS INT64) AS metric_value),
    STRUCT('folder_opens'               AS metric_name, CAST(IFNULL(folder_opens,0)               AS INT64) AS metric_value),
    STRUCT('add_to_shoppinglists'       AS metric_name, CAST(IFNULL(add_to_shoppinglists,0)       AS INT64) AS metric_value),
    STRUCT('navigated'                  AS metric_name, CAST(IFNULL(navigated,0)                  AS INT64) AS metric_value),
    STRUCT('webshop_products_viewed'    AS metric_name, CAST(IFNULL(webshop_products_viewed,0)    AS INT64) AS metric_value)
  ]) AS m
),

/* -------------------------------------------------------------
 5) Global denominators for requested relative metrics
-------------------------------------------------------------- */
global_denoms AS (
  SELECT
    COUNT(DISTINCT session_id) AS all_unique_sessions,
    COUNT(DISTINCT user_pseudo_id) AS all_unique_users,
    SUM(metric_value) AS all_metrics_event_count
  FROM session_long
),

/* -------------------------------------------------------------
 6) Session-level binning (0..25, 25+)
-------------------------------------------------------------- */
session_binned AS (
  SELECT
    metric_name,
    CAST(LEAST(ROUND(CAST(metric_value AS FLOAT64)), 25) AS INT64) AS bin_numeric_0_25,
    IF(ROUND(CAST(metric_value AS FLOAT64)) >= 25, '25+', CAST(ROUND(CAST(metric_value AS FLOAT64)) AS STRING)) AS bin_label_0_25,
    session_id,

    -- keep labels for filtering/splitting in charts
    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label,
    entry_exit_3lvl_label
  FROM session_long
),

/* -------------------------------------------------------------
 7) User-level totals per metric (sum across engaged sessions),
    then bin (0..25, 25+)
    - We exclude users with total==0 from the histogram,
      but use ALL engaged users as denominator for % (as requested).
-------------------------------------------------------------- */
user_metric_totals AS (
  SELECT
    metric_name,
    user_pseudo_id,
    SUM(metric_value) AS user_total_for_metric,

    -- Note: these per-user labels are taken arbitrarily from their sessions (for filtering convenience).
    ANY_VALUE(login_status)                   AS login_status,
    ANY_VALUE(is_loyalty_session_label)       AS is_loyalty_session_label,
    ANY_VALUE(is_new_user_15d_label)          AS is_new_user_15d_label,
    ANY_VALUE(user_new_low_high_bucket_label) AS user_new_low_high_bucket_label
  FROM session_long
  GROUP BY metric_name, user_pseudo_id
),
user_binned AS (
  SELECT
    metric_name,
    CAST(LEAST(ROUND(CAST(user_total_for_metric AS FLOAT64)), 25) AS INT64) AS bin_numeric_0_25,
    IF(ROUND(CAST(user_total_for_metric AS FLOAT64)) >= 25, '25+', CAST(ROUND(CAST(user_total_for_metric AS FLOAT64)) AS STRING)) AS bin_label_0_25,
    user_pseudo_id,

    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label
  FROM user_metric_totals
  WHERE user_total_for_metric > 0  -- “users that clicked the metric”
),

/* -------------------------------------------------------------
 8) Totals per metric for the TOP_FEATURES page
-------------------------------------------------------------- */
metric_totals AS (
  SELECT
    sl.metric_name,
    SUM(sl.metric_value) AS total_usage_count,
    -- BigQuery: use conditional DISTINCT instead of FILTER syntax
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
    SAFE_DIVIDE(mt.users_clicking_metric, gd.all_unique_users)     AS relative_users_clicking_pct
  FROM metric_totals mt
  CROSS JOIN global_denoms gd
)

/* =====================================================================
   FINAL SELECT #1 — SESSION_vs_USER_DISTRIBUTIONS_25BINS
   One long table with a `distribution_level` column:
     'session' rows:   counts & percentages vs ALL unique engaged sessions
     'user' rows:      counts & percentages vs ALL unique engaged users
   Use filters on the label columns to build each comparison page.
===================================================================== */
-- SESSION distribution
SELECT
  'session' AS distribution_level,                 -- <- use this for a legend or a parameter
  b.metric_name,
  b.bin_numeric_0_25,
  b.bin_label_0_25,

  b.login_status,
  b.is_loyalty_session_label,
  b.is_new_user_15d_label,
  b.user_new_low_high_bucket_label,
  b.entry_exit_3lvl_label,

  COUNT(DISTINCT b.session_id) AS count_in_bin,
  SAFE_DIVIDE(COUNT(DISTINCT b.session_id), gd.all_unique_sessions) AS pct_of_all_unique_sessions,

  -- optional within-feature percentage (bins sum to 100% per metric)
  SAFE_DIVIDE(
    COUNT(DISTINCT b.session_id),
    SUM(COUNT(DISTINCT b.session_id)) OVER (PARTITION BY b.metric_name)
  ) AS pct_of_sessions_within_feature

FROM session_binned b
CROSS JOIN global_denoms gd
GROUP BY
  b.metric_name, b.bin_numeric_0_25, b.bin_label_0_25,
  b.login_status, b.is_loyalty_session_label, b.is_new_user_15d_label,
  b.user_new_low_high_bucket_label, b.entry_exit_3lvl_label, gd.all_unique_sessions

UNION ALL

-- USER distribution (per-user total usage binned; relative vs ALL engaged users)
SELECT
  'user' AS distribution_level,
  u.metric_name,
  u.bin_numeric_0_25,
  u.bin_label_0_25,

  u.login_status,
  u.is_loyalty_session_label,
  u.is_new_user_15d_label,
  u.user_new_low_high_bucket_label,
  CAST(NULL AS STRING) AS entry_exit_3lvl_label,  -- not meaningful at user grain

  COUNT(DISTINCT u.user_pseudo_id) AS count_in_bin,
  SAFE_DIVIDE(COUNT(DISTINCT u.user_pseudo_id), gd.all_unique_users) AS pct_of_all_unique_users,

  -- optional within-feature percentage (bins sum to 100% per metric)
  SAFE_DIVIDE(
    COUNT(DISTINCT u.user_pseudo_id),
    SUM(COUNT(DISTINCT u.user_pseudo_id)) OVER (PARTITION BY u.metric_name)
  ) AS pct_of_users_within_feature

FROM user_binned u
CROSS JOIN global_denoms gd
GROUP BY
  u.metric_name, u.bin_numeric_0_25, u.bin_label_0_25,
  u.login_status, u.is_loyalty_session_label, u.is_new_user_15d_label,
  u.user_new_low_high_bucket_label, gd.all_unique_users
ORDER BY metric_name, distribution_level, bin_numeric_0_25
;

/* =====================================================================
   FINAL SELECT #2 — TOP_FEATURE_TOTALS
   Per-metric totals for the "all-metrics" page (one row per metric).
   Filter this table in LS using label columns if you duplicate data
   sources with WHEREs (this base output is overall totals).
===================================================================== */
SELECT
  mtr.metric_name,

  -- Totals & relative shares (exactly as requested)
  mtr.total_usage_count,
  mtr.relative_usage_count_pct,         -- vs sum across all metrics
  mtr.users_clicking_metric,
  mtr.relative_users_clicking_pct       -- vs ALL unique engaged users

FROM metric_totals_with_rel mtr
ORDER BY mtr.metric_name
;
