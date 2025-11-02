-- TOP FEATURE TOTALS (per metric × cohort), with cohort flags and top session-level bin
-- 30 bins: 0..30 (30+ groups everything >=30)

WITH engaged AS (
  SELECT
    date,
    user_pseudo_id,
    CAST(session_id AS INT64) AS session_id,
    COALESCE(engaged_session, engaged_sessions) AS engaged_session,
    identified_session,
    login,
    loyalty_card_show,

    promo_views, category_screens, weekdeals_screens, nextweekdeals_screens,
    searches, plp_product_impressions, pdp_views, main_page_teasers,
    category_carousel_selected, products_screens, moment_screens,
    shoppinglist_screens, myaction_screen, barcodescanner_open,
    digital_receipt_views, loyalty_game_entries, folder_opens,
    add_to_shoppinglists, navigated, webshop_products_viewed
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE COALESCE(engaged_session, engaged_sessions) = TRUE
),

user_stats AS (
  SELECT
    user_pseudo_id,
    MIN(date) AS first_seen_date,
    COUNT(DISTINCT session_id) AS total_engaged_sessions_per_user,
    COUNTIF(IFNULL(loyalty_card_show,0) > 0) AS loyalty_show_engaged_sessions_per_user
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
    -- logged-in
    (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE)) AS is_logged_in,
    CASE WHEN (IFNULL(identified_session,FALSE) OR IFNULL(login,FALSE))
         THEN 'Logged In' ELSE 'Guest' END AS login_status,

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

-- long shape: session × metric, include cohort flags
session_long AS (
  SELECT
    s.date,
    s.user_pseudo_id,
    s.session_id,

    s.is_logged_in,
    s.login_status,
    s.is_loyalty_session,
    s.is_loyalty_session_label,
    s.is_new_user_15d,
    s.is_new_user_15d_label,
    s.user_new_low_high_bucket_label,
    s.entry_exit_3lvl_label,

    m.metric_name,
    CAST(IFNULL(m.metric_value,0) AS INT64) AS metric_value
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

-- global denominators
global_denoms AS (
  SELECT
    COUNT(DISTINCT session_id) AS all_unique_sessions,
    COUNT(DISTINCT user_pseudo_id) AS all_unique_users,
    SUM(metric_value) AS all_metrics_event_count
  FROM session_long
),

-- metric totals globally (for pct calculations)
metric_totals AS (
  SELECT
    metric_name,
    SUM(metric_value) AS total_usage_count,
    COUNT(DISTINCT IF(metric_value > 0, user_pseudo_id, NULL)) AS users_clicking_metric
  FROM session_long
  GROUP BY metric_name
),

metric_totals_with_rel AS (
  SELECT
    mt.metric_name,
    mt.total_usage_count,
    SAFE_DIVIDE(mt.total_usage_count, gd.all_metrics_event_count) AS relative_usage_count_pct,
    mt.users_clicking_metric,
    SAFE_DIVIDE(mt.users_clicking_metric, gd.all_unique_users) AS relative_users_clicking_pct_vs_all_users
  FROM metric_totals mt
  CROSS JOIN global_denoms gd
),

-- session-level binning 0..30
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

-- aggregated sums per session-bin + cohort
session_binned_agg AS (
  SELECT
    sb.metric_name,
    sb.bin_label_0_30,
    sb.login_status,
    sb.is_loyalty_session_label,
    sb.is_new_user_15d_label,
    sb.user_new_low_high_bucket_label,
    sb.entry_exit_3lvl_label,

    COUNT(DISTINCT sb.session_id) AS count_in_bin,
    SUM(sb.metric_value) AS sum_metric_value_in_bin

  FROM session_binned sb
  GROUP BY
    sb.metric_name, sb.bin_label_0_30,
    sb.login_status, sb.is_loyalty_session_label, sb.is_new_user_15d_label,
    sb.user_new_low_high_bucket_label, sb.entry_exit_3lvl_label
),

-- attach pct_of_metric_total_value (sum_bin / metric_total) and cohort denominators
session_binned_agg_with_pct AS (
  SELECT
    sba.*,
    mt.total_usage_count,
    SAFE_DIVIDE(sba.sum_metric_value_in_bin, NULLIF(mt.total_usage_count,0)) AS pct_of_metric_total_value
  FROM session_binned_agg sba
  LEFT JOIN metric_totals mt USING (metric_name)
),

-- per metric x cohort totals: sessions, users, total usage, users clicking metric
metric_cohort_agg AS (
  SELECT
    sl.metric_name,
    sl.login_status,
    sl.is_loyalty_session_label,
    sl.is_new_user_15d_label,
    sl.user_new_low_high_bucket_label,
    sl.entry_exit_3lvl_label,

    COUNT(DISTINCT sl.session_id) AS cohort_sessions,
    COUNT(DISTINCT sl.user_pseudo_id) AS cohort_users,
    SUM(sl.metric_value) AS cohort_total_usage_count,
    COUNT(DISTINCT IF(sl.metric_value > 0, sl.user_pseudo_id, NULL)) AS cohort_users_clicking_metric

  FROM session_long sl
  GROUP BY
    sl.metric_name,
    sl.login_status,
    sl.is_loyalty_session_label,
    sl.is_new_user_15d_label,
    sl.user_new_low_high_bucket_label,
    sl.entry_exit_3lvl_label
),

-- pick the top session bin per metric × cohort using ROW_NUMBER to de-correlate
top_session_bin_per_cohort AS (
  SELECT
    metric_name,
    login_status,
    is_loyalty_session_label,
    is_new_user_15d_label,
    user_new_low_high_bucket_label,
    entry_exit_3lvl_label,

    bin_label_0_30 AS top_session_bin_label,
    pct_of_metric_total_value AS top_session_bin_pct_of_metric_total_value,
    sum_metric_value_in_bin AS top_session_bin_sum_metric_value_in_bin,
    count_in_bin AS top_session_bin_count_in_bin,

    ROW_NUMBER() OVER (
      PARTITION BY metric_name, login_status, is_loyalty_session_label, is_new_user_15d_label, user_new_low_high_bucket_label, entry_exit_3lvl_label
      ORDER BY pct_of_metric_total_value DESC NULLS LAST
    ) AS rn
  FROM session_binned_agg_with_pct
)

SELECT
  mca.metric_name,

  -- cohort labels
  mca.login_status,
  mca.is_loyalty_session_label,
  mca.is_new_user_15d_label,
  mca.user_new_low_high_bucket_label,
  mca.entry_exit_3lvl_label,

  -- cohort denominators & raw counts
  mca.cohort_sessions,
  mca.cohort_users,
  mca.cohort_total_usage_count,
  mca.cohort_users_clicking_metric,

  -- cohort-level averages & shares
  SAFE_DIVIDE(mca.cohort_total_usage_count, NULLIF(mca.cohort_users,0)) AS avg_events_per_user_in_cohort,
  SAFE_DIVIDE(mca.cohort_total_usage_count, NULLIF(mca.cohort_users_clicking_metric,0)) AS avg_events_per_active_user_in_cohort,
  SAFE_DIVIDE(mca.cohort_total_usage_count, NULLIF(mca.cohort_sessions,0)) AS avg_events_per_session_in_cohort,

  -- share vs metric-global & vs all metrics
  SAFE_DIVIDE(mca.cohort_total_usage_count, NULLIF(mt.total_usage_count,0)) AS pct_of_metric_total_value_in_cohort,
  SAFE_DIVIDE(mca.cohort_total_usage_count, NULLIF(gd.all_metrics_event_count,0)) AS pct_of_all_metrics_event_count,

  -- global metric totals/reach for convenience
  mt.total_usage_count AS metric_total_usage_count,
  mt.relative_usage_count_pct AS metric_relative_usage_vs_all_metrics,
  mt.users_clicking_metric AS metric_users_clicking,

  -- top session-level bin in this cohort
  ts.top_session_bin_label,
  ts.top_session_bin_pct_of_metric_total_value,
  ts.top_session_bin_sum_metric_value_in_bin,
  ts.top_session_bin_count_in_bin

FROM metric_cohort_agg mca
LEFT JOIN metric_totals_with_rel mt USING (metric_name)
CROSS JOIN global_denoms gd

LEFT JOIN (
  SELECT * EXCEPT(rn) FROM top_session_bin_per_cohort WHERE rn = 1
) ts
  ON ts.metric_name = mca.metric_name
 AND ts.login_status = mca.login_status
 AND ts.is_loyalty_session_label = mca.is_loyalty_session_label
 AND ts.is_new_user_15d_label = mca.is_new_user_15d_label
 AND ts.user_new_low_high_bucket_label = mca.user_new_low_high_bucket_label
 AND ts.entry_exit_3lvl_label = mca.entry_exit_3lvl_label

ORDER BY mca.metric_name, mca.login_status, mca.is_loyalty_session_label, mca.is_new_user_15d_label;
