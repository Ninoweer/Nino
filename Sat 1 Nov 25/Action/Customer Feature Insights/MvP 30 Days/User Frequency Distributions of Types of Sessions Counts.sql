-- =====================================================================
-- DISTRIBUTIONS OF PER-USER ENGAGED SESSION COUNTS (25 BINS)
-- Single unified output for sections:
--   Single_Series) Single-series 25-bin histograms (Engaged/Loyalty/Logged-in/Identified/New-user)
--   Comparisons)   Comparison 25-bin histograms (Yes/No for Logged-in/New-user/Loyalty/Identified)
--   Delta_avg)     Deltas (GroupA − GroupB) of average sessions per user
--   Score)         Scorecards (top-of-dashboard KPIs)
--
-- intent:
--   1) Work only with ENGAGED sessions for series & comparisons:
--        COALESCE(engaged_session, engaged_sessions) = TRUE
--   2) Count each session once via DISTINCT (user_pseudo_id, session_id).
--   3) Build per-user totals:
--        - Engaged sessions (all engaged)
--        - Loyalty sessions        (engaged & loyalty_card_show > 0)
--        - Logged-in sessions      (engaged & login = TRUE)
--        - Identified sessions     (engaged & identified_session = TRUE)
--        - New-user sessions       (engaged & session_date <= first_seen_date + 15 days)
--   4) Convert per-user totals (first 5 series) into 25 bins (25="25+").
--   5) Output unified table with discriminator column `section` in
--      {'Single_Series','Comparisons','Delta_avg','Score'}.
--
-- IMPORTANT on "new-user sessions"
--   We define first_seen_date as the MIN(date) for each user in the ENTIRE BASE TABLE,
--   not just the report window. This avoids classifying long-standing users as "new"
--   when the report window starts after their true first activity (left-censoring).
--
-- -------------------------
-- PARAMETERS (edit as needed)
-- -------------------------
DECLARE report_start DATE DEFAULT DATE('2025-01-01');  -- inclusive
DECLARE report_end   DATE DEFAULT DATE('2025-10-31');  -- inclusive

-- =====================================================================
-- 1) Establish each user's true first_seen_date from the entire base table
-- =====================================================================
WITH user_first_seen AS (
  SELECT
    user_pseudo_id,
    MIN(date) AS first_seen_date
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE user_pseudo_id IS NOT NULL
  GROUP BY user_pseudo_id
),

-- =====================================================================
-- 2) Pull engaged sessions within the REPORT WINDOW; normalize types; add flags
-- =====================================================================
engaged_sessions AS (
  SELECT DISTINCT
    s.user_pseudo_id,
    SAFE_CAST(s.session_id AS INT64) AS session_id,
    s.date AS session_date,

    -- Normalize per-session flags; NULL -> safe defaults.
    IFNULL(s.login, FALSE)               AS is_logged_in,
    IFNULL(s.identified_session, FALSE)  AS is_identified,
    (IFNULL(s.loyalty_card_show, 0) > 0) AS has_loyalty
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount` AS s
  WHERE
    s.date BETWEEN report_start AND report_end                            -- report window
    AND COALESCE(s.engaged_session, s.engaged_sessions) = TRUE            -- engaged only
    AND s.user_pseudo_id IS NOT NULL
    AND SAFE_CAST(s.session_id AS INT64) IS NOT NULL                      -- must have valid session key
),

-- =====================================================================
-- 3) Attach first_seen_date and compute new-user-at-session flag
--    Definition: session is "new-user" if session_date <= first_seen_date + 15 days
-- =====================================================================
engaged_with_lifecycle AS (
  SELECT
    e.user_pseudo_id,
    e.session_id,
    e.session_date,
    e.is_logged_in,
    e.is_identified,
    e.has_loyalty,
    ufs.first_seen_date,
    CASE
      WHEN e.session_date <= DATE_ADD(ufs.first_seen_date, INTERVAL 15 DAY)
      THEN TRUE ELSE FALSE
    END AS is_new_user_session
  FROM engaged_sessions AS e
  JOIN user_first_seen AS ufs
    ON ufs.user_pseudo_id = e.user_pseudo_id
),

-- =====================================================================
-- 4) Per-user totals across engaged sessions (in-window)
-- =====================================================================
per_user AS (
  SELECT
    user_pseudo_id,

    -- Explicit COUNT of DISTINCT sessions to avoid ambiguity
    COUNT(DISTINCT session_id)                                       AS engaged_sessions,

    COUNT(DISTINCT IF(has_loyalty,           session_id, NULL))      AS loyalty_sessions,
    COUNT(DISTINCT IF(is_logged_in,          session_id, NULL))      AS logged_in_sessions,
    COUNT(DISTINCT IF(is_identified,         session_id, NULL))      AS identified_sessions,

    -- NEW-USER definition by lifecycle window (15 days from true first_seen_date)
    COUNT(DISTINCT IF(is_new_user_session,   session_id, NULL))      AS new_user_sessions
  FROM engaged_with_lifecycle
  GROUP BY user_pseudo_id
),

-- =====================================================================
-- 5) Single-series 25-bin histograms (Single_Series)
-- =====================================================================
long_user_series AS (
  SELECT user_pseudo_id, 'Engaged Sessions'      AS series, engaged_sessions      AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Loyalty Sessions'      AS series, loyalty_sessions      AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Logged-in Sessions'    AS series, logged_in_sessions    AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Identified Sessions'   AS series, identified_sessions   AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'New-user Sessions'     AS series, new_user_sessions     AS session_count FROM per_user
),
nonzero AS (
  SELECT series, user_pseudo_id, session_count
  FROM long_user_series
  WHERE session_count > 0
),
bucketed AS (
  SELECT
    series,
    user_pseudo_id,
    LEAST(session_count, 25) AS session_bucket
  FROM nonzero
),
freq AS (
  SELECT
    series,
    session_bucket,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM bucketed
  GROUP BY series, session_bucket
),
final_bins AS (
  SELECT
    f.series,
    f.session_bucket,
    CASE WHEN f.session_bucket = 25 THEN '25+' ELSE CAST(f.session_bucket AS STRING) END AS bucket_label,
    f.users,
    SAFE_DIVIDE(
      f.users,
      NULLIF(SUM(f.users) OVER (PARTITION BY f.series), 0)
    ) AS users_pct_users
  FROM freq AS f
),

-- =====================================================================
-- 6) Comparison 25-bin histograms (Comparisons):
--     Groups and labels standardized across the pipeline.
--     Dimensions:
--       * 'Logged_vs_Guest'      : groups {'Logged in','Guest'}
--       * 'New_vs_Recurring'     : groups {'New User','Recurring'}
--       * 'Loyalty_vs_NoCard'    : groups {'Loyalty Card Shown','No Card Shown'}
--       * 'Identified_vs_Anonymous': groups {'Identified','Anonymous'}
-- =====================================================================
per_user_by_group AS (
  -- Logged vs Guest
  SELECT user_pseudo_id, 'Logged_vs_Guest' AS comparison_dimension, 'Logged in' AS comparison_group,
         COUNT(DISTINCT IF(is_logged_in,        session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id
  UNION ALL
  SELECT user_pseudo_id, 'Logged_vs_Guest', 'Guest',
         COUNT(DISTINCT IF(NOT is_logged_in,    session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id

  UNION ALL
  -- New vs Recurring
  SELECT user_pseudo_id, 'New_vs_Recurring', 'New User',
         COUNT(DISTINCT IF(is_new_user_session, session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id
  UNION ALL
  SELECT user_pseudo_id, 'New_vs_Recurring', 'Recurring',
         COUNT(DISTINCT IF(NOT is_new_user_session, session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id

  UNION ALL
  -- Loyalty vs No Card
  SELECT user_pseudo_id, 'Loyalty_vs_NoCard', 'Loyalty Card Shown',
         COUNT(DISTINCT IF(has_loyalty,         session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id
  UNION ALL
  SELECT user_pseudo_id, 'Loyalty_vs_NoCard', 'No Card Shown',
         COUNT(DISTINCT IF(NOT has_loyalty,     session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id

  UNION ALL
  -- Identified vs Anonymous
  SELECT user_pseudo_id, 'Identified_vs_Anonymous', 'Identified',
         COUNT(DISTINCT IF(is_identified,       session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id
  UNION ALL
  SELECT user_pseudo_id, 'Identified_vs_Anonymous', 'Anonymous',
         COUNT(DISTINCT IF(NOT is_identified,   session_id, NULL)) AS session_count
  FROM engaged_with_lifecycle GROUP BY user_pseudo_id
),
per_user_by_group_nonzero AS (
  SELECT * FROM per_user_by_group WHERE session_count > 0
),
per_user_by_group_bucketed AS (
  SELECT
    comparison_dimension,
    comparison_group,
    user_pseudo_id,
    LEAST(session_count, 25) AS session_bucket
  FROM per_user_by_group_nonzero
),
comparison_bins AS (
  SELECT
    comparison_dimension,
    comparison_group,
    session_bucket,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM per_user_by_group_bucketed
  GROUP BY comparison_dimension, comparison_group, session_bucket
),
final_comparison_bins AS (
  SELECT
    comparison_dimension,
    comparison_group,
    session_bucket,
    CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
    users,
    SAFE_DIVIDE(
      users,
      NULLIF(SUM(users) OVER (PARTITION BY comparison_dimension, comparison_group), 0)
    ) AS users_pct_users
  FROM comparison_bins
),

-- =====================================================================
-- 7) Deltas (Delta_avg): average sessions per user in each group + (GroupA − GroupB)
--     We define GroupA and GroupB explicitly per dimension to ensure consistent deltas:
--       Logged_vs_Guest      : 'Logged in'       − 'Guest'
--       New_vs_Recurring     : 'New User'        − 'Recurring'
--       Loyalty_vs_NoCard    : 'Loyalty Card Shown' − 'No Card Shown'
--       Identified_vs_Anonymous: 'Identified'    − 'Anonymous'
-- =====================================================================
group_averages AS (
  SELECT
    comparison_dimension,
    comparison_group,
    AVG(session_count) AS avg_sessions_per_user,
    COUNT(DISTINCT user_pseudo_id) AS users_in_group
  FROM per_user_by_group_nonzero
  GROUP BY comparison_dimension, comparison_group
),
-- helper mapping for A/B group names per dimension
dim_pairs AS (
  SELECT 'Logged_vs_Guest' AS comparison_dimension, 'Logged in' AS group_a, 'Guest' AS group_b UNION ALL
  SELECT 'New_vs_Recurring', 'New User', 'Recurring' UNION ALL
  SELECT 'Loyalty_vs_NoCard', 'Loyalty Card Shown', 'No Card Shown' UNION ALL
  SELECT 'Identified_vs_Anonymous', 'Identified', 'Anonymous'
),
comparison_deltas AS (
  SELECT
    p.comparison_dimension,
    a.avg_sessions_per_user AS avg_sessions_group_a,
    b.avg_sessions_per_user AS avg_sessions_group_b,
    SAFE_SUBTRACT(a.avg_sessions_per_user, b.avg_sessions_per_user) AS delta_group_a_minus_b,
    a.users_in_group AS users_in_group_a,
    b.users_in_group AS users_in_group_b,
    p.group_a,
    p.group_b
  FROM dim_pairs p
  LEFT JOIN group_averages a
    ON a.comparison_dimension = p.comparison_dimension
   AND a.comparison_group     = p.group_a
  LEFT JOIN group_averages b
    ON b.comparison_dimension = p.comparison_dimension
   AND b.comparison_group     = p.group_b
),

-- =====================================================================
-- 8) Scorecards (Score): totals & percentages
-- =====================================================================
all_sessions_in_window AS (
  SELECT DISTINCT
    user_pseudo_id,
    SAFE_CAST(session_id AS INT64) AS session_id,
    date AS session_date
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE
    date BETWEEN report_start AND report_end
    AND user_pseudo_id IS NOT NULL
    AND SAFE_CAST(session_id AS INT64) IS NOT NULL
),
engaged_totals AS (
  SELECT
    COUNT(DISTINCT session_id) AS engaged_sessions_total,
    COUNT(DISTINCT IF(is_logged_in,        session_id, NULL)) AS logged_in_sessions_total,
    COUNT(DISTINCT IF(is_identified,       session_id, NULL)) AS identified_sessions_total,
    COUNT(DISTINCT IF(has_loyalty,         session_id, NULL)) AS loyalty_sessions_total,
    COUNT(DISTINCT IF(is_new_user_session, session_id, NULL)) AS new_user_sessions_total
  FROM engaged_with_lifecycle
),
engaged_user_flags AS (
  SELECT
    user_pseudo_id,
    MIN(session_date) AS first_engaged_in_window,
    ANY_VALUE(first_seen_date) AS first_seen_date
  FROM engaged_with_lifecycle
  GROUP BY user_pseudo_id
),
engaged_user_breakdown AS (
  SELECT
    COUNT(DISTINCT user_pseudo_id) AS engaged_users_total,
    COUNT(DISTINCT IF(first_engaged_in_window <= DATE_ADD(first_seen_date, INTERVAL 15 DAY),
                      user_pseudo_id, NULL)) AS engaged_users_new,
    COUNT(DISTINCT IF(first_engaged_in_window  > DATE_ADD(first_seen_date, INTERVAL 15 DAY),
                      user_pseudo_id, NULL)) AS engaged_users_recurring
  FROM engaged_user_flags
),
scorecards AS (
  SELECT
    -- Session totals
    (SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window)        AS all_sessions_total,
    (SELECT engaged_sessions_total            FROM engaged_totals)          AS engaged_sessions_total,
    SAFE_DIVIDE(
      (SELECT engaged_sessions_total FROM engaged_totals),
      NULLIF((SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window),0)
    ) AS engaged_sessions_pct_of_all,

    -- User totals (based on engaged)
    (SELECT engaged_users_total    FROM engaged_user_breakdown)            AS engaged_users_total,
    (SELECT engaged_users_new      FROM engaged_user_breakdown)            AS engaged_users_new,
    (SELECT engaged_users_recurring FROM engaged_user_breakdown)           AS engaged_users_recurring,
    SAFE_DIVIDE(
      (SELECT engaged_users_new FROM engaged_user_breakdown),
      NULLIF((SELECT engaged_users_total FROM engaged_user_breakdown),0)
    ) AS engaged_users_new_pct,
    SAFE_DIVIDE(
      (SELECT engaged_users_recurring FROM engaged_user_breakdown),
      NULLIF((SELECT engaged_users_total FROM engaged_user_breakdown),0)
    ) AS engaged_users_recurring_pct,

    -- Flagged engaged session totals and their % of ENGAGED and % of ALL
    (SELECT loyalty_sessions_total   FROM engaged_totals)                  AS loyalty_sessions_total,
    (SELECT identified_sessions_total FROM engaged_totals)                 AS identified_sessions_total,
    (SELECT logged_in_sessions_total FROM engaged_totals)                  AS logged_in_sessions_total,
    (SELECT new_user_sessions_total  FROM engaged_totals)                  AS new_user_sessions_total,

    SAFE_DIVIDE((SELECT loyalty_sessions_total   FROM engaged_totals),
                NULLIF((SELECT engaged_sessions_total FROM engaged_totals),0)) AS loyalty_sessions_pct_of_engaged,
    SAFE_DIVIDE((SELECT identified_sessions_total FROM engaged_totals),
                NULLIF((SELECT engaged_sessions_total FROM engaged_totals),0)) AS identified_sessions_pct_of_engaged,
    SAFE_DIVIDE((SELECT logged_in_sessions_total FROM engaged_totals),
                NULLIF((SELECT engaged_sessions_total FROM engaged_totals),0)) AS logged_in_sessions_pct_of_engaged,
    SAFE_DIVIDE((SELECT new_user_sessions_total  FROM engaged_totals),
                NULLIF((SELECT engaged_sessions_total FROM engaged_totals),0)) AS new_user_sessions_pct_of_engaged,

    SAFE_DIVIDE((SELECT loyalty_sessions_total   FROM engaged_totals),
                NULLIF((SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window),0)) AS loyalty_sessions_pct_of_all,
    SAFE_DIVIDE((SELECT identified_sessions_total FROM engaged_totals),
                NULLIF((SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window),0)) AS identified_sessions_pct_of_all,
    SAFE_DIVIDE((SELECT logged_in_sessions_total FROM engaged_totals),
                NULLIF((SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window),0)) AS logged_in_sessions_pct_of_all,
    SAFE_DIVIDE((SELECT new_user_sessions_total  FROM engaged_totals),
                NULLIF((SELECT COUNT(DISTINCT session_id) FROM all_sessions_in_window),0)) AS new_user_sessions_pct_of_all
)

-- =====================================================================
-- 9) UNIFIED OUTPUT (Single_Series + Comparisons + Delta_avg + Score) — single table
--     Common schema with discriminator column `section`.
--     Non-applicable fields are NULL by design.
-- =====================================================================
SELECT
  'Single_Series' AS section,            -- Single-series 25-bin histograms
  NULL AS comparison_dimension,          -- not used in Single_Series
  NULL AS comparison_group,              -- not used in Single_Series
  series,
  session_bucket,
  bucket_label,
  users,
  users_pct_users,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_a,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_b,
  CAST(NULL AS FLOAT64) AS delta_group_a_minus_b,
  CAST(NULL AS INT64)   AS users_in_group_a,
  CAST(NULL AS INT64)   AS users_in_group_b,
  CAST(NULL AS STRING)  AS group_a_label,
  CAST(NULL AS STRING)  AS group_b_label,
  CAST(NULL AS INT64)   AS all_sessions_total,
  CAST(NULL AS INT64)   AS engaged_sessions_total,
  CAST(NULL AS FLOAT64) AS engaged_sessions_pct_of_all,
  CAST(NULL AS INT64)   AS engaged_users_total,
  CAST(NULL AS INT64)   AS engaged_users_new,
  CAST(NULL AS INT64)   AS engaged_users_recurring,
  CAST(NULL AS FLOAT64) AS engaged_users_new_pct,
  CAST(NULL AS FLOAT64) AS engaged_users_recurring_pct,
  CAST(NULL AS INT64)   AS loyalty_sessions_total,
  CAST(NULL AS INT64)   AS identified_sessions_total,
  CAST(NULL AS INT64)   AS logged_in_sessions_total,
  CAST(NULL AS INT64)   AS new_user_sessions_total,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_all
FROM final_bins

UNION ALL
SELECT
  'Comparisons' AS section,              -- Comparison 25-bin histograms
  comparison_dimension,
  comparison_group,
  NULL AS series,
  session_bucket,
  bucket_label,
  users,
  users_pct_users,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_a,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_b,
  CAST(NULL AS FLOAT64) AS delta_group_a_minus_b,
  CAST(NULL AS INT64)   AS users_in_group_a,
  CAST(NULL AS INT64)   AS users_in_group_b,
  CAST(NULL AS STRING)  AS group_a_label,
  CAST(NULL AS STRING)  AS group_b_label,
  CAST(NULL AS INT64)   AS all_sessions_total,
  CAST(NULL AS INT64)   AS engaged_sessions_total,
  CAST(NULL AS FLOAT64) AS engaged_sessions_pct_of_all,
  CAST(NULL AS INT64)   AS engaged_users_total,
  CAST(NULL AS INT64)   AS engaged_users_new,
  CAST(NULL AS INT64)   AS engaged_users_recurring,
  CAST(NULL AS FLOAT64) AS engaged_users_new_pct,
  CAST(NULL AS FLOAT64) AS engaged_users_recurring_pct,
  CAST(NULL AS INT64)   AS loyalty_sessions_total,
  CAST(NULL AS INT64)   AS identified_sessions_total,
  CAST(NULL AS INT64)   AS logged_in_sessions_total,
  CAST(NULL AS INT64)   AS new_user_sessions_total,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_all
FROM final_comparison_bins

UNION ALL
SELECT
  'Delta_avg' AS section,                -- Deltas (GroupA − GroupB)
  comparison_dimension,
  NULL AS comparison_group,
  NULL AS series,
  NULL AS session_bucket,
  NULL AS bucket_label,
  CAST(NULL AS INT64)   AS users,
  CAST(NULL AS FLOAT64) AS users_pct_users,
  avg_sessions_group_a,
  avg_sessions_group_b,
  delta_group_a_minus_b,
  users_in_group_a,
  users_in_group_b,
  group_a AS group_a_label,
  group_b AS group_b_label,
  CAST(NULL AS INT64)   AS all_sessions_total,
  CAST(NULL AS INT64)   AS engaged_sessions_total,
  CAST(NULL AS FLOAT64) AS engaged_sessions_pct_of_all,
  CAST(NULL AS INT64)   AS engaged_users_total,
  CAST(NULL AS INT64)   AS engaged_users_new,
  CAST(NULL AS INT64)   AS engaged_users_recurring,
  CAST(NULL AS FLOAT64) AS engaged_users_new_pct,
  CAST(NULL AS FLOAT64) AS engaged_users_recurring_pct,
  CAST(NULL AS INT64)   AS loyalty_sessions_total,
  CAST(NULL AS INT64)   AS identified_sessions_total,
  CAST(NULL AS INT64)   AS logged_in_sessions_total,
  CAST(NULL AS INT64)   AS new_user_sessions_total,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_engaged,
  CAST(NULL AS FLOAT64) AS loyalty_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS identified_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS logged_in_sessions_pct_of_all,
  CAST(NULL AS FLOAT64) AS new_user_sessions_pct_of_all
FROM comparison_deltas

UNION ALL
SELECT
  'Score' AS section,                    -- Scorecards (single row)
  NULL AS comparison_dimension,
  NULL AS comparison_group,
  NULL AS series,
  NULL AS session_bucket,
  NULL AS bucket_label,
  CAST(NULL AS INT64)   AS users,
  CAST(NULL AS FLOAT64) AS users_pct_users,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_a,
  CAST(NULL AS FLOAT64) AS avg_sessions_group_b,
  CAST(NULL AS FLOAT64) AS delta_group_a_minus_b,
  CAST(NULL AS INT64)   AS users_in_group_a,
  CAST(NULL AS INT64)   AS users_in_group_b,
  CAST(NULL AS STRING)  AS group_a_label,
  CAST(NULL AS STRING)  AS group_b_label,
  all_sessions_total,
  engaged_sessions_total,
  engaged_sessions_pct_of_all,
  engaged_users_total,
  engaged_users_new,
  engaged_users_recurring,
  engaged_users_new_pct,
  engaged_users_recurring_pct,
  loyalty_sessions_total,
  identified_sessions_total,
  logged_in_sessions_total,
  new_user_sessions_total,
  loyalty_sessions_pct_of_engaged,
  identified_sessions_pct_of_engaged,
  logged_in_sessions_pct_of_engaged,
  new_user_sessions_pct_of_engaged,
  loyalty_sessions_pct_of_all,
  identified_sessions_pct_of_all,
  logged_in_sessions_pct_of_all,
  new_user_sessions_pct_of_all
FROM scorecards
;
