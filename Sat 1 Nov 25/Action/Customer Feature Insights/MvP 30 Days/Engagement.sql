-- =====================================================================
-- Unified output: 25-bin per-user engaged session distributions
-- for core series AND comparisons (single source for Looker Studio).
-- =====================================================================

-- PARAMETERS (edit as needed)
DECLARE report_start DATE DEFAULT DATE('2025-01-01');
DECLARE report_end   DATE DEFAULT DATE('2025-10-31');

-- 1) First seen date per user (global across entire base table)
WITH user_first_seen AS (
  SELECT user_pseudo_id, MIN(date) AS first_seen_date
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount`
  WHERE user_pseudo_id IS NOT NULL
  GROUP BY user_pseudo_id
),

-- 2) Engaged sessions in the report window, normalized flags
engaged_sessions AS (
  SELECT DISTINCT
    s.user_pseudo_id,
    SAFE_CAST(s.session_id AS INT64) AS session_id,
    s.date AS session_date,
    IFNULL(s.login, FALSE)              AS is_logged_in,
    IFNULL(s.identified_session, FALSE) AS is_identified,
    IFNULL(s.registration, FALSE)       AS is_registered,
    (IFNULL(s.loyalty_card_show, 0) > 0) AS has_loyalty
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount` AS s
  WHERE
    s.date BETWEEN report_start AND report_end
    AND COALESCE(s.engaged_session, s.engaged_sessions) = TRUE
    AND s.user_pseudo_id IS NOT NULL
    AND SAFE_CAST(s.session_id AS INT64) IS NOT NULL
),

-- 3) Attach lifecycle; compute new-user session flag (<= first_seen_date + 15 days)
engaged_with_lifecycle AS (
  SELECT
    e.user_pseudo_id,
    e.session_id,
    e.session_date,
    e.is_logged_in,
    e.is_identified,
    e.is_registered,
    e.has_loyalty,
    ufs.first_seen_date,
    CASE WHEN e.session_date <= DATE_ADD(ufs.first_seen_date, INTERVAL 15 DAY) THEN TRUE ELSE FALSE END AS is_new_user_session
  FROM engaged_sessions AS e
  JOIN user_first_seen AS ufs
    ON ufs.user_pseudo_id = e.user_pseudo_id
),

-- 4) Per-user totals (distinct engaged sessions of each flavor)
per_user AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT session_id)                                                     AS engaged_sessions,
    COUNT(DISTINCT IF(has_loyalty,         session_id, NULL))                      AS loyalty_sessions,
    COUNT(DISTINCT IF(is_logged_in,        session_id, NULL))                      AS logged_in_sessions,
    COUNT(DISTINCT IF(is_identified,       session_id, NULL))                      AS identified_sessions,
    COUNT(DISTINCT IF(is_new_user_session, session_id, NULL))                      AS new_user_sessions,
    COUNT(DISTINCT IF(is_registered,       session_id, NULL))                      AS registered_sessions
  FROM engaged_with_lifecycle
  GROUP BY user_pseudo_id
),

-- 5) Long format for the 5 series (these will become the 'series' kind rows)
long_user_series AS (
  SELECT user_pseudo_id, 'Engaged Sessions'    AS series, engaged_sessions   AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Loyalty Sessions'    AS series, loyalty_sessions   AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Logged-in Sessions'  AS series, logged_in_sessions  AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Identified Sessions' AS series, identified_sessions AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'New-user Sessions'   AS series, new_user_sessions   AS session_count FROM per_user
),

-- 6) Keep users with >0 sessions per series
nonzero_series AS (
  SELECT series, user_pseudo_id, session_count
  FROM long_user_series
  WHERE session_count > 0
),

-- 7) Bucketize to 25 bins (25 means 25+)
bucketed_series AS (
  SELECT series, user_pseudo_id, LEAST(session_count, 25) AS session_bucket
  FROM nonzero_series
),

-- 8) Frequency counts for series
freq_series AS (
  SELECT series AS name, session_bucket, COUNT(DISTINCT user_pseudo_id) AS users
  FROM bucketed_series
  GROUP BY series, session_bucket
),

-- 9) Make final series rows with pct
final_series AS (
  SELECT
    'series' AS kind,
    name,
    name AS group_label,  -- for series we use the series name as group_label (BI-friendly)
    session_bucket,
    CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
    users,
    SAFE_DIVIDE(users, NULLIF(SUM(users) OVER (PARTITION BY name), 0)) AS users_pct_users
  FROM freq_series
),

-- --------------------------
-- Comparison preparation
-- --------------------------
-- 10) Classify users into groups for comparisons (user-level classification)
per_user_classified AS (
  SELECT
    user_pseudo_id,
    engaged_sessions,
    CASE WHEN logged_in_sessions >= 1 THEN 'Logged in' ELSE 'Guest' END                         AS logged_vs_guest,
    CASE WHEN new_user_sessions >= 1 THEN 'New User' ELSE 'Recurring' END                         AS new_vs_recurring,
    CASE WHEN loyalty_sessions >= 1 THEN 'Loyalty Card Shown' ELSE 'No Card Shown' END             AS loyalty_vs_nocard,
    CASE WHEN identified_sessions >= 1 THEN 'Identified' ELSE 'Anonymous' END                    AS identified_vs_anonymous
  FROM per_user
),

-- 11) For each comparison: prepare session_bucket per user, then group
--    This 2-step pattern prevents "not grouped or aggregated" errors.

-- 11a) Logged vs Guest
comp_logged_prep AS (
  SELECT user_pseudo_id, logged_vs_guest AS group_label, LEAST(engaged_sessions, 25) AS session_bucket
  FROM per_user_classified
  WHERE engaged_sessions > 0
),
comp_logged AS (
  SELECT 'Logged_vs_Guest' AS comparison, group_label, session_bucket,
         CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
         COUNT(DISTINCT user_pseudo_id) AS users
  FROM comp_logged_prep
  GROUP BY group_label, session_bucket
),

-- 11b) New vs Recurring
comp_new_prep AS (
  SELECT user_pseudo_id, new_vs_recurring AS group_label, LEAST(engaged_sessions, 25) AS session_bucket
  FROM per_user_classified
  WHERE engaged_sessions > 0
),
comp_new AS (
  SELECT 'New_vs_Recurring' AS comparison, group_label, session_bucket,
         CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
         COUNT(DISTINCT user_pseudo_id) AS users
  FROM comp_new_prep
  GROUP BY group_label, session_bucket
),

-- 11c) Loyalty vs NoCard
comp_loyalty_prep AS (
  SELECT user_pseudo_id, loyalty_vs_nocard AS group_label, LEAST(engaged_sessions, 25) AS session_bucket
  FROM per_user_classified
  WHERE engaged_sessions > 0
),
comp_loyalty AS (
  SELECT 'Loyalty_vs_NoCard' AS comparison, group_label, session_bucket,
         CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
         COUNT(DISTINCT user_pseudo_id) AS users
  FROM comp_loyalty_prep
  GROUP BY group_label, session_bucket
),

-- 11d) Identified vs Anonymous
comp_identified_prep AS (
  SELECT user_pseudo_id, identified_vs_anonymous AS group_label, LEAST(engaged_sessions, 25) AS session_bucket
  FROM per_user_classified
  WHERE engaged_sessions > 0
),
comp_identified AS (
  SELECT 'Identified_vs_Anonymous' AS comparison, group_label, session_bucket,
         CASE WHEN session_bucket = 25 THEN '25+' ELSE CAST(session_bucket AS STRING) END AS bucket_label,
         COUNT(DISTINCT user_pseudo_id) AS users
  FROM comp_identified_prep
  GROUP BY group_label, session_bucket
),

-- 12) Finalize comparisons with pct per (comparison, group_label)
comp_logged_final AS (
  SELECT 'comparison' AS kind, comparison AS name, group_label, session_bucket, bucket_label, users,
         SAFE_DIVIDE(users, NULLIF(SUM(users) OVER (PARTITION BY comparison, group_label), 0)) AS users_pct_users
  FROM comp_logged
),
comp_new_final AS (
  SELECT 'comparison' AS kind, comparison AS name, group_label, session_bucket, bucket_label, users,
         SAFE_DIVIDE(users, NULLIF(SUM(users) OVER (PARTITION BY comparison, group_label), 0)) AS users_pct_users
  FROM comp_new
),
comp_loyalty_final AS (
  SELECT 'comparison' AS kind, comparison AS name, group_label, session_bucket, bucket_label, users,
         SAFE_DIVIDE(users, NULLIF(SUM(users) OVER (PARTITION BY comparison, group_label), 0)) AS users_pct_users
  FROM comp_loyalty
),
comp_identified_final AS (
  SELECT 'comparison' AS kind, comparison AS name, group_label, session_bucket, bucket_label, users,
         SAFE_DIVIDE(users, NULLIF(SUM(users) OVER (PARTITION BY comparison, group_label), 0)) AS users_pct_users
  FROM comp_identified
),

-- 13) Union all comparison finals
comparison_final AS (
  SELECT * FROM comp_logged_final
  UNION ALL
  SELECT * FROM comp_new_final
  UNION ALL
  SELECT * FROM comp_loyalty_final
  UNION ALL
  SELECT * FROM comp_identified_final
),

-- 14) UNION series + comparisons into a single source
unified_output AS (
  SELECT kind, name, group_label, session_bucket, bucket_label, users, users_pct_users
  FROM final_series
  UNION ALL
  SELECT kind, name, group_label, session_bucket, bucket_label, users, users_pct_users
  FROM comparison_final
)

-- 15) Final select: single result set for Looker Studio
SELECT
  kind,                 -- 'series' or 'comparison'
  name,                 -- series name (e.g., 'Engaged Sessions') OR comparison id (e.g., 'Logged_vs_Guest')
  group_label,          -- for series: same as name; for comparison: group label (e.g., 'Logged in')
  session_bucket,       -- 1..25 (25 = 25+)
  bucket_label,         -- '1'..'24' or '25+'
  users,                -- distinct users in this bin for this (kind,name,group_label)
  users_pct_users       -- users / total users of that (kind,name,group_label)
FROM unified_output
ORDER BY kind, name, group_label, session_bucket;
