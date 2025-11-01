-- =====================================================================
-- PER-USER FREQUENCY DISTRIBUTIONS (25 BINS) OF ENGAGED SESSION COUNTS
-- Series emitted (each gets its own 25-bin histogram: 1..25, where 25="25+"):
--   - Engaged Sessions
--   - Loyalty Sessions          (loyalty_card_show > 0)
--   - Logged-in Sessions        (login = TRUE)
--   - Identified Sessions       (identified_session = TRUE)
--   - New-user Sessions         (session_date <= first_seen_date + 15 days)
--
-- Notes:
--   * Only ENGAGED sessions are considered: COALESCE(engaged_session, engaged_sessions)=TRUE
--   * Sessions are deduped per (user_pseudo_id, session_id).
--   * DISTINCT user counting is explicit, even when redundant.
--   * New-user rule uses TRUE first_seen_date (across full base table) to avoid left-censoring.
--
-- -------------------------
-- PARAMETERS
-- -------------------------
DECLARE report_start DATE DEFAULT DATE('2025-01-01');  -- inclusive
DECLARE report_end   DATE DEFAULT DATE('2025-10-31');  -- inclusive

-- =====================================================================
-- 1) Global first_seen_date per user (entire base table, not just window)
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
-- 2) Engaged sessions in report window, normalized flags
-- =====================================================================
engaged_sessions AS (
  SELECT DISTINCT
    s.user_pseudo_id,
    SAFE_CAST(s.session_id AS INT64) AS session_id, -- Safe cast to fall back if session_id is not an integer
    s.date AS session_date,

    IFNULL(s.login, FALSE)                 AS is_logged_in,
    IFNULL(s.identified_session, FALSE)    AS is_identified,
    (IFNULL(s.loyalty_card_show, 0) > 0)   AS has_loyalty
  FROM `action-dwh.sandbox.ga_app_reporting_eventcount` AS s
  WHERE
    s.date BETWEEN report_start AND report_end
    AND COALESCE(s.engaged_session, s.engaged_sessions) = TRUE -- Pick first non-Null value, rows with both null/false are excluded
    AND s.user_pseudo_id IS NOT NULL
    AND SAFE_CAST(s.session_id AS INT64) IS NOT NULL
),

-- =====================================================================
-- 3) Attach lifecycle; compute "new-user session" (<= 15 days from first seen)
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
-- 4) Per-user totals (DISTINCT sessions for each series)
-- =====================================================================
per_user AS (
  SELECT
    user_pseudo_id,

    COUNT(DISTINCT session_id)                                       AS engaged_sessions,
    COUNT(DISTINCT IF(has_loyalty,           session_id, NULL))      AS loyalty_sessions,
    COUNT(DISTINCT IF(is_logged_in,          session_id, NULL))      AS logged_in_sessions,
    COUNT(DISTINCT IF(is_identified,         session_id, NULL))      AS identified_sessions,
    COUNT(DISTINCT IF(is_new_user_session,   session_id, NULL))      AS new_user_sessions
  FROM engaged_with_lifecycle
  GROUP BY user_pseudo_id
),

-- =====================================================================
-- 5) Long format for ALL 25-bin series
-- =====================================================================
long_user_series AS (
  SELECT user_pseudo_id, 'Engaged Sessions'      AS series, engaged_sessions      AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Loyalty Sessions'      AS series, loyalty_sessions      AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Logged-in Sessions'    AS series, logged_in_sessions    AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'Identified Sessions'   AS series, identified_sessions   AS session_count FROM per_user UNION ALL
  SELECT user_pseudo_id, 'New-user Sessions'     AS series, new_user_sessions     AS session_count FROM per_user
),

-- =====================================================================
-- 6) Drop users with zero sessions for the given series
-- =====================================================================
nonzero AS (
  SELECT
    series,
    user_pseudo_id,
    session_count
  FROM long_user_series
  WHERE session_count > 0
),

-- =====================================================================
-- 7) Bucketize to 25 bins (25 = "25+")
-- =====================================================================
bucketed AS (
  SELECT
    series,
    user_pseudo_id,
    LEAST(session_count, 25) AS session_bucket
  FROM nonzero
),

-- =====================================================================
-- 8) Frequency tables with explicit DISTINCT user counting
-- =====================================================================
freq AS (
  SELECT
    series,
    session_bucket,
    COUNT(DISTINCT user_pseudo_id) AS users
  FROM bucketed
  GROUP BY series, session_bucket
),

-- =====================================================================
-- 9) Add % of users per series; add label
-- =====================================================================
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
)

-- =====================================================================
-- 10) OUTPUT
-- =====================================================================
SELECT
  series,
  session_bucket,
  bucket_label,
  users,
  users_pct_users
FROM final_bins
ORDER BY series, session_bucket;
