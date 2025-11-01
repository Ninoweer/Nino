-- Customer journey (TWO 1-hour slices on TWO consecutive days; bytes-estimate friendly)
-- Edit ONLY the literals in `params` below.

WITH
  params AS (
    SELECT
      DATE '2025-01-22'                                  AS day1,         -- <-- EDIT: first day (UTC)
      TIME '14:00:00'                                    AS hour_utc,     -- <-- EDIT: hour start (UTC)
      1                                                  AS duration_hours, -- 1 hour
      7                                                  AS lookback_days,  -- prior days to scan prev sessions
      0.985                                              AS coverage_target, -- keep ~98.5% of volume
      0.985                                              AS step_pct          -- truncate at 98.5th pctile of steps
  ),

  -- Build concrete window timestamps
  win AS (
    SELECT
      p.day1                                           AS d1,
      DATE_ADD(p.day1, INTERVAL 1 DAY)                 AS d2,
      TIMESTAMP(DATETIME(p.day1, p.hour_utc))          AS d1_start,
      TIMESTAMP_ADD(TIMESTAMP(DATETIME(p.day1, p.hour_utc)),
                    INTERVAL p.duration_hours HOUR)     AS d1_end,
      TIMESTAMP(DATETIME(DATE_ADD(p.day1, INTERVAL 1 DAY), p.hour_utc)) AS d2_start,
      TIMESTAMP_ADD(TIMESTAMP(DATETIME(DATE_ADD(p.day1, INTERVAL 1 DAY), p.hour_utc)),
                    INTERVAL p.duration_hours HOUR)     AS d2_end,
      DATE_SUB(p.day1, INTERVAL p.lookback_days DAY)    AS lb_start_day,
      p.coverage_target                                 AS coverage_target,
      p.step_pct                                        AS step_pct
    FROM params p
  ),

  /* ================= Base events for two exact 1-hour windows ================= */
  base AS (
    SELECT
      t.event_date,
      t.time.event_timestamp_utc                                              AS ts,
      COALESCE(NULLIF(t.user_id,''), t.user_pseudo_id)                        AS user_key,
      CAST(t.ga_session_id AS STRING)                                         AS session_id,
      LOWER(COALESCE(NULLIF(TRIM(t.firebase_screen), ''),
                     NULLIF(TRIM(t.firebase_screen_class),''),
                     NULLIF(TRIM(t.screen_name),''),
                     t.event_name))                                           AS state_raw,
      SAFE_CAST(t.engagement_time_msec AS INT64)                              AS et_ms,
      SAFE_CAST(t.engaged_session_event AS INT64)                             AS engaged_event,
      t.time.user_first_touch_timestamp_utc                                   AS user_first_touch_utc,
      -- Segment: guest vs logged-in
      CASE
        WHEN NULLIF(t.user_id,'') IS NOT NULL OR LOWER(IFNULL(t.logged_in_up,'')) IN ('yes','true','1')
          THEN 'logged_in' ELSE 'guest'
      END                                                                      AS user_type
      -- OPTIONAL: Loyalty segmentation (requires a reliable flag; uncomment and adjust when available)
      -- , CASE WHEN t.loyalty_card_used = TRUE THEN 'loyalty' ELSE 'no_loyalty' END AS loyalty_flag
    FROM `action-dwh.mart_ga4.ga_app_events` t
    CROSS JOIN win w
    WHERE t.is_final = TRUE
      AND t.ga_session_id IS NOT NULL
      AND t.event_date IN (w.d1, w.d2)                                 -- strong partition prune to 2 days
      AND (
            (t.time.event_timestamp_utc >= w.d1_start AND t.time.event_timestamp_utc < w.d1_end)
         OR (t.time.event_timestamp_utc >= w.d2_start AND t.time.event_timestamp_utc < w.d2_end)
          )
  ),

  /* ================= Optional lookback to get previous session end for reactivation ================= */
  lookback AS (
    SELECT
      t.user_pseudo_id AS user_key_fallback,
      COALESCE(NULLIF(t.user_id,''), t.user_pseudo_id) AS user_key,
      CAST(t.ga_session_id AS STRING)                  AS session_id,
      MAX(t.time.event_timestamp_utc)                  AS sess_end
    FROM `action-dwh.mart_ga4.ga_app_events` t
    CROSS JOIN win w
    WHERE t.is_final = TRUE
      AND t.ga_session_id IS NOT NULL
      AND t.event_date BETWEEN w.lb_start_day AND w.d1   -- lookback days through day1
      AND t.time.event_timestamp_utc < w.d1_start        -- strictly before our first window
    GROUP BY 1,2,3
  ),
  prev_session_end AS (
    SELECT
      user_key,
      MAX(sess_end) AS last_sess_end_before_window
    FROM lookback
    GROUP BY 1
  ),

  /* ================= Normalize states (no manual taxonomy) ================= */
  norm AS (
    SELECT
      b.*,
      TRIM(LOWER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(b.state_raw, r'https?://\S+', ''),      -- drop urls
            r'\b(\d+|[a-f0-9]{6,})\b', ''),                        -- drop ids/numbers/hex tokens
          r'[^a-z]+', ' ')                                         -- keep letters; collapse punct/space
      )) AS state_clean
    FROM base b
  ),

  /* ================= Data-driven vocabulary (~98.5% of volume) ================= */
  state_freq AS (
    SELECT SUBSTR(state_clean, 1, 80) AS state, COUNT(*) AS n
    FROM norm
    WHERE state_clean IS NOT NULL AND state_clean <> ''
    GROUP BY 1
  ),
  ranked AS (
    SELECT
      state,
      n,
      SUM(n) OVER (ORDER BY n DESC) AS cum_n,
      SUM(n) OVER ()                AS total_n
    FROM state_freq
  ),
  vocab AS (
    SELECT r.state
    FROM ranked r, win w
    WHERE SAFE_DIVIDE(r.cum_n, r.total_n) <= w.coverage_target
  ),

  /* ================= Canonical events (tail -> 'other') + user_status + reactivation ================= */
  ev AS (
    SELECT
      n.event_date,
      n.ts,
      n.user_key,
      n.session_id,
      n.user_type,
      /* Optional: loyalty flag passthrough (uncomment if added in base)
      n.loyalty_flag, */
      COALESCE(v.state, 'other') AS state,
      IFNULL(n.et_ms,0)          AS et_ms,
      IFNULL(n.engaged_event,0)  AS engaged_event,
      -- New vs Existing relative to first window start
      CASE
        WHEN n.user_first_touch_utc IS NULL THEN 'unknown'
        WHEN n.user_first_touch_utc >= (SELECT d1_start FROM win) THEN 'new'
        ELSE 'existing'
      END AS user_status,
      -- Reactivation flag: previous session end exists and gap > 24h
      CASE
        WHEN p.last_sess_end_before_window IS NULL THEN FALSE
        WHEN TIMESTAMP_DIFF((SELECT d1_start FROM win), p.last_sess_end_before_window, HOUR) > 24 THEN TRUE
        ELSE FALSE
      END AS reactivated_24h
    FROM norm n
    LEFT JOIN vocab v
      ON SUBSTR(n.state_clean,1,80) = v.state
    LEFT JOIN prev_session_end p
      ON n.user_key = p.user_key
    WHERE n.state_clean IS NOT NULL AND n.state_clean <> ''
  ),

  /* ================= Order within session + compress repeats ================= */
  seq AS (
    SELECT
      e.*,
      ROW_NUMBER() OVER (PARTITION BY user_key, session_id ORDER BY ts) AS rn,
      LAG(state)   OVER (PARTITION BY user_key, session_id ORDER BY ts) AS prev_state
    FROM ev e
  ),
  seq_comp AS (
    SELECT * EXCEPT(prev_state)
    FROM seq
    WHERE prev_state IS NULL OR state <> prev_state
  ),

  /* ================= Transitions (state-level) ================= */
 transitions_state AS (
  SELECT
    user_type,
    user_status,
    reactivated_24h,
    -- loyalty_flag,
    from_state,
    to_state,
    COUNT(*) AS transitions,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
      PARTITION BY user_type, user_status, reactivated_24h /*, loyalty_flag */, from_state
    ), 4) AS p_from_to
  FROM (
    SELECT
      user_type, user_status, reactivated_24h /*, loyalty_flag */,
      user_key, session_id, rn, state AS from_state,
      LEAD(state) OVER (PARTITION BY user_key, session_id ORDER BY rn) AS to_state
    FROM seq_comp
  )
  WHERE to_state IS NOT NULL
  GROUP BY
    user_type, user_status, reactivated_24h, /* loyalty_flag, */
    from_state, to_state
),

  /* ================= “Family” = first token of the cleaned state ================= */
transitions_family AS (
  SELECT
    user_type,
    user_status,
    reactivated_24h,
    -- loyalty_flag,
    from_family,
    to_family,
    COUNT(*) AS transitions,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER (
      PARTITION BY user_type, user_status, reactivated_24h /*, loyalty_flag */, from_family
    ), 4) AS p_from_to
  FROM (
    SELECT
      user_type, user_status, reactivated_24h /*, loyalty_flag */,
      user_key, session_id, rn,
      SPLIT(state, ' ')[OFFSET(0)] AS from_family,
      SPLIT(LEAD(state) OVER (PARTITION BY user_key, session_id ORDER BY rn), ' ')[OFFSET(0)] AS to_family
    FROM seq_comp
  )
  WHERE to_family IS NOT NULL
  GROUP BY
    user_type, user_status, reactivated_24h, /* loyalty_flag, */
    from_family, to_family
),

  /* ================= Entry / exit states ================= */
entry_states AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    state AS entry_state,
    COUNT(DISTINCT CONCAT(user_key,'|',session_id)) AS sessions
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_key, session_id ORDER BY rn) AS r_first
    FROM seq_comp
  )
  WHERE r_first = 1
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    entry_state
),

 exit_states AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    state AS exit_state,
    COUNT(DISTINCT CONCAT(user_key,'|',session_id)) AS sessions
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_key, session_id ORDER BY rn DESC) AS r_last
    FROM seq_comp
  )
  WHERE r_last = 1
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    exit_state
),

  /* ================= 98.5th-percentile step cut ================= */
  step_cut AS (
    WITH q AS (
      SELECT APPROX_QUANTILES(steps, 101) AS qlist
      FROM (
        SELECT COUNT(*) AS steps
        FROM seq_comp
        GROUP BY user_key, session_id
      )
    ), p AS (SELECT step_pct FROM win)
    SELECT CAST( qlist[OFFSET(CAST(ROUND(100 * p.step_pct) AS INT64))] AS INT64 ) AS max_steps
    FROM q, p
  ),

  /* ================= Top paths (slice arrays up to max_steps) ================= */
per_session_paths AS (
  SELECT
    s.user_type, s.user_status, s.reactivated_24h /*, s.loyalty_flag */,
    s.user_key, s.session_id, sc.max_steps,
    ARRAY_AGG(s.state ORDER BY s.rn) AS states_full
  FROM seq_comp s
  CROSS JOIN step_cut sc
  GROUP BY
    s.user_type, s.user_status, s.reactivated_24h /*, s.loyalty_flag */,
    s.user_key, s.session_id, sc.max_steps
),
top_paths AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT st
        FROM UNNEST(states_full) AS st WITH OFFSET pos
        WHERE pos < max_steps
      ),
      ' > '
    ) AS path,
    COUNT(*) AS sessions
  FROM per_session_paths
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    path
),

  /* ================= Dwell proxy per state ================= */
state_time AS (
  WITH per_session_state AS (
    SELECT user_type, user_status, reactivated_24h /*, loyalty_flag */,
           user_key, session_id, state, SUM(et_ms) AS ms
    FROM ev
    GROUP BY
      user_type, user_status, reactivated_24h /*, loyalty_flag */,
      user_key, session_id, state
  )
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    state,
    COUNT(*) AS sessions_with_state,
    ROUND(AVG(ms)/1000, 2) AS avg_seconds_per_session,
    ROUND(SUM(ms)/1000, 2) AS total_seconds
  FROM per_session_state
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    state
),

  /* ================= Entry -> Dominant-state migrations ================= */
per_session_dominant AS (
  SELECT user_type, user_status, reactivated_24h /*, loyalty_flag */,
         user_key, session_id,
         ARRAY_AGG(STRUCT(state, ms_sum)
                   ORDER BY ms_sum DESC, state ASC
                   LIMIT 1)[OFFSET(0)] AS top_state
  FROM (
    SELECT user_type, user_status, reactivated_24h /*, loyalty_flag */,
           user_key, session_id, state, SUM(et_ms) AS ms_sum
    FROM ev
    GROUP BY
      user_type, user_status, reactivated_24h /*, loyalty_flag */,
      user_key, session_id, state
  )
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    user_key, session_id
),

  per_session_entry AS (
    SELECT user_type, user_status, reactivated_24h /*, loyalty_flag */,
           user_key, session_id, state AS entry_state
    FROM (
      SELECT user_type, user_status, reactivated_24h /*, loyalty_flag */,
             user_key, session_id, state,
             ROW_NUMBER() OVER (PARTITION BY user_key, session_id ORDER BY rn) AS r_first
      FROM seq_comp
    )
    WHERE r_first = 1
  ),
migrations_state AS (
  SELECT
    e.user_type, e.user_status, e.reactivated_24h /*, e.loyalty_flag */,
    e.entry_state,
    d.top_state.state AS dominant_state,
    COUNT(*) AS sessions
  FROM per_session_entry e
  JOIN per_session_dominant d
    ON e.user_type = d.user_type
   AND e.user_status = d.user_status
   AND e.reactivated_24h = d.reactivated_24h
   /* AND e.loyalty_flag = d.loyalty_flag */
   AND e.user_key = d.user_key
   AND e.session_id = d.session_id
  GROUP BY
    e.user_type, e.user_status, e.reactivated_24h /*, e.loyalty_flag */,
    e.entry_state, dominant_state
),

migrations_family AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    SPLIT(entry_state,   ' ')[OFFSET(0)] AS entry_family,
    SPLIT(dominant_state,' ')[OFFSET(0)] AS dominant_family,
    COUNT(*) AS sessions
  FROM migrations_state
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    entry_family, dominant_family
),

  /* ================= Session-level stats ================= */
session_stats AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    user_key, session_id,
    COUNT(*) AS steps,
    COUNT(DISTINCT state) AS unique_states,
    (SUM(engaged_event) > 0 OR SUM(et_ms) >= 10000) AS engaged_session,
    SUM(et_ms) / 1000.0 AS dwell_seconds
  FROM ev
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    user_key, session_id
),
session_summary AS (
  SELECT
    user_type, user_status, reactivated_24h /*, loyalty_flag */,
    COUNT(*) AS sessions,
    APPROX_QUANTILES(steps, 101)[OFFSET(50)] AS p50_steps,
    APPROX_QUANTILES(steps, 101)[OFFSET(98)] AS p98_steps,
    ROUND(SUM(CASE WHEN steps=1 THEN 1 ELSE 0 END)/COUNT(*), 4) AS bounce_rate,
    ROUND(SUM(CASE WHEN steps > unique_states THEN 1 ELSE 0 END)/COUNT(*), 4) AS revisit_rate,
    ROUND(SUM(CASE WHEN engaged_session THEN 1 ELSE 0 END)/COUNT(*), 4) AS engaged_share,
    ROUND(APPROX_QUANTILES(dwell_seconds, 101)[OFFSET(50)], 2) AS p50_dwell_seconds
  FROM session_stats
  GROUP BY
    user_type, user_status, reactivated_24h /*, loyalty_flag */
)

SELECT
  ARRAY(SELECT AS STRUCT * FROM transitions_state   ORDER BY user_type, user_status, reactivated_24h, from_state, transitions DESC) AS transitions_state,
  ARRAY(SELECT AS STRUCT * FROM transitions_family  ORDER BY user_type, user_status, reactivated_24h, from_family, transitions DESC) AS transitions_family,
  ARRAY(SELECT AS STRUCT * FROM entry_states        ORDER BY user_type, user_status, reactivated_24h, sessions DESC)                AS entry_states,
  ARRAY(SELECT AS STRUCT * FROM exit_states         ORDER BY user_type, user_status, reactivated_24h, sessions DESC)               AS exit_states,
  ARRAY(SELECT AS STRUCT * FROM top_paths           ORDER BY user_type, user_status, reactivated_24h, sessions DESC)               AS top_paths,
  ARRAY(SELECT AS STRUCT * FROM state_time          ORDER BY user_type, user_status, reactivated_24h, sessions_with_state DESC)    AS state_time,
  ARRAY(SELECT AS STRUCT * FROM migrations_state    ORDER BY user_type, user_status, reactivated_24h, sessions DESC)               AS entry_to_dominant_state,
  ARRAY(SELECT AS STRUCT * FROM migrations_family   ORDER BY user_type, user_status, reactivated_24h, sessions DESC)               AS entry_to_dominant_family,
  ARRAY(SELECT AS STRUCT * FROM session_summary     ORDER BY user_type, user_status, reactivated_24h)                              AS session_summary
;


