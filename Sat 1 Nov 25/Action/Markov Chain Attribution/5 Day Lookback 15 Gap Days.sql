-- ============================================================================
-- ACTION — ATTRIBUTION DATA EXTRACT (GA4 APP SESSIONS)
-- ============================================================================
-- This file contains two parts:
--   A) The ORIGINAL query (unchanged characters, keep for copy/paste).
--   B) A HEAVILY ANNOTATED version where we add comment lines BEFORE or
--      BETWEEN statements to explain purpose, logic, trade‑offs, and pitfalls.
--      (We NEVER modify your SQL code lines; we only surround them with comments.)
--
-- Style notes respected:
--   • Spaces around commas, brackets, parentheses (e.g., plot( x , y ) style).
--   • OVER( PARTITION BY … ORDER BY … ) spacing.
--
-- Outputs one row per ( dimension , journey ) with:
--   • conversion : number of journeys that converted (membership flip 0 → 1)
--   • na         : number of journeys that did NOT convert
-- Supported dimensions: medium , source , campaign , channelgroup
-- Journey representation: 'A > B > C' (ordered by session_date , event_ts).
--
-- Key design decisions (summarized):
--   1) Conversion proxy = membership flip. Replace later if a final KPI is chosen.
--   2) Journey boundaries = first row per user, gap >= gap_days, or after a conversion.
--   3) Touchpoint fallbacks = avoid null/empty steps so paths remain contiguous.
--   4) Engaged sessions only = reduce noise and short bounces.
--   5) Output grain = aggregated paths for plotting (not per-user PII).
-- ============================================================================

-- ============================================================================
-- A) ORIGINAL (unchanged)
-- ============================================================================

DECLARE lookback_days INT64 DEFAULT 5 ;
DECLARE gap_days      INT64 DEFAULT 15 ;

WITH base AS (
  SELECT
    user_pseudo_id ,
    CAST( session_id AS STRING ) AS ga_session_id ,
    membership_id ,
    session_date ,
    FORMAT_DATE( '%Y%m%d' , session_date ) AS event_date_yyyymmdd ,
    TIMESTAMP_MICROS( last_event_timestamp ) AS event_ts ,
    NULLIF( user_session_final_medium   , '' ) AS final_medium ,
    NULLIF( user_session_final_source   , '' ) AS final_source ,
    NULLIF( user_session_final_campaign , '' ) AS final_campaign ,
    NULLIF( session_channelgroup        , '' ) AS channelgroup ,
    engaged_session
  FROM `action-dwh.dataform_intermediate.int_ga_app_sessions_channelgroup`
  WHERE session_date BETWEEN DATE_SUB( CURRENT_DATE() , INTERVAL lookback_days DAY ) AND CURRENT_DATE()
    AND engaged_session = 1
) ,
ordered AS (
  SELECT
    b.* ,
    IF( membership_id IS NOT NULL , 1 , 0 ) AS member_flag ,
    LAG( user_pseudo_id ) OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_user ,
    LAG( session_date )  OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_date ,
    LAG( IF( membership_id IS NOT NULL , 1 , 0 ) )
      OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_member_flag
  FROM base b
) ,
journey_marks AS (
  SELECT
    * ,
    IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) AS conversion_occured ,
    CASE
      WHEN prev_user IS NULL THEN 1
      WHEN DATE_DIFF( session_date , prev_date , DAY ) >= gap_days THEN 1
      WHEN IFNULL( LAG( IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) )
             OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) , 0 ) = 1 THEN 1
      ELSE 0
    END AS new_journey_flag
  FROM ordered
) ,
journey_ids AS (
  SELECT
    * ,
    SUM( new_journey_flag ) OVER (
      PARTITION BY user_pseudo_id ORDER BY session_date , event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS journey_idx
  FROM journey_marks
) ,
touchpoints AS (
  SELECT
    user_pseudo_id , journey_idx , session_date , event_ts ,
    'medium' AS dimension , COALESCE( final_medium , channelgroup , 'Direct' ) AS touchpoint , conversion_occured
  FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'source' , COALESCE( final_source , 'Direct' ) , conversion_occured FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'campaign' , COALESCE( final_campaign , 'Unspecified' ) , conversion_occured FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'channelgroup' , COALESCE( channelgroup , 'Direct' ) , conversion_occured FROM journey_ids
) ,
paths AS (
  SELECT
    dimension , user_pseudo_id , journey_idx ,
    STRING_AGG( touchpoint , ' > ' ORDER BY session_date , event_ts ) AS journey ,
    MAX( conversion_occured ) > 0 AS is_conversion
  FROM touchpoints
  GROUP BY dimension , user_pseudo_id , journey_idx
)
SELECT
  dimension , journey ,
  SUM( CASE WHEN is_conversion THEN 1 ELSE 0 END ) AS conversion ,
  SUM( CASE WHEN is_conversion THEN 0 ELSE 1 END ) AS na
FROM paths
GROUP BY dimension , journey
ORDER BY dimension , conversion DESC , na DESC ;

-- ============================================================================
-- B) HEAVILY ANNOTATED VERSION (code lines are unchanged; only comments added)
-- ============================================================================
-- Parameter block: choose how far back to read and when to split journeys
-- • lookback_days : analysis window size (e.g., 5 days for a quick sample)
-- • gap_days      : if days between two sessions ≥ gap_days → new journey

DECLARE lookback_days INT64 DEFAULT 5 ;
DECLARE gap_days      INT64 DEFAULT 15 ;


-- BASE: pull engaged sessions and normalize touchpoint fields
-- Rationale: use resolved 'final_*' fields for clean attribution source; keep 'engaged_session' to filter noise.
WITH base AS (
  SELECT
    user_pseudo_id ,
    CAST( session_id AS STRING ) AS ga_session_id ,
    membership_id ,
    session_date ,
    FORMAT_DATE( '%Y%m%d' , session_date ) AS event_date_yyyymmdd ,
    TIMESTAMP_MICROS( last_event_timestamp ) AS event_ts ,
    NULLIF( user_session_final_medium   , '' ) AS final_medium ,
    NULLIF( user_session_final_source   , '' ) AS final_source ,
    NULLIF( user_session_final_campaign , '' ) AS final_campaign ,
    NULLIF( session_channelgroup        , '' ) AS channelgroup ,
    engaged_session
  FROM `action-dwh.dataform_intermediate.int_ga_app_sessions_channelgroup`
-- Lookback window: adjust to match reporting period (e.g., 30 , 60 , 90 days).
  WHERE session_date BETWEEN DATE_SUB( CURRENT_DATE() , INTERVAL lookback_days DAY ) AND CURRENT_DATE()
-- Keep only engaged sessions to avoid noise from ultra-short/bounced sessions.
    AND engaged_session = 1
) ,

-- ORDERED: create 'previous' columns per user to detect conversions and journey boundaries
-- Pitfall: ensure ORDER BY uses both session_date and event_ts to be stable within a day.
ordered AS (
  SELECT
    b.* ,
    IF( membership_id IS NOT NULL , 1 , 0 ) AS member_flag ,
    LAG( user_pseudo_id ) OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_user ,
    LAG( session_date )  OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_date ,
    LAG( IF( membership_id IS NOT NULL , 1 , 0 ) )
      OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_member_flag
  FROM base b
) ,

-- JOURNEY_MARKS: mark conversion rows and whether a new journey should start at this row
-- Conversion definition here = membership flip 0 → 1 (change later if KPI changes).
journey_marks AS (
  SELECT
    * ,
    IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) AS conversion_occured ,
    CASE
      WHEN prev_user IS NULL THEN 1
      WHEN DATE_DIFF( session_date , prev_date , DAY ) >= gap_days THEN 1
-- prev_member_flag: used to detect membership flips (0 → 1) across consecutive sessions.
      WHEN IFNULL( LAG( IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) )
             OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) , 0 ) = 1 THEN 1
      ELSE 0
    END AS new_journey_flag
  FROM ordered
) ,

-- JOURNEY_IDS: cumulative SUM over 'new_journey_flag' to assign a running journey index per user
journey_ids AS (
  SELECT
    * ,
    SUM( new_journey_flag ) OVER (
      PARTITION BY user_pseudo_id ORDER BY session_date , event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS journey_idx
  FROM journey_marks
) ,

-- TOUCHPOINTS: explode to 4 dimensions; add robust fallbacks so paths are contiguous
-- Notes:
--  • medium falls back to channelgroup, then 'Direct'
--  • source falls back to 'Direct'
--  • campaign falls back to 'Unspecified'
--  • channelgroup falls back to 'Direct'
touchpoints AS (
  SELECT
    user_pseudo_id , journey_idx , session_date , event_ts ,
-- Fallback chain chosen to avoid null steps in paths (prevents ' >  > ' gaps).
    'medium' AS dimension , COALESCE( final_medium , channelgroup , 'Direct' ) AS touchpoint , conversion_occured
  FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'source' , COALESCE( final_source , 'Direct' ) , conversion_occured FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'campaign' , COALESCE( final_campaign , 'Unspecified' ) , conversion_occured FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id , journey_idx , session_date , event_ts , 'channelgroup' , COALESCE( channelgroup , 'Direct' ) , conversion_occured FROM journey_ids
) ,

-- PATHS: stitch sessions into ordered strings per ( user , journey_idx , dimension )
-- We then flag if ANY session in that journey converted.
paths AS (
  SELECT
    dimension , user_pseudo_id , journey_idx ,
-- Build ordered path string for plotting 'Top 12 journeys' and Sankey / heatmaps.
    STRING_AGG( touchpoint , ' > ' ORDER BY session_date , event_ts ) AS journey ,
-- A journey is considered converted if ANY session within it triggered the flag.
    MAX( conversion_occured ) > 0 AS is_conversion
  FROM touchpoints
  GROUP BY dimension , user_pseudo_id , journey_idx
)
SELECT
  dimension , journey ,
-- Count how many journeys in this ( dimension , path ) end up converting.
  SUM( CASE WHEN is_conversion THEN 1 ELSE 0 END ) AS conversion ,
-- Complement count: non‑converting journeys with the exact same path.
  SUM( CASE WHEN is_conversion THEN 0 ELSE 1 END ) AS na
FROM paths
GROUP BY dimension , journey
ORDER BY dimension , conversion DESC , na DESC ;
