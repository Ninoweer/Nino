-- Parameters:
--  - lookback_days : how far back in calendar days to pull sessions.
--  - gap_days      : maximum gap (in days) allowed between sessions before a new journey starts.
DECLARE lookback_days INT64 DEFAULT 5 ;
DECLARE gap_days      INT64 DEFAULT 15 ;

-- BASE: one row per engaged session with canonical touchpoint fields and timestamps.
WITH base AS (
  SELECT
    user_pseudo_id ,
    CAST( session_id AS STRING ) AS ga_session_id ,                  -- GA session identifier (cast to string for safety)
    membership_id ,                                                  -- if present, user has a membership at this session
    session_date ,                                                   -- session-level date
    FORMAT_DATE( '%Y%m%d' , session_date ) AS event_date_yyyymmdd ,  -- formatted date (e.g., 20250131) for downstream ordering
    TIMESTAMP_MICROS( last_event_timestamp ) AS event_ts ,           -- last event ts within the session for ordering

    -- final_* are the session-level resolved last non-null values
    NULLIF( user_session_final_medium   , '' ) AS final_medium ,
    NULLIF( user_session_final_source   , '' ) AS final_source ,
    NULLIF( user_session_final_campaign , '' ) AS final_campaign ,
    NULLIF( session_channelgroup        , '' ) AS channelgroup ,

    engaged_session                                                       -- GA engaged session flag
  FROM `action-dwh.dataform_intermediate.int_ga_app_sessions_channelgroup`
  WHERE session_date BETWEEN DATE_SUB( CURRENT_DATE() , INTERVAL lookback_days DAY ) AND CURRENT_DATE()
    AND engaged_session = 1                                              -- keep only engaged sessions to reduce noise
) ,

-- ORDERED: add "previous row" values for journey logic (per user in chronological order).
ordered AS (
  SELECT
    b.* ,
    IF( membership_id IS NOT NULL , 1 , 0 ) AS member_flag ,                               -- current session membership flag
    LAG( user_pseudo_id ) OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_user , -- null means first row for user
    LAG( session_date )  OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_date ,  -- previous session date
    LAG( IF( membership_id IS NOT NULL , 1 , 0 ) )
      OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) AS prev_member_flag                 -- previous membership flag
  FROM base b
) ,

-- JOURNEY_MARKS: determine conversion rows and where a new journey should start.
journey_marks AS (
  SELECT
    * ,

    -- Conversion definition:
    --  a conversion occurs when membership flips from 0 -> 1 within a session compared to the previous one.
    IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) AS conversion_occured ,

    -- New-journey flag:
    --  start a journey if first row for user, or gap >= gap_days, or immediately after a conversion row.
    CASE
      WHEN prev_user IS NULL THEN 1
      WHEN DATE_DIFF( session_date , prev_date , DAY ) >= gap_days THEN 1
      WHEN IFNULL( LAG( IF( member_flag = 1 AND IFNULL( prev_member_flag , 0 ) = 0 , 1 , 0 ) )
             OVER ( PARTITION BY user_pseudo_id ORDER BY session_date , event_ts ) , 0 ) = 1 THEN 1
      ELSE 0
    END AS new_journey_flag
  FROM ordered
) ,

-- JOURNEY_IDS: assign a running journey index per user (1, 2, 3, …) using cumulative sum of start flags.
journey_ids AS (
  SELECT
    * ,
    SUM( new_journey_flag ) OVER (
      PARTITION BY user_pseudo_id ORDER BY session_date , event_ts
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS journey_idx
  FROM journey_marks
) ,

-- TOUCHPOINTS: explode each session into four rows (one per dimension) so we can build a path per dimension.
-- Notes:
--  - Touchpoint fallbacks ensure we never have gaps: e.g., medium falls back to channel group, then 'Direct'.
--  - The conversion flag is carried over to the touchpoint rows for later aggregation.
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

-- PATHS: create an ordered 'A > B > C' journey per (user, journey_idx, dimension), and mark if any step converted.
paths AS (
  SELECT
    dimension , user_pseudo_id , journey_idx ,
    STRING_AGG( touchpoint , ' > ' ORDER BY session_date , event_ts ) AS journey ,
    MAX( conversion_occured ) > 0 AS is_conversion
  FROM touchpoints
  GROUP BY dimension , user_pseudo_id , journey_idx
)

-- FINAL: one row per (dimension, journey) with counts split into converters vs non-converters.
SELECT
  dimension , journey ,
  SUM( CASE WHEN is_conversion THEN 1 ELSE 0 END ) AS conversion ,
  SUM( CASE WHEN is_conversion THEN 0 ELSE 1 END ) AS na
FROM paths
GROUP BY dimension , journey
ORDER BY dimension , conversion DESC , na DESC ;
