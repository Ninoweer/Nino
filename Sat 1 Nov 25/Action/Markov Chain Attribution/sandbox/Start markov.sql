-- PARAMS (edit if needed)
DECLARE lookback_days INT64 DEFAULT 5;
DECLARE gap_days INT64 DEFAULT 15;

WITH base AS (
  SELECT
    user_pseudo_id,
    CAST(session_id AS STRING) AS ga_session_id,
    membership_id,
    session_date,
    FORMAT_DATE('%Y%m%d', session_date) AS event_date_yyyymmdd,
    TIMESTAMP_MICROS(last_event_timestamp) AS event_ts,
    -- Canonicalized touchpoints (fallbacks for nulls/empties)
    NULLIF(user_session_final_medium, '')     AS final_medium,
    NULLIF(user_session_final_source, '')     AS final_source,
    NULLIF(user_session_final_campaign, '')   AS final_campaign,
    NULLIF(session_channelgroup, '')          AS channelgroup,
    engaged_session
  FROM `action-dwh.dataform_intermediate.int_ga_app_sessions_channelgroup`
  WHERE session_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY) AND CURRENT_DATE()
    AND engaged_session = 1
),
ordered AS (
  SELECT
    b.*,
    -- membership signal in this session
    IF(membership_id IS NOT NULL, 1, 0) AS member_flag,
    -- previous values for journey logic
    LAG(user_pseudo_id)  OVER (PARTITION BY user_pseudo_id ORDER BY session_date, event_ts) AS prev_user,
    LAG(session_date)     OVER (PARTITION BY user_pseudo_id ORDER BY session_date, event_ts) AS prev_date,
    LAG(IF(membership_id IS NOT NULL, 1, 0))
                         OVER (PARTITION BY user_pseudo_id ORDER BY session_date, event_ts) AS prev_member_flag
  FROM base b
),
journey_marks AS (
  SELECT
    *,
    -- conversion occurs when membership flips from 0 -> 1
    IF(member_flag = 1 AND IFNULL(prev_member_flag, 0) = 0, 1, 0) AS conversion_occured,

    -- start a new journey if:
    --   first row for user OR gap >= 15 days OR previous row converted
    CASE
      WHEN prev_user IS NULL THEN 1
      WHEN DATE_DIFF(session_date, prev_date, DAY) >= gap_days THEN 1
      WHEN IFNULL(LAG(IF(member_flag = 1 AND IFNULL(prev_member_flag,0)=0,1,0))
                  OVER (PARTITION BY user_pseudo_id ORDER BY session_date, event_ts), 0) = 1 THEN 1
      ELSE 0
    END AS new_journey_flag
  FROM ordered
),
journey_ids AS (
  SELECT
    *,
    -- cumulative journey counter per user
    SUM(new_journey_flag) OVER (PARTITION BY user_pseudo_id ORDER BY session_date, event_ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS journey_idx
  FROM journey_marks
),
-- create one row per dimension per session with a chosen touchpoint label
touchpoints AS (
  SELECT
    user_pseudo_id,
    journey_idx,
    session_date,
    event_ts,
    'medium'       AS dimension,
    COALESCE(final_medium, channelgroup, 'Direct') AS touchpoint,
    conversion_occured
  FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id, journey_idx, session_date, event_ts,
         'source', COALESCE(final_source, 'Direct'), conversion_occured
  FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id, journey_idx, session_date, event_ts,
         'campaign', COALESCE(final_campaign, 'Unspecified'), conversion_occured
  FROM journey_ids
  UNION ALL
  SELECT user_pseudo_id, journey_idx, session_date, event_ts,
         'channelgroup', COALESCE(channelgroup, 'Direct'), conversion_occured
  FROM journey_ids
),
paths AS (
  -- build the ordered path per (user, journey, dimension)
  SELECT
    dimension,
    user_pseudo_id,
    journey_idx,
    STRING_AGG(touchpoint, ' > ' ORDER BY session_date, event_ts) AS journey,
    -- a journey is a conversion if any session in it has conversion_occured = 1
    MAX(conversion_occured) > 0 AS is_conversion
  FROM touchpoints
  GROUP BY dimension, user_pseudo_id, journey_idx
)
-- FINAL: compact table for ChannelAttribution
SELECT
  dimension,
  journey,
  SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) AS conversion,
  SUM(CASE WHEN is_conversion THEN 0 ELSE 1 END) AS na
FROM paths
GROUP BY dimension, journey
ORDER BY dimension, conversion DESC, na DESC;


