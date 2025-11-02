-- USER STAGING (Gewoonenergie) - SELECT (family = event_name)
WITH raw_events AS (
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    LOWER(event_name) AS event_name,
    user_pseudo_id,
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) ep WHERE ep.key='ga_session_id' LIMIT 1) AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) ep WHERE ep.key='ga_session_number' LIMIT 1) AS ga_session_number,
    event_params
  FROM `innova-gewoon.analytics_271623485.events_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 89 DAY))
                        AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
),
-- carry session identifiers so we can count distinct sessions
events_with_session AS (
  SELECT
    user_pseudo_id,
    event_ts,
    event_name,
    ga_session_id,
    ga_session_number,
    event_name AS family
  FROM raw_events
),
user_event_counts AS (
  SELECT user_pseudo_id, event_name, COUNT(1) AS cnt
  FROM events_with_session
  GROUP BY user_pseudo_id, event_name
),
user_event_counts_arr AS (
  SELECT user_pseudo_id,
         ARRAY_AGG(STRUCT(event_name, cnt) ORDER BY cnt DESC LIMIT 100) AS event_counts_arr
  FROM user_event_counts
  GROUP BY user_pseudo_id
),
user_family_counts AS (
  SELECT user_pseudo_id, family, COUNT(1) AS cnt
  FROM events_with_session
  GROUP BY user_pseudo_id, family
),
user_family_counts_arr AS (
  SELECT user_pseudo_id,
         ARRAY_AGG(STRUCT(family, cnt) ORDER BY cnt DESC) AS family_counts_arr
  FROM user_family_counts
  GROUP BY user_pseudo_id
),
user_agg AS (
  SELECT
    user_pseudo_id,
    MIN(event_ts) AS first_touch_ts,
    MAX(event_ts) AS last_active_ts,
    COUNT(1) AS total_events,
    COUNT(DISTINCT COALESCE(CAST(ga_session_id AS STRING),
                            CONCAT(user_pseudo_id,'-',CAST(ga_session_number AS STRING)),
                            CONCAT(user_pseudo_id,'-',FORMAT_DATE('%Y%m%d', DATE(event_ts))))) AS num_sessions
  FROM events_with_session
  GROUP BY user_pseudo_id
),
user_conv AS (
  SELECT user_pseudo_id,
         MAX(CASE WHEN event_name IN ('purchase','checkout_step8_succes','bezoek_bedankpagina') THEN 1 ELSE 0 END) = 1 AS is_user_conversion
  FROM raw_events
  GROUP BY user_pseudo_id
)
SELECT
  ua.user_pseudo_id,
  ua.first_touch_ts,
  ua.last_active_ts,
  ua.total_events,
  ua.num_sessions,
  TO_JSON_STRING(uec.event_counts_arr) AS event_counts_json,
  TO_JSON_STRING(ufc.family_counts_arr) AS family_counts_json,
  IFNULL(uc.is_user_conversion, FALSE) AS is_user_conversion
FROM user_agg ua
LEFT JOIN user_event_counts_arr uec USING(user_pseudo_id)
LEFT JOIN user_family_counts_arr ufc USING(user_pseudo_id)
LEFT JOIN user_conv uc USING(user_pseudo_id)
ORDER BY ua.last_active_ts DESC
