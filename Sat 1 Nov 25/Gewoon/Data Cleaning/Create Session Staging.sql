DECLARE start_date STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 89 DAY));
DECLARE end_date   STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());

CREATE OR REPLACE TABLE `your_project.your_dataset.session_staging_gewoon`
PARTITION BY DATE(session_start_ts) AS
WITH raw_events AS (
  SELECT
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    LOWER(event_name) AS event_name,
    user_pseudo_id, user_id,
    (SELECT value.int_value FROM UNNEST(event_params) ep WHERE ep.key='ga_session_id' LIMIT 1) AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) ep WHERE ep.key='ga_session_number' LIMIT 1) AS ga_session_number,
    event_params
  FROM `innova-gewoon.analytics_271623485.events_*`
  WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),
sessions AS (
  SELECT
    COALESCE(CAST(ga_session_id AS STRING),
             CONCAT(user_pseudo_id, '-', CAST(ga_session_number AS STRING)),
             CONCAT(user_pseudo_id, '-', FORMAT_DATE('%Y%m%d', DATE(event_ts)))) AS session_id,
    user_pseudo_id, user_id,
    MIN(event_ts) AS session_start_ts,
    MAX(event_ts) AS session_end_ts,
    COUNT(1) AS session_event_count
  FROM raw_events
  GROUP BY session_id, user_pseudo_id, user_id
),
session_events AS (
  SELECT
    s.session_id, s.user_pseudo_id, s.user_id, s.session_start_ts, s.session_end_ts, s.session_event_count,
    r.event_ts, r.event_name,
    COALESCE(m.family, 'other') AS family
  FROM sessions s
  JOIN raw_events r
    ON (COALESCE(CAST(r.ga_session_id AS STRING),
                 CONCAT(r.user_pseudo_id, '-', CAST(r.ga_session_number AS STRING)),
                 CONCAT(r.user_pseudo_id, '-', FORMAT_DATE('%Y%m%d', DATE(r.event_ts))))
         = s.session_id)
  LEFT JOIN `your_project.your_dataset.event_family_map` m
    ON r.event_name = m.event_name
  ORDER BY s.session_id, r.event_ts
),
session_family_seq AS (
  SELECT session_id,
         STRING_AGG(family, '|' ORDER BY MIN(event_ts)) AS family_seq
  FROM (
    SELECT session_id, family, MIN(event_ts) AS event_ts
    FROM session_events
    GROUP BY session_id, family
  )
  GROUP BY session_id
),
session_events_list AS (
  SELECT session_id,
         ARRAY_AGG(STRUCT(event_ts, event_name, family) ORDER BY event_ts LIMIT 200) AS events_list
  FROM session_events
  GROUP BY session_id
),
session_family_counts AS (
  SELECT session_id, family, COUNT(1) AS cnt FROM session_events GROUP BY session_id, family
),
session_event_counts AS (
  SELECT session_id, event_name, COUNT(1) AS cnt FROM session_events GROUP BY session_id, event_name
),
session_conv AS (
  SELECT session_id, MAX(CASE WHEN event_name IN ('purchase','checkout_step8_succes','bezoek_bedankpagina') THEN 1 ELSE 0 END) = 1 AS is_session_conversion
  FROM session_events
  GROUP BY session_id
)
SELECT
  s.session_id, s.user_pseudo_id, s.user_id,
  s.session_start_ts, s.session_end_ts,
  TIMESTAMP_DIFF(s.session_end_ts, s.session_start_ts, SECOND) AS session_duration_sec,
  s.session_event_count,
  sf.family_seq,
  sel.events_list,
  (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(event_name, cnt) ORDER BY cnt DESC LIMIT 50)) FROM session_event_counts ec WHERE ec.session_id = s.session_id) AS event_counts_json,
  (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(family, cnt) ORDER BY cnt DESC)) FROM session_family_counts fc WHERE fc.session_id = s.session_id) AS family_counts_json,
  IFNULL(sc.is_session_conversion, FALSE) AS is_session_conversion
FROM sessions s
LEFT JOIN session_family_seq sf USING(session_id)
LEFT JOIN session_events_list sel USING(session_id)
LEFT JOIN session_conv sc USING(session_id);
