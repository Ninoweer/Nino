CREATE TEMP FUNCTION ERF_JS(x FLOAT64) RETURNS FLOAT64 LANGUAGE js AS """
  var sign = (x < 0) ? -1 : 1;
  x = Math.abs(x);
  var a1=0.254829592, a2=-0.284496736, a3=1.421413741,
      a4=-1.453152027, a5=1.061405429, p=0.3275911;
  var t = 1/(1+p*x);
  var y = 1 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x);
  return sign*y;
""";

CREATE TEMP FUNCTION ONE_SIDED_P(z FLOAT64) RETURNS FLOAT64 AS (
  0.5 * (1 - ERF_JS(ABS(z)/SQRT(2)))
);


/* ════════════════════════ 1. RAW EVENTS ════════════════════════ */
WITH raw AS (
  -- Pigprogress
  SELECT user_pseudo_id , event_timestamp , event_name ,
         ecommerce.transaction_id        AS transaction_id ,
         ecommerce.purchase_revenue      AS purchase_revenue ,
         device.category                 AS Devicecategory ,
         'Pigprogress'                   AS Brand ,
         (SELECT value.int_value    FROM UNNEST(user_properties) WHERE key='testId')    AS experimentId ,
         (SELECT value.string_value FROM UNNEST(user_properties) WHERE key='version')   AS version
  FROM `symbolic-tape-385112.analytics_307369923.events_*`
  WHERE _TABLE_SUFFIX >= '20250601'

  UNION ALL
  -- Boerderij
  SELECT user_pseudo_id , event_timestamp , event_name ,
         ecommerce.transaction_id , ecommerce.purchase_revenue ,
         device.category , 'Boerderij' AS Brand ,
         (SELECT value.int_value    FROM UNNEST(user_properties) WHERE key='testId'),
         (SELECT value.string_value FROM UNNEST(user_properties) WHERE key='version')
  FROM `symbolic-tape-385112.analytics_249585125.events_*`
  WHERE _TABLE_SUFFIX >= '20250601'

  UNION ALL
  -- Trekkeronline
  SELECT user_pseudo_id , event_timestamp , event_name ,
         ecommerce.transaction_id , ecommerce.purchase_revenue ,
         device.category , 'Trekkeronline' AS Brand ,
         (SELECT value.int_value    FROM UNNEST(user_properties) WHERE key='testId'),
         (SELECT value.string_value FROM UNNEST(user_properties) WHERE key='version')
  FROM `symbolic-tape-385112.analytics_266032046.events_*`
  WHERE _TABLE_SUFFIX >= '20250601'
),

kpi_events AS (
  SELECT *
  FROM   raw
  WHERE  event_name IN ('view_item_list','select_item','view_item',
                        'begin_checkout','add_payment_info','purchase')
),

/* ════════════════════════ 2. VISITOR FILTERS ════════════════════════ */
visitors AS (
  SELECT
      user_pseudo_id ,
      experimentId ,
      CASE
        WHEN ANY_VALUE(version) IN ('Slot 1','Slot 2','Slot 3','Slot 4')  THEN 'Control'
        WHEN ANY_VALUE(version) IN ('Slot 5','Slot 6','Slot 7','Slot 8','Slot 9','Slot 10')
                                                                        THEN 'Test'
        ELSE ANY_VALUE(version)
      END                 AS Variant ,
      ARRAY_AGG(DISTINCT Devicecategory) AS dev_arr ,
      ARRAY_AGG(DISTINCT Brand)          AS brand_arr ,
      MIN(event_timestamp)               AS start_ts ,
      MAX(event_timestamp)               AS end_ts
  FROM kpi_events
  WHERE experimentId IS NOT NULL
  GROUP BY user_pseudo_id , experimentId
  HAVING ARRAY_LENGTH(dev_arr)=1
     AND ARRAY_LENGTH(brand_arr)=1
     AND Variant IN ('Control','Test')
),

valid_exp AS (      -- experiments with both variants
  SELECT experimentId
  FROM   visitors
  GROUP  BY experimentId
  HAVING COUNTIF(Variant='Control')>0 AND COUNTIF(Variant='Test')>0
),

users AS (
  SELECT
      v.user_pseudo_id ,
      v.experimentId ,
      v.Variant ,
      v.dev_arr[OFFSET(0)]   AS Devicecategory ,
      v.brand_arr[OFFSET(0)] AS Brand ,
      v.start_ts ,
      v.end_ts ,
      CONCAT(v.brand_arr[OFFSET(0)],'|',v.user_pseudo_id) AS vkey
  FROM visitors v
  JOIN valid_exp USING (experimentId)
),

/* ════════════════════════ 3. SPLIT DIMENSION GRID ════════════════════════ */
/* ════════════════════════ 3-bis.  SPLIT DIMENSION GRID  (type-safe) ════════════════════════ */
split_dim AS (
  SELECT *
  FROM UNNEST([
      -- tg      BrandKey   DeviceKey
      STRUCT('orig' AS tg , 'orig' AS BrandKey , 'orig' AS DeviceKey),   -- Brand × Device (no roll-up)
      STRUCT('bAll' AS tg , NULL   AS BrandKey , 'orig' AS DeviceKey),   -- Brand = All   (Device kept)
      STRUCT('dAll' AS tg , 'orig' AS BrandKey , NULL   AS DeviceKey),   -- Device = All  (Brand kept)
      STRUCT('totl' AS tg , NULL   AS BrandKey , NULL   AS DeviceKey)    -- Brand = All & Device = All
  ]) s
),


/* ════════════════════════ 4.  KPI NUMERATORS & VISIT DENOMINATORS ════════════════════════ */
/* ════════════════════════ 4-bis.  KPI source rows – no alias reuse ════════════════════════ */
kpi_raw AS (

  /* ----  (1) All KPI events as simple counts  -------------------- */
  SELECT
      u.user_pseudo_id ,
      u.experimentId ,
      u.Variant ,
      u.Brand ,
      u.Devicecategory ,

      CASE
        WHEN e.event_name = 'purchase' THEN 'numberOfConversions'
        ELSE e.event_name
      END                     AS kpi ,

      1                       AS kpi_val
  FROM   users      AS u
  JOIN   kpi_events AS e
         ON  e.user_pseudo_id = u.user_pseudo_id
        AND e.event_timestamp BETWEEN u.start_ts AND u.end_ts
        AND e.experimentId     = u.experimentId

  UNION ALL

  /* ----  (2) Same purchase rows but as revenue  ------------------ */
  SELECT
      u.user_pseudo_id ,
      u.experimentId ,
      u.Variant ,
      u.Brand ,
      u.Devicecategory ,
      'purchase_revenue'       AS kpi ,
      e.purchase_revenue       AS kpi_val
  FROM   users      AS u
  JOIN   kpi_events AS e
         ON  e.user_pseudo_id = u.user_pseudo_id
        AND e.event_timestamp BETWEEN u.start_ts AND u.end_ts
        AND e.experimentId     = u.experimentId
  WHERE  e.event_name = 'purchase'
        AND e.purchase_revenue IS NOT NULL     -- guard against nulls
),

/* ── apply the four split views in one pass ───────────────────────────── */
kpi_split AS (
  SELECT
      IF(s.BrandKey  IS NULL,            'All', IF(s.BrandKey  ='orig', k.Brand, 'All'))            AS Brand ,
      IF(s.DeviceKey IS NULL,            'All', IF(s.DeviceKey ='orig', k.Devicecategory,'All'))    AS Devicecategory ,
      k.experimentId ,
      k.Variant ,
      k.kpi ,
      SUM(k.kpi_val)                     AS kpi_value
  FROM   kpi_raw k
  CROSS  JOIN split_dim s
  GROUP  BY Brand,Devicecategory,experimentId,Variant,kpi
),

visits_split AS (
  SELECT
      IF(s.BrandKey  IS NULL,            'All', IF(s.BrandKey  ='orig', u.Brand, 'All'))            AS Brand ,
      IF(s.DeviceKey IS NULL,            'All', IF(s.DeviceKey ='orig', u.Devicecategory,'All'))    AS Devicecategory ,
      u.experimentId ,
      COUNT(DISTINCT IF(u.Variant='Control', u.vkey, NULL))   AS numvisit_control ,
      COUNT(DISTINCT IF(u.Variant='Test',    u.vkey, NULL))   AS numvisit_test ,
      COUNT(DISTINCT u.vkey)                                  AS numvisit_total
  FROM   users u
  CROSS  JOIN split_dim s
  GROUP  BY Brand,Devicecategory,experimentId
),

/* ════════════════════════ 5.  PIVOT CONTROL / TEST ════════════════════════ */
kpi_pivot AS (
  SELECT
      Brand , Devicecategory , experimentId , kpi ,
      SUM(IF(Variant='Control', kpi_value, 0)) AS kpi_control ,
      SUM(IF(Variant='Test',    kpi_value, 0)) AS kpi_test ,
      SUM(kpi_value)                           AS kpi_total
  FROM kpi_split
  GROUP BY Brand,Devicecategory,experimentId,kpi
),

/* ───────────────────────────────────────────────────────────────────
   6-bis. BUILD THE FULL PIVOT WITH RATES + DENOMINATORS
─────────────────────────────────────────────────────────────────── */
pivot_tbl AS (
  SELECT
    v.Brand,
    v.Devicecategory,
    v.experimentId,
    k.kpi,

    -- denominators from visits_split
    v.numvisit_control,
    v.numvisit_test,
    v.numvisit_total,

    -- raw KPI sums from kpi_pivot
    k.kpi_control,
    k.kpi_test,
    k.kpi_total,

    -- per-visitor rates
    SAFE_DIVIDE(k.kpi_control, v.numvisit_control) AS kpi_rate_control,
    SAFE_DIVIDE(k.kpi_test   , v.numvisit_test)    AS kpi_rate_test,
    SAFE_DIVIDE(k.kpi_total  , v.numvisit_total)   AS kpi_rate_total
FROM visits_split AS v
JOIN kpi_pivot   AS k
  ON  v.Brand          = k.Brand
  AND v.Devicecategory = k.Devicecategory
  AND v.experimentId   = k.experimentId
-- no USING(kpi) here, we only join on the three shared keys

),

/* ════════════════════════ 6.  PER-VISITOR COUNTS → STATS ════════════════════════ */
/* ════════════════════════ 6-bis.  PER-VISITOR COUNTS → STATS  (fixed alias) ════════════════════════ */
user_counts AS (
  /* ----  per-visitor KPI total ----------------------------------- */
  WITH user_kpi AS (
    SELECT
        u.Brand ,
        u.Devicecategory ,
        u.experimentId ,
        u.Variant ,
        u.user_pseudo_id ,
        k.kpi ,
        SUM(k.kpi_val) AS kpi_cnt
    FROM   users     AS u
    JOIN   kpi_raw   AS k
           ON k.user_pseudo_id = u.user_pseudo_id
          AND k.experimentId   = u.experimentId
    GROUP  BY u.Brand,u.Devicecategory,u.experimentId,
             u.Variant,u.user_pseudo_id,k.kpi
  )

  /* ----  apply the four split views ------------------------------ */
  SELECT
      IF(s.BrandKey  IS NULL,'All',IF(s.BrandKey  = 'orig', uk.Brand , 'All'))  AS Brand ,
      IF(s.DeviceKey IS NULL,'All',IF(s.DeviceKey = 'orig', uk.Devicecategory , 'All')) AS Devicecategory ,
      uk.experimentId ,
      uk.Variant ,
      uk.kpi ,
      COUNT(*)                           AS n_vis ,
      SUM(uk.kpi_cnt)                    AS sum_cnt ,
      SUM(uk.kpi_cnt * uk.kpi_cnt)       AS sum_sq
  FROM   user_kpi  AS uk
  CROSS  JOIN split_dim AS s
  GROUP  BY Brand,Devicecategory,experimentId,Variant,kpi
),


stats AS (
  SELECT
      Brand , Devicecategory , experimentId , kpi , Variant ,
      n_vis                                 AS n ,
      sum_cnt / n_vis                       AS mean_kpi ,
      GREATEST((sum_sq - sum_cnt*sum_cnt/n_vis) / NULLIF(n_vis-1,0), 0) AS var_kpi
  FROM user_counts
),

/* ════════════════════════ 6-bis.  TEST-STAT  (safe-divide, no ε noise) ════════════════════════ */
/* ════════════════════════ 7-bis.  t / z on RATE difference ════════════════════════ */
test_stat AS (
  SELECT
      c.Brand ,
      c.Devicecategory ,
      c.experimentId ,
      c.kpi ,

      /* z-test for conversion counts (proportions) */
      CASE
        WHEN c.kpi = 'numberOfConversions' THEN
          SAFE_DIVIDE(
            /* rate_test – rate_control */
            p.kpi_rate_test - p.kpi_rate_control,
            SQRT(
              SAFE_DIVIDE(c.mean_kpi + t.mean_kpi , c.n + t.n)
            * (1 - SAFE_DIVIDE(c.mean_kpi + t.mean_kpi , c.n + t.n))
            * (SAFE_DIVIDE(1,c.n) + SAFE_DIVIDE(1,t.n))
          ) )

      /* Welch-t for all other KPIs, using RATE difference in the numerator */
        ELSE
          SAFE_DIVIDE(
            p.kpi_rate_test - p.kpi_rate_control ,                 -- numerator
            SQRT( SAFE_DIVIDE(c.var_kpi , c.n) + SAFE_DIVIDE(t.var_kpi , t.n) )
          )
      END AS test_statistic
  FROM  stats c                              -- control moments
  JOIN  stats t                              -- test moments
        ON  t.Brand          = c.Brand
       AND t.Devicecategory = c.Devicecategory
       AND t.experimentId   = c.experimentId
       AND t.kpi            = c.kpi
  JOIN pivot_tbl AS p
    ON  p.Brand          = c.Brand
   AND p.Devicecategory = c.Devicecategory
   AND p.experimentId   = c.experimentId
   AND p.kpi            = c.kpi
  WHERE c.Variant='Control' AND t.Variant='Test'
)

/* ════════════════════════ 8.  OUTPUT  ════════════════════════ */
SELECT
    p.* ,
    ts.test_statistic ,
    ONE_SIDED_P(ts.test_statistic)                              AS p_value ,
    CASE WHEN ts.test_statistic > 0
         THEN 1 - ONE_SIDED_P(ts.test_statistic)                -- Test lifts ≥ 0
         ELSE      ONE_SIDED_P(ts.test_statistic)               -- Test ≤ Control
    END                                                         AS chance_B_over_A ,
    SAFE_DIVIDE( ( kpi_rate_test  - kpi_rate_control ), kpi_rate_control) AS relative_uplift ,
    SAFE_DIVIDE( (kpi_rate_control - kpi_rate_test ) , kpi_rate_test)    AS rel_uplift_ctrl
FROM   pivot_tbl p
LEFT JOIN test_stat AS ts
  USING (Brand, Devicecategory, experimentId, kpi)
ORDER BY experimentId, Brand, Devicecategory, kpi;

