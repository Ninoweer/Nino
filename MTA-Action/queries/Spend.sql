-- Sawiday MTA daily spend input.
-- Daily spend uses the same channel grouping logic/values as the session input.
--
-- Output columns:
--   date
--   channel_group
--   cost
--
-- No monthly spend. No ORDER BY in the partitioned CTAS.

DECLARE start_date DATE DEFAULT DATE '2026-05-01';
DECLARE end_date   DATE DEFAULT DATE '2026-06-10';

DECLARE campaign_column STRING;
DECLARE campaign_expr STRING;

-- Try to find the campaign-name field in the marketing table.
-- If your table definitely uses another name, add it to this list.
SET campaign_column = (
  SELECT column_name
  FROM `action-dwh.mart_ga4.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'ga_marketing_reporting'
    AND column_name IN (
      'session_campaign',
      'campaign',
      'campaign_name',
      'campaignName',
      'google_ads_campaign_name',
      'google_campaign_name'
    )
  ORDER BY
    CASE column_name
      WHEN 'session_campaign' THEN 1
      WHEN 'campaign' THEN 2
      WHEN 'campaign_name' THEN 3
      WHEN 'campaignName' THEN 4
      WHEN 'google_ads_campaign_name' THEN 5
      WHEN 'google_campaign_name' THEN 6
      ELSE 99
    END
  LIMIT 1
);

SET campaign_expr = IF(
  campaign_column IS NULL,
  'CAST(NULL AS STRING)',
  FORMAT('CAST(`%s` AS STRING)', campaign_column)
);

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `action-dwh.attribution_model_ecom.mta_spend_daily_20260501_20260610`
PARTITION BY date
CLUSTER BY channel_group
AS

WITH spend_base AS (
  SELECT
    date,
    COALESCE(default_channel_grouping, 'Unattributed') AS default_channel_grouping,
    %s AS campaign_name,
    COALESCE(costs, 0.0) AS cost
  FROM `action-dwh.mart_ga4.ga_marketing_reporting`
  WHERE date BETWEEN DATE '2026-05-01' AND DATE '2026-06-10'
),

spend_channel_mapped AS (
  SELECT
    date,

    CASE
      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'performance max'
        THEN 'Performance Max - Generic'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid search'
        AND (
          LOWER(COALESCE(campaign_name, '')) LIKE '%%-brd%%'
          OR LOWER(COALESCE(campaign_name, '')) = 'brd'
        )
        THEN 'Paid Search - Branded'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid search'
        AND (
          LOWER(COALESCE(campaign_name, '')) LIKE '%%-gen%%'
          OR LOWER(COALESCE(campaign_name, '')) = 'gen'
        )
        THEN 'Paid Search - Generic'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid search'
        THEN 'Paid Search - Unclassified'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid shopping'
        AND LOWER(COALESCE(campaign_name, '')) LIKE '%%-brd%%'
        THEN 'Paid Shopping - Branded'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid shopping'
        AND LOWER(COALESCE(campaign_name, '')) LIKE '%%-gen%%'
        THEN 'Paid Shopping - Generic'

      WHEN LOWER(COALESCE(default_channel_grouping, '')) = 'paid shopping'
        THEN 'Paid Shopping - Unclassified'

      ELSE COALESCE(default_channel_grouping, 'Unattributed')
    END AS channel_group,

    cost
  FROM spend_base
)

SELECT
  date,
  channel_group,
  SUM(cost) AS cost
FROM spend_channel_mapped
GROUP BY
  date,
  channel_group
""", campaign_expr);
