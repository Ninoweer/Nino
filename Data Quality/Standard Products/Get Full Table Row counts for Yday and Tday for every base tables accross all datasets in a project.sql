/*
  audit_table_increments_sunweb.sql
  -----------------------------------
  Purpose:  Capture full-table row counts for "yesterday" and "today"
            for every base table across all datasets in a project.
            Stores results in a partitioned & clustered audit table
            (dataset: `audits`, table: `table_increments`).

  Usage:
  1) In BigQuery Console, set:
     - Query Location = europe-west4
     - Enable scripting
  2) Copy & paste the entire script below and click RUN.
  3) Schedule this script daily (e.g. via Cloud Scheduler → Cloud Function
     or an Airflow DAG) to keep the audit table up to date.

  Reusing for other clients:
  - Replace `project_id` with your GCP project.
  - Rename the audit dataset (`audits`) if needed.
  - If your region differs, adjust the LOCATION in CREATE SCHEMA.
  - Ensure your users have permission to read INFORMATION_SCHEMA
    and write to the audit dataset.
*/

-- ▶️ 0) DROP any existing audit objects so we start clean
DROP TABLE IF EXISTS
  `{{PROJECT_ID}}.audits.table_increments`;
DROP SCHEMA IF EXISTS
  `{{PROJECT_ID}}`.`audits`;

-- ▶️ 1) CREATE the audit dataset in the target region
CREATE SCHEMA IF NOT EXISTS
  `{{PROJECT_ID}}`.`audits`
OPTIONS(
  location = "europe-west4"  -- change if your data is in a different region
);

-- ▶️ 2) CREATE the partitioned & clustered audit table
CREATE TABLE IF NOT EXISTS
  `{{PROJECT_ID}}.audits.table_increments` (
    audit_date DATE     NOT NULL,  -- the date of the snapshot
    dataset    STRING   NOT NULL,  -- source dataset name
    table_name STRING   NOT NULL,  -- source table name
    row_count  INT64    NOT NULL   -- full-table row count
)
PARTITION BY
  audit_date                     -- partition for efficient date filtering
CLUSTER BY
  dataset, table_name            -- clustering for faster filtering by dataset/table
OPTIONS(
  description = "Daily full-table COUNT(*) snapshots per dataset.table"
);

-- ▶️ 3) POPULATE yesterday’s & today’s counts for every table
DECLARE project_id STRING    DEFAULT '{{PROJECT_ID}}';
DECLARE ds_list    ARRAY<STRING>;
DECLARE tbl_list   ARRAY<STRING>;
DECLARE i          INT64     DEFAULT 0;
DECLARE j          INT64     DEFAULT 0;
DECLARE ds         STRING;
DECLARE tbl        STRING;

-- 3a) Discover all user datasets (exclude the audit dataset itself)
EXECUTE IMMEDIATE FORMAT("""
  SELECT COALESCE(ARRAY_AGG(schema_name), [])
  FROM `%s`.INFORMATION_SCHEMA.SCHEMATA
  WHERE schema_name NOT IN ('audits', 'INFORMATION_SCHEMA')
""", project_id)
INTO ds_list;

-- 3b) Loop through each dataset
WHILE i < ARRAY_LENGTH(ds_list) DO
  SET ds = ds_list[ORDINAL(i+1)];

  -- 3b.i) Discover all base tables in the current dataset
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COALESCE(ARRAY_AGG(table_name), [])
    FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
  """, project_id, ds)
  INTO tbl_list;

  -- 3b.ii) Loop through each table
  SET j = 0;
  WHILE j < ARRAY_LENGTH(tbl_list) DO
    SET tbl = tbl_list[ORDINAL(j+1)];

    -- 3b.ii.a) Insert yesterday's full count via time-travel
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audits.table_increments`
        (audit_date, dataset, table_name, row_count)
      SELECT
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS audit_date,
        '%s' AS dataset,
        '%s' AS table_name,
        COUNT(*) AS row_count
      FROM `%s.%s.%s`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    """,
      project_id, ds, tbl,
      project_id, ds, tbl
    );

    -- 3b.ii.b) Insert today's full live count
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audits.table_increments`
        (audit_date, dataset, table_name, row_count)
      SELECT
        CURRENT_DATE() AS audit_date,
        '%s' AS dataset,
        '%s' AS table_name,
        COUNT(*) AS row_count
      FROM `%s.%s.%s`
    """,
      project_id, ds, tbl,
      project_id, ds, tbl
    );

    SET j = j + 1;
  END WHILE;

  SET i = i + 1;
END WHILE;

/*
  After running:
  - Query `{{PROJECT_ID}}.audits.table_increments`
    to see two rows per table (yesterday & today).
  - Use a pivot or simple SELECT to compute diff and % change:

    SELECT
      dataset,
      table_name,
      MAX(IF(audit_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), row_count, NULL)) AS yesterday_count,
      MAX(IF(audit_date = CURRENT_DATE(), row_count, NULL)) AS today_count,
      (today_count - yesterday_count) AS diff,
      SAFE_DIVIDE(today_count - yesterday_count, yesterda
      y_count) * 100 AS pct_change
    FROM `{{PROJECT_ID}}.audits.table_increments`
    GROUP BY dataset, table_name;

  - Point Looker Studio to this table or use a Custom Query
    to visualize row-count trends and % changes.

  Reuse Steps for Other Clients:
  1) Replace `{{PROJECT_ID}}` with your own project ID.
  2) (Optional) Change the audit dataset name if `audits` is taken.
  3) Adjust `location` in CREATE SCHEMA if your data lives outside europe-west4.
  4) Ensure proper IAM: users need read on source datasets and write on audit dataset.
  5) Modify schedule frequency – daily is typical, but you can run more or less often.
*/
