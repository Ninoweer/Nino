---- For Sunweb
-- ▶️ 0) DROP any old audit objects
DROP TABLE IF EXISTS
  `amazon-vendor-reporting.audits.table_increments`;

DROP SCHEMA IF EXISTS
  `amazon-vendor-reporting`.`audits`;

-- ▶️ 1) CREATE the audit dataset in the EU multi-region
CREATE SCHEMA IF NOT EXISTS
  `amazon-vendor-reporting`.`audits`
OPTIONS(
  location = "EU"
);

-- ▶️ 2) CREATE the partitioned & clustered audit table
CREATE TABLE IF NOT EXISTS
  `amazon-vendor-reporting.audits.table_increments` (
    audit_date DATE     NOT NULL,
    dataset    STRING   NOT NULL,
    table_name STRING   NOT NULL,
    row_count  INT64    NOT NULL
  )
PARTITION BY
  audit_date
CLUSTER BY
  dataset, table_name
OPTIONS(
  description = "Daily full-table COUNT(*) snapshots per dataset.table"
);


-- ▶️ 3) POPULATE yesterday’s & today’s full counts for every table
DECLARE project_id STRING    DEFAULT 'amazon-vendor-reporting';
DECLARE ds_list    ARRAY<STRING>;
DECLARE tbl_list   ARRAY<STRING>;
DECLARE i          INT64     DEFAULT 0;
DECLARE j          INT64     DEFAULT 0;
DECLARE ds         STRING;
DECLARE tbl        STRING;

-- 3a) Get all non-audit datasets in the project
EXECUTE IMMEDIATE FORMAT("""
  SELECT COALESCE(ARRAY_AGG(schema_name), [])
  FROM `%s`.INFORMATION_SCHEMA.SCHEMATA
  WHERE schema_name NOT IN ('audit','INFORMATION_SCHEMA')
""", project_id)
INTO ds_list;

-- 3b) For each dataset, discover its tables and insert counts
WHILE i < ARRAY_LENGTH(ds_list) DO
  SET ds = ds_list[ORDINAL(i+1)];

  -- list all base tables in this dataset
  EXECUTE IMMEDIATE FORMAT("""
    SELECT COALESCE(ARRAY_AGG(table_name), [])
    FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
  """, project_id, ds)
  INTO tbl_list;

  SET j = 0;
  WHILE j < ARRAY_LENGTH(tbl_list) DO
    SET tbl = tbl_list[ORDINAL(j+1)];

    -- yesterday’s count via time‐travel
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audits.table_increments`
        (audit_date, dataset, table_name, row_count)
      SELECT
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS audit_date,
        '%s'                                     AS dataset,
        '%s'                                     AS table_name,
        COUNT(*)                                 AS row_count
      FROM `%s.%s.%s`
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    """,
      project_id,
      ds,
      tbl,
      project_id, ds, tbl
    );

    -- today’s live count
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO `%s.audits.table_increments`
        (audit_date, dataset, table_name, row_count)
      SELECT
        CURRENT_DATE() AS audit_date,
        '%s'           AS dataset,
        '%s'           AS table_name,
        COUNT(*)       AS row_count
      FROM `%s.%s.%s`
    """,
      project_id,
      ds,
      tbl,
      project_id, ds, tbl
    );

    SET j = j + 1;
  END WHILE;

  SET i = i + 1;
END WHILE;
