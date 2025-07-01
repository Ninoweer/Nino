# BigQuery Health‐Check Jobs

## Overview
This repo contains BigQuery scripts to:
1. Snapshot full table counts (yesterday vs. today)
2. Store results in a partitioned/clustered audit table
3. Surface row‐count deltas in LookerStudio

## Getting started
1. Copy `audit_table_increments.sql.tpl` → `sql/<client>/audit_table_increments_<client>.sql`
2. Update at top:
   ```sql
   DECLARE project_id   STRING DEFAULT '<YOUR_PROJECT_ID>';
   DECLARE dataset_list ARRAY<STRING> DEFAULT [ 'analytics_xxx', … ];
