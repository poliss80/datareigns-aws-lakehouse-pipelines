# DataReigns AWS Lakehouse Pipelines

Sample project: event driven ingestion into a lakehouse built on AWS patterns,
running locally with only Python and PyYAML.

## Problem this solves

Raw event and catalog files land in a bucket at all hours. Somebody has to
notice them, check they are well formed, shape them into clean partitioned
datasets, and register them where analysts can find them. Done by hand this is
slow and fragile. Done as code it is repeatable: every file either lands in
the curated zone or lands in quarantine with an alert explaining why.

This project is that code, using local folders in place of S3 so the whole
thing runs on a laptop.

## Architecture

```
s3_landing   ==>   trigger.py   ==>   orchestrator.py   ==>   s3_curated
raw JSON          S3 event           YAML state              partitioned CSV
and CSV           to Lambda          machine: validate,      plus _SUCCESS
files land        stand in           transform, catalog,     markers
here                                 notify
                                        ||
                                        vv
                                   catalog.json   +   curated.db
                                   Glue Data          SQLite mirror
                                   Catalog            for local queries
                                   stand in

alerts.log collects completion notes and failure alerts, the SNS stand in.
quarantine/ holds files that failed after retries, the DLQ stand in.
```

## Quickstart

1. Install the one dependency:
   pip install pyyaml

2. Create sample raw files in the landing bucket:
   python data/generate_data.py

3. Run the trigger. It finds new files and runs each pipeline:
   python src/trigger.py

4. Inspect the results:
   curated partitions under s3_curated/curated/
   table entries in catalog.json
   completion and alert lines in alerts.log

5. Run the sample queries against the SQLite mirror:
   sqlite3 curated.db < athena_queries.sql

## Config reference

Each pipeline is one YAML file in configs/. Fields:

pipeline: unique name, used in logs and quarantine paths
description: what the pipeline does
trigger.landing_dir: bucket folder to watch (s3_landing)
trigger.prefix: folder under the bucket, e.g. raw/clickstream
trigger.file_pattern: glob for new files, e.g. *.json
source.format: json_lines or csv
source.required_fields: every record must carry these
transform.kind: partitioned_events or scd1_dimension
transform.derive_event_date_from: timestamp field used to derive event_date (events only)
curated.table: table name registered in the catalog
curated.base_path: where curated files are written
curated.format: csv
curated.partition_by: partition columns, e.g. [region]
curated.columns: ordered column list with types (string, timestamp, date, decimal, integer, boolean)
curated.scd_type and curated.merge_key: set for SCD1 dimensions; incoming rows overwrite on the merge_key
stages: ordered stage list; each stage has a name and a retries value

Two transform kinds ship with the project:
partitioned_events: JSON events to region partitioned CSV with _SUCCESS markers
scd1_dimension: CSV rows merged into a dimension table on the merge key (type 1 means latest wins)

## State machine reference

Stages run in the listed order:

validate: parse the file and require every record to carry the required fields. Empty files and missing fields fail here.
transform: shape records and write the curated output (partitioned CSV or SCD1 merge).
catalog: register the table in catalog.json (columns, types, partitions, location) and mirror rows into curated.db.
notify: append a completion line to alerts.log.

Retry: a stage that raises is retried per its retries value. retries: 2 means 3 total attempts. When attempts run out, the source file moves to quarantine/<pipeline>/ and an ALERT line goes to alerts.log. This mirrors Step Functions Retry plus Catch routing to a dead letter queue with SNS notification.

## Production mapping

Local stand in  ==>  AWS service
s3_landing/ and s3_curated/  ==>  Amazon S3 buckets (add versioning and lifecycle rules)
trigger.py  ==>  Lambda function subscribed to S3 ObjectCreated event notifications
YAML state machines plus orchestrator.py  ==>  Step Functions state machines with native Retry and Catch
catalog.json  ==>  Glue Data Catalog tables, via crawler or explicit table definitions
Partitioned CSV files  ==>  Parquet files on S3 (swap the CSV writer; Athena prefers columnar storage)
curated.db  ==>  local validation only; not part of the AWS design
athena_queries.sql  ==>  Athena queries against the Glue tables, unchanged
alerts.log  ==>  SNS topic for notifications
quarantine/  ==>  SQS dead letter queue for failed files

## Notes

All sample data is synthetic, generated locally with a fixed seed.
Table and column names match across catalog.json, curated.db, and athena_queries.sql, so every query runs as is in both places.
CSV keeps this demo dependency free. In production, swap the writer for Parquet and the queries stay the same.
