# Log Processing Pipeline on AWS (Sample Project)

A sample re-creation of a log processing pipeline pattern I built for 5G network test logs at Nokia. This repository uses **sample data only** — it contains no Nokia code or data.

The pipeline ingests raw log files, uploads them to Amazon S3 in date partitions, runs data quality checks, parses each line into structured columns with PySpark on AWS Glue, separates good and bad records, and writes the clean output as Parquet. Jenkins runs the whole flow nightly as CI/CD.

## Architecture

~~~
Raw log files (local folder, simulating an NFS mount)
        |
        v
scripts/upload_to_s3.py  -- incremental upload (files changed in last 24h)
        |
        v
S3  raw/logs/year=YYYY/month=MM/day=DD/
        |
        v
AWS Glue job (PySpark)  -- scripts/glue_transform.py
   - data quality checks (row count, empty lines, valid log levels)
   - regex parsing -> timestamp, log_level, message
   - adds processed_date
   - splits good vs bad records
        |
        +--> S3  processed/logs/   (Parquet, good records)
        +--> S3  bad_records/      (unparsed lines, kept for investigation)
~~~

Processed Parquet is ready to be catalogued with a Glue Crawler and queried with Amazon Athena.

## CI/CD with Jenkins

The `Jenkinsfile` runs nightly (`cron('0 0 * * *')`) and on every build:

1. **Checkout** the code from GitHub
2. **Verify** the AWS connection
3. **Run unit tests** with pytest — the pipeline stops if any test fails
4. **Upload** the latest Glue script to S3
5. **Trigger** the Glue job and poll its status until it succeeds or fails

AWS keys are read from Jenkins credentials (`aws-access-key`, `aws-secret-key`), never stored in the code.

## Repository structure

~~~
.
├── Jenkinsfile                 # CI/CD pipeline
├── requirements.txt            # boto3, pytest
├── scripts/
│   ├── upload_to_s3.py         # incremental, date-partitioned upload to S3
│   └── glue_transform.py       # Glue PySpark job: quality checks, parsing, Parquet output
├── tests/
│   ├── test_upload.py          # tests for partitioning and new-file detection
│   └── test_transform.py       # tests for the log parsing pattern
└── data/sample_logs/app.log    # sample log file
~~~

## Running it

**Prerequisites:** Python 3, AWS CLI configured, an S3 bucket, and a Glue job whose script location points to `scripts/glue_transform.py` in S3. Update `BUCKET_NAME`, `GLUE_JOB_NAME` and the S3 paths for your own account.

~~~bash
pip install -r requirements.txt

# run unit tests (from the repository root)
python3 -m pytest tests/ -v

# upload new log files to S3
python3 scripts/upload_to_s3.py
~~~

The Glue job itself runs on AWS Glue, which provides PySpark and the `awsglue` library.

## Data quality checks

Before transforming, the Glue job checks that:

- the input is not empty (the job stops if it is)
- empty lines are flagged
- every line carries a valid log level (ERROR, INFO, WARN)

Lines that do not match the expected format are written to `bad_records/` instead of being dropped silently.

## Planned improvements

- 5G-style sample logs with RRC messages and RF KPIs (BLER, RSRP, RSRQ), plus KPI parsing and aggregation
- Partition processed output by `processed_date` so each nightly run keeps history
- Move parsing logic into functions so tests exercise the Glue code directly
- Process only new raw partitions in the Glue job

## Tech stack

Python · PySpark · AWS Glue · Amazon S3 · Parquet · Amazon Athena · Jenkins · pytest · boto3
