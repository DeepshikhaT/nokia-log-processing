# ─────────────────────────────────────────
# Script  : glue_transform.py
# Purpose : Read raw logs from S3, transform and write as Parquet
# Author  : Your Name
# ─────────────────────────────────────────

# ─────────────────────────────────────────
# Imports
# ─────────────────────────────────────────
from pyspark.sql.functions import regexp_extract, lit   # For transformations
from pyspark.context import SparkContext                 # PySpark entry point
from awsglue.context import GlueContext                  # Glue wrapper on Spark
from datetime import datetime                            # To get todays date

# ─────────────────────────────────────────
# Initialize Glue and Spark
# ─────────────────────────────────────────
glue_context = GlueContext(SparkContext.getOrCreate())
spark        = glue_context.spark_session

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
INPUT_PATH       = "s3://nokia-log-processing-bucket/raw/logs/"
OUTPUT_PATH      = "s3://nokia-log-processing-bucket/processed/logs/"
BAD_RECORDS_PATH = "s3://nokia-log-processing-bucket/bad_records/"

# Todays date — will be added as processed_date column
today = datetime.now().strftime("%Y-%m-%d")

# ─────────────────────────────────────────
# Step 1 — Read raw logs from S3
# Each line in log file becomes one row
# All rows go into single column called "value"
# ─────────────────────────────────────────
print("Reading raw logs from S3...")
df = spark.read.text(INPUT_PATH)
print(f"Total lines read: {df.count()}")

# ─────────────────────────────────────────
# Step 2 — Define regex pattern
# Pattern has 3 groups (boxes):
# Group 1 → timestamp
# Group 2 → log level (ERROR/INFO/WARN)
# Group 3 → message
# ─────────────────────────────────────────
pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(ERROR|INFO|WARN)\s+(.*)"

# ─────────────────────────────────────────
# Step 3 — Extract columns using regexp_extract
# Each row goes through pattern matching
# Group number tells which box to pick
# ─────────────────────────────────────────
print("Extracting columns from raw log lines...")
df = df.withColumn("timestamp",  regexp_extract("value", pattern, 1))
df = df.withColumn("log_level",  regexp_extract("value", pattern, 2))
df = df.withColumn("message",    regexp_extract("value", pattern, 3))

# ─────────────────────────────────────────
# Step 4 — Add processed_date column
# lit() adds same constant value to every row
# ─────────────────────────────────────────
df = df.withColumn("processed_date", lit(today))

# ─────────────────────────────────────────
# Step 5 — Separate good and bad records
# Good records — timestamp extracted successfully
# Bad records  — lines that did not match pattern
#                timestamp will be empty string ""
# ─────────────────────────────────────────
good_df = df.filter(df.timestamp != "")
bad_df  = df.filter(df.timestamp == "")

print(f"Good records: {good_df.count()}")
print(f"Bad  records: {bad_df.count()}")

# ─────────────────────────────────────────
# Step 6 — Drop original raw value column
# We dont need it anymore
# All useful data is now in proper columns
# ─────────────────────────────────────────
good_df = good_df.drop("value")

# ─────────────────────────────────────────
# Step 7 — Write good records as Parquet to S3
# mode overwrite — replace if already exists
# ─────────────────────────────────────────
print("Writing good records to S3 processed folder...")
good_df.write.mode("overwrite").parquet(OUTPUT_PATH)
print("Good records written successfully!")

# ─────────────────────────────────────────
# Step 8 — Write bad records to separate path
# So we can investigate them later
# ─────────────────────────────────────────
if bad_df.count() > 0:
    print(f"Writing bad records to bad_records folder...")
    bad_df.write.mode("overwrite").text(BAD_RECORDS_PATH)
    print("Bad records written!")
else:
    print("No bad records found! All lines matched pattern.")

print("─" * 50)
print("Glue transformation job completed successfully!")
print("─" * 50)


