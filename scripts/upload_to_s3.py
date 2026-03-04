# ─────────────────────────────────────────
# Script  : upload_to_s3.py
# Purpose : Read log files from local folder (simulating NFS)
#           and upload them to S3 with date partitioning
# ─────────────────────────────────────────

# Import required libraries
import boto3                        # AWS SDK - to talk to S3
import os                           # To work with files and folders
from datetime import datetime, timedelta  # To get today's date and calculate time difference


# ─────────────────────────────────────────
# CONFIGURATION — change these as needed
# ─────────────────────────────────────────

# Source path — where log files are sitting (simulating NFS mount)
# In real world this would be /mnt/nfs/logs
SOURCE_PATH = 'data/sample_logs'

# S3 bucket name where we want to upload logs
BUCKET_NAME = 'nokia-log-processing-bucket-2024'

# S3 folder prefix — raw layer
S3_PREFIX = 'raw/logs'


# ─────────────────────────────────────────
# STEP 1 — Build date partition string
# This creates folder structure like year=2024/month=02/day=27
# ─────────────────────────────────────────

def get_date_partition():
    today = datetime.now()
    partition = f"year={today.strftime('%Y')}/month={today.strftime('%m')}/day={today.strftime('%d')}"
    return partition


# ─────────────────────────────────────────
# STEP 2 — Get only new files (Incremental logic)
# We dont want to re upload old files every day
# Only pick files modified in last 24 hours
# ─────────────────────────────────────────

def get_new_files(source_path):
    new_files = []

    if not os.path.exists(source_path):
        print(f"WARNING: Path {source_path} does not exist!")
        return []
    
    cutoff_time = datetime.now() - timedelta(hours=24)  # 24 hours ago

    for filename in os.listdir(source_path):
        # Only process .log files
        if filename.endswith('.log'):
            full_path = os.path.join(source_path, filename)

            # Get last modified time of file
            modified_time = datetime.fromtimestamp(os.path.getmtime(full_path))

            # Check if file was modified in last 24 hours
            if modified_time > cutoff_time:
                new_files.append(full_path)
                print(f"New file found: {filename} (modified: {modified_time})")
            else:
                print(f"Skipping old file: {filename} (modified: {modified_time})")

    return new_files


# ─────────────────────────────────────────
# STEP 3 — Upload files to S3
# ─────────────────────────────────────────

def upload_to_s3(files, bucket, prefix, date_partition):
    # Create S3 client
    s3 = boto3.client('s3')

    if not files:
        print("No new files to upload today.")
        return

    for file_path in files:
        filename = os.path.basename(file_path)  # Get just the filename from full path

        # Build S3 key with date partition
        # Example: raw/logs/year=2024/month=02/day=27/app.log
        s3_key = f"{prefix}/{date_partition}/{filename}"

        try:
            print(f"Uploading {filename} to s3://{bucket}/{s3_key} ...")
            s3.upload_file(file_path, bucket, s3_key)
            print(f"SUCCESS: {filename} uploaded successfully!")

        except Exception as e:
            print(f"FAILED: Could not upload {filename}. Error: {str(e)}")


# ─────────────────────────────────────────
# MAIN — Entry point of the script
# This is where everything gets called
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("Nokia Log Processing — Upload to S3")
    print("=" * 50)

    # Step 1 — Get date partition
    date_partition = get_date_partition()
    print(f"Date partition: {date_partition}")

    # Step 2 — Get new files from source
    print("\nChecking for new log files...")
    new_files = get_new_files(SOURCE_PATH)
    print(f"Total new files found: {len(new_files)}")

    # Step 3 — Upload to S3
    print("\nStarting upload to S3...")
    upload_to_s3(new_files, BUCKET_NAME, S3_PREFIX, date_partition)

    print("\nDone!")