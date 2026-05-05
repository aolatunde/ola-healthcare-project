import sys
import json
import io
from datetime import datetime, timezone

import boto3

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType


print("RUNNING GOOGLE DRIVE MULTI-FOLDER TO S3 BRONZE WITH DELTA TRACKING VERSION 001")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "folder_ids",
        "s3_bucket",
        "s3_prefix",
        "creds_bucket",
        "creds_key",
        "tracking_table_path"
    ]
)

JOB_NAME = args["JOB_NAME"]
FOLDER_IDS = [x.strip() for x in args["folder_ids"].split(",") if x.strip()]
S3_BUCKET = args["s3_bucket"]
S3_PREFIX = args["s3_prefix"].strip("/")
CREDS_BUCKET = args["creds_bucket"]
CREDS_KEY = args["creds_key"]
TRACKING_TABLE_PATH = args["tracking_table_path"].rstrip("/")

s3 = boto3.client("s3")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(JOB_NAME, args)


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def current_load_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def normalize_dataset_name(file_name):
    return (
        file_name
        .rsplit(".", 1)[0]
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


def load_credentials():
    obj = s3.get_object(Bucket=CREDS_BUCKET, Key=CREDS_KEY)
    creds_json = json.loads(obj["Body"].read().decode("utf-8"))

    return service_account.Credentials.from_service_account_info(
        creds_json,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )


def get_drive_service():
    credentials = load_credentials()
    return build("drive", "v3", credentials=credentials)


def list_files(service, folder_id):
    files = []
    page_token = None

    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="nextPageToken, files(id, name, modifiedTime)",
            pageToken=page_token
        ).execute()

        for file in response.get("files", []):
            file["folder_id"] = folder_id
            files.append(file)

        page_token = response.get("nextPageToken")

        if not page_token:
            break

    return files


def delta_tracking_exists():
    try:
        spark.read.format("delta").load(TRACKING_TABLE_PATH).limit(1).count()
        return True
    except Exception:
        return False


def get_tracking_record(file_id):
    if not delta_tracking_exists():
        return None

    df = (
        spark.read
        .format("delta")
        .load(TRACKING_TABLE_PATH)
        .filter(col("file_id") == file_id)
        .orderBy(col("tracking_updated_at").desc())
        .limit(1)
    )

    rows = df.collect()
    return rows[0].asDict() if rows else None


def should_process(file):
    record = get_tracking_record(file["id"])

    if not record:
        return True

    if record.get("modified_time") != file["modifiedTime"]:
        return True

    if record.get("raw_status") != "completed":
        return True

    return False


def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()

    downloader = MediaIoBaseDownload(file_stream, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"Download {int(status.progress() * 100)}%")

    file_stream.seek(0)
    return file_stream


def upload_to_s3(file_stream, file):
    load_date = current_load_date()
    dataset_name = normalize_dataset_name(file["name"])
    cleaned_file_name = f"{dataset_name}.csv"

    s3_key = (
        f"{S3_PREFIX}/"
        f"{dataset_name}/"
        f"load_date={load_date}/"
        f"{cleaned_file_name}"
    )

    s3.upload_fileobj(file_stream, S3_BUCKET, s3_key)

    return s3_key, dataset_name, load_date

tracking_schema = StructType([
    StructField("file_id", StringType(), True),
    StructField("folder_id", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("dataset_name", StringType(), True),
    StructField("modified_time", StringType(), True),
    StructField("raw_s3_bucket", StringType(), True),
    StructField("raw_s3_key", StringType(), True),
    StructField("raw_status", StringType(), True),
    StructField("raw_error_message", StringType(), True),
    StructField("records_read", LongType(), True),
    StructField("tracking_updated_at", StringType(), True)
])

def write_tracking_record(
    file,
    dataset_name,
    status,
    s3_key=None,
    error_message=None,
    records_read=None
):
    tracking_record = [
        {
            "file_id": str(file["id"]),
            "folder_id": str(file.get("folder_id", "")),
            "file_name": str(file["name"]),
            "dataset_name": str(dataset_name),
            "modified_time": str(file["modifiedTime"]),
            "raw_s3_bucket": str(S3_BUCKET),
            "raw_s3_key": s3_key if s3_key is not None else "",
            "raw_status": str(status),
            "raw_error_message": error_message if error_message is not None else "",
            "records_read": int(records_read) if records_read is not None else 0,
            "tracking_updated_at": utc_now_iso()
        }
    ]

    tracking_df = spark.createDataFrame(tracking_record, schema=tracking_schema)

    (
        tracking_df.write
        .format("delta")
        .mode("append")
        .save(TRACKING_TABLE_PATH)
    )


def main():
    service = get_drive_service()

    all_files = []

    for folder_id in FOLDER_IDS:
        print(f"Listing CSV files in Google Drive folder: {folder_id}")
        folder_files = list_files(service, folder_id)
        print(f"Found {len(folder_files)} CSV files in folder {folder_id}")
        all_files.extend(folder_files)

    print(f"Total CSV files found across all folders: {len(all_files)}")

    processed_count = 0
    skipped_count = 0
    failed_count = 0

    for file in all_files:
        file_name = file["name"]
        dataset_name = normalize_dataset_name(file_name)

        if not file_name.lower().endswith(".csv"):
            print(f"Skipping non-CSV file: {file_name}")
            skipped_count += 1
            continue

        if not should_process(file):
            print(f"Skipping already processed file: {file_name}")
            skipped_count += 1
            continue

        print(f"Processing file: {file_name}")
        print(f"Source folder: {file.get('folder_id')}")

        try:
            write_tracking_record(
                file=file,
                dataset_name=dataset_name,
                status="processing"
            )

            file_stream = download_file(service, file["id"])
            s3_key, dataset_name, load_date = upload_to_s3(file_stream, file)

            write_tracking_record(
                file=file,
                dataset_name=dataset_name,
                status="completed",
                s3_key=s3_key
            )

            print(f"Successfully uploaded {file_name} to s3://{S3_BUCKET}/{s3_key}")
            processed_count += 1

        except Exception as e:
            error_message = str(e)
            print(f"Error processing {file_name}: {error_message}")

            write_tracking_record(
                file=file,
                dataset_name=dataset_name,
                status="failed",
                error_message=error_message
            )

            failed_count += 1

    print("Job summary")
    print(f"Processed: {processed_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Failed: {failed_count}")

    job.commit()


if __name__ == "__main__":
    main()