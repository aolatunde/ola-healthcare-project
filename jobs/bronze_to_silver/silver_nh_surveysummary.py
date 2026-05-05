import sys
import re
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, trim, upper, regexp_replace,
    current_timestamp, sha2, concat_ws, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


print("RUNNING SILVER NH SURVEYSUMMARY VERSION 001")

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "target_path", "control_path", "load_date"]
)

JOB_NAME = args["JOB_NAME"]
SOURCE_PATH = args["source_path"].rstrip("/")
TARGET_PATH = args["target_path"].rstrip("/")
CONTROL_PATH = args["control_path"].rstrip("/")
LOAD_DATE = args["load_date"]

pipeline_run_id = str(uuid.uuid4())

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext.getOrCreate(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(JOB_NAME, args)

control_schema = StructType([
    StructField("pipeline_run_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("source_path", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("load_date", StringType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at_utc", StringType(), True)
])


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_column_name(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def standardize_columns(df):
    for old_col in df.columns:
        df = df.withColumnRenamed(old_col, clean_column_name(old_col))
    return df


def rename_if_exists(df, old_name, new_name):
    if old_name in df.columns and old_name != new_name:
        return df.withColumnRenamed(old_name, new_name)
    return df


def clean_string(df, column_name):
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            regexp_replace(trim(col(column_name).cast("string")), r"\s+", " ")
        )
    return df


def clean_upper_string(df, column_name):
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            upper(regexp_replace(trim(col(column_name).cast("string")), r"\s+", " "))
        )
    return df


def cast_if_exists(df, column_name, target_type):
    if column_name in df.columns:
        df = df.withColumn(column_name, col(column_name).cast(target_type))
    return df


def parse_date_if_exists(df, column_name):
    if column_name in df.columns:
        df = df.withColumn(column_name, to_date(col(column_name)))
    return df


def write_control(status, records_read=0, records_written=0, error_message=""):
    record = [{
        "pipeline_run_id": pipeline_run_id,
        "table_name": "silver_nh_surveysummary",
        "source_path": SOURCE_PATH,
        "target_path": TARGET_PATH,
        "load_date": LOAD_DATE,
        "records_read": int(records_read),
        "records_written": int(records_written),
        "status": status,
        "error_message": error_message,
        "created_at_utc": utc_now_iso()
    }]

    df = spark.createDataFrame(record, schema=control_schema)

    (
        df.write
        .format("delta")
        .mode("append")
        .save(CONTROL_PATH)
    )


try:
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", "\"")
        .csv(SOURCE_PATH)
    )

    records_read = df_raw.count()
    print(f"Records read: {records_read}")

    df = standardize_columns(df_raw)

    rename_map = {
        "cms_certification_number_ccn": "provider_id",
        "provider_name": "provider_name",
        "provider_address": "provider_address",
        "city_town": "city",
        "state": "state",
        "zip_code": "zip_code",
        "inspection_cycle": "inspection_cycle",
        "health_survey_date": "health_survey_date",
        "fire_safety_survey_date": "fire_safety_survey_date",
        "location": "location",
        "processing_date": "processing_date"
    }

    for old_name, new_name in rename_map.items():
        df = rename_if_exists(df, old_name, new_name)

    for c in [
        "provider_id",
        "provider_name",
        "provider_address",
        "city",
        "zip_code",
        "location"
    ]:
        df = clean_string(df, c)

    df = clean_upper_string(df, "state")

    for c in [
        "health_survey_date",
        "fire_safety_survey_date",
        "processing_date"
    ]:
        df = parse_date_if_exists(df, c)

    df = cast_if_exists(df, "inspection_cycle", "int")

    for c in df.columns:
        if (
            c.startswith("total_number_of_")
            or c.startswith("count_of_")
        ):
            df = cast_if_exists(df, c, "int")

    if "provider_id" in df.columns:
        df = df.filter(col("provider_id").isNotNull())

    dedupe_cols = [
        c for c in ["provider_id", "inspection_cycle", "processing_date"]
        if c in df.columns
    ]

    if dedupe_cols:
        df = df.dropDuplicates(dedupe_cols)
    else:
        df = df.dropDuplicates()

    source_cols = df.columns

    df_silver = (
        df
        .withColumn("silver_load_date", lit(LOAD_DATE))
        .withColumn("silver_pipeline_run_id", lit(pipeline_run_id))
        .withColumn("silver_ingested_at_utc", current_timestamp())
        .withColumn(
            "silver_record_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in source_cols]), 256)
        )
    )

    records_written = df_silver.count()

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(TARGET_PATH)
    )

    write_control("SUCCESS", records_read, records_written)

    print("Silver NH survey summary completed")
    print(f"Records written: {records_written}")

except Exception as e:
    error_message = str(e)
    print(f"FAILED: {error_message}")

    write_control("FAILED", error_message=error_message)

    raise e


job.commit()