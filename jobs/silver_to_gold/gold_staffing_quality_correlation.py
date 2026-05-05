import sys
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    concat_ws,
    avg,
    max as spark_max,
    first,
    when,
    coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


print("RUNNING GOLD STAFFING QUALITY CORRELATION VERSION 001")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "staffing_gold_path",
        "quality_gold_path",
        "target_path",
        "control_path",
        "load_date"
    ]
)

JOB_NAME = args["JOB_NAME"]
STAFFING_GOLD_PATH = args["staffing_gold_path"].rstrip("/")
QUALITY_GOLD_PATH = args["quality_gold_path"].rstrip("/")
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
    StructField("target_path", StringType(), True),
    StructField("load_date", StringType(), True),
    StructField("records_written", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at_utc", StringType(), True)
])


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def write_control(status, records_written=0, error_message=""):
    record = [{
        "pipeline_run_id": pipeline_run_id,
        "table_name": "gold_staffing_quality_correlation",
        "target_path": TARGET_PATH,
        "load_date": LOAD_DATE,
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
    staffing = spark.read.format("delta").load(STAFFING_GOLD_PATH)
    quality = spark.read.format("delta").load(QUALITY_GOLD_PATH)

    staffing_summary = (
        staffing
        .groupBy("provider_id")
        .agg(
            first("provider_name", ignorenulls=True).alias("provider_name"),
            first("state", ignorenulls=True).alias("state"),
            first("city", ignorenulls=True).alias("city"),
            first("county", ignorenulls=True).alias("county"),
            first("ownership_type", ignorenulls=True).alias("ownership_type"),
            first("provider_type", ignorenulls=True).alias("provider_type"),
            first("number_of_certified_beds", ignorenulls=True).alias("number_of_certified_beds"),

            avg("staffing_hprd").alias("avg_staffing_hprd"),
            avg("rn_ratio").alias("avg_rn_ratio"),
            avg("lpn_ratio").alias("avg_lpn_ratio"),
            avg("cna_ratio").alias("avg_cna_ratio"),
            avg("occupancy_rate").alias("avg_occupancy_rate"),
            avg("resident_census").alias("avg_resident_census"),
            avg("total_nurse_staffing_hours").alias("avg_total_nurse_staffing_hours"),

            spark_max("staffing_risk_level").alias("latest_staffing_risk_level"),
            spark_max("occupancy_risk_level").alias("latest_occupancy_risk_level")
        )
    )

    quality_cols = [
        c for c in [
            "provider_id",
            "overall_rating",
            "health_inspection_rating",
            "quality_measure_rating",
            "staffing_rating",
            "combined_cycle_deficiencies",
            "latest_total_deficiencies",
            "total_health_citations",
            "standard_deficiency_count",
            "complaint_deficiency_count",
            "infection_control_citation_count",
            "substantiated_complaints",
            "infection_control_citations",
            "total_penalties",
            "quality_risk_score",
            "quality_risk_level",
            "rating_risk_level",
            "inspection_risk_level"
        ]
        if c in quality.columns
    ]

    quality_summary = quality.select(*quality_cols).dropDuplicates(["provider_id"])

    gold = staffing_summary.join(quality_summary, on="provider_id", how="left")

    gold = gold.withColumn(
        "correlation_bucket",
        when(
            (col("avg_staffing_hprd") < 3.5) & (col("quality_risk_score") >= 50),
            "LOW_STAFFING_HIGH_QUALITY_RISK"
        )
        .when(
            (col("avg_staffing_hprd") < 3.5) & (col("quality_risk_score") < 50),
            "LOW_STAFFING_LOWER_QUALITY_RISK"
        )
        .when(
            (col("avg_staffing_hprd") >= 4.0) & (col("quality_risk_score") >= 50),
            "HIGH_STAFFING_HIGH_QUALITY_RISK"
        )
        .when(
            (col("avg_staffing_hprd") >= 4.0) & (col("quality_risk_score") < 20),
            "HIGH_STAFFING_LOW_QUALITY_RISK"
        )
        .otherwise("NORMAL")
    )

    gold = gold.withColumn(
        "staffing_quality_alert",
        when(
            (col("avg_staffing_hprd") < 3.5) & (col("avg_occupancy_rate") >= 0.9),
            "UNDERSTAFFED_HIGH_OCCUPANCY"
        )
        .when(
            (col("avg_staffing_hprd") < 3.5),
            "UNDERSTAFFED"
        )
        .when(
            (col("quality_risk_score") >= 50),
            "QUALITY_RISK"
        )
        .otherwise("NORMAL")
    )

    gold = gold.withColumn(
        "staffing_quality_score",
        (
            coalesce(col("avg_staffing_hprd"), lit(0.0)) * lit(10.0)
        ) - coalesce(col("quality_risk_score"), lit(0.0))
    )

    source_cols = gold.columns

    gold = (
        gold
        .withColumn("gold_load_date", lit(LOAD_DATE))
        .withColumn("gold_pipeline_run_id", lit(pipeline_run_id))
        .withColumn("gold_created_at_utc", current_timestamp())
        .withColumn(
            "gold_record_hash",
            sha2(concat_ws("||", *[col(c).cast("string") for c in source_cols]), 256)
        )
    )

    records_written = gold.count()

    (
        gold.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("gold_load_date")
        .save(TARGET_PATH)
    )

    write_control("SUCCESS", records_written)

    print("Gold staffing quality correlation completed")
    print(f"Records written: {records_written}")

except Exception as e:
    error_message = str(e)
    print(f"FAILED: {error_message}")

    write_control("FAILED", 0, error_message)

    raise e


job.commit()