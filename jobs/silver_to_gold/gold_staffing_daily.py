import sys
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws,
    avg, max as spark_max, first, when
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


print("RUNNING GOLD STAFFING DAILY VERSION 002")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "staffing_silver_path",
        "provider_silver_path",
        "ownership_silver_path",
        "target_path",
        "control_path",
        "load_date"
    ]
)

JOB_NAME = args["JOB_NAME"]
STAFFING_SILVER_PATH = args["staffing_silver_path"].rstrip("/")
PROVIDER_SILVER_PATH = args["provider_silver_path"].rstrip("/")
OWNERSHIP_SILVER_PATH = args["ownership_silver_path"].rstrip("/")
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
        "table_name": "gold_staffing_daily",
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
    staffing = spark.read.format("delta").load(STAFFING_SILVER_PATH)
    provider = spark.read.format("delta").load(PROVIDER_SILVER_PATH)
    ownership = spark.read.format("delta").load(OWNERSHIP_SILVER_PATH)

    staffing_daily = (
        staffing
        .select(
            "provider_id",
            "provider_name",
            "city",
            "state",
            "county",
            "calendar_quarter",
            "work_date",
            "resident_census",
            "total_rn_hours",
            "total_lpn_hours",
            "total_cna_hours",
            "total_nurse_staffing_hours",
            "staffing_hprd"
        )
        .dropDuplicates(["provider_id", "work_date"])
    )

    provider_cols = [
        c for c in [
            "provider_id",
            "legal_business_name",
            "provider_address",
            "zip_code",
            "phone_number",
            "ssa_county_code",
            "ownership_type",
            "provider_type",
            "provider_resides_in_hospital",
            "number_of_certified_beds",
            "avg_residents_per_day",
            "overall_rating",
            "health_inspection_rating",
            "quality_measure_rating",
            "staffing_rating",
            "reported_cna_hprd",
            "reported_lpn_hprd",
            "reported_rn_hprd",
            "reported_total_hprd",
            "weekend_total_hprd",
            "weekend_rn_hprd",
            "total_staff_turnover",
            "rn_turnover",
            "case_mix_index",
            "case_mix_total_hprd",
            "adj_total_hprd",
            "cycle1_total_deficiencies",
            "cycle2_total_deficiencies",
            "cycle3_total_deficiencies",
            "total_health_score",
            "reported_incidents",
            "substantiated_complaints",
            "infection_control_citations",
            "number_of_fines",
            "total_fines_amount",
            "payment_denials",
            "total_penalties",
            "latitude",
            "longitude",
            "processing_date"
        ]
        if c in provider.columns
    ]

    provider_dim = provider.select(*provider_cols).dropDuplicates(["provider_id"])

    ownership_dim = (
        ownership
        .groupBy("provider_id")
        .agg(
            first("owner_type", ignorenulls=True).alias("owner_type"),
            first("owner_name", ignorenulls=True).alias("primary_owner_name"),
            first("owner_role", ignorenulls=True).alias("owner_role"),
            spark_max("ownership_percentage").alias("max_ownership_percentage")
        )
    )

    gold = (
        staffing_daily
        .join(provider_dim, on="provider_id", how="left")
        .join(ownership_dim, on="provider_id", how="left")
    )

    gold = gold.withColumn(
        "occupancy_rate",
        when(
            col("number_of_certified_beds") > 0,
            col("resident_census") / col("number_of_certified_beds")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "staffing_per_bed",
        when(
            col("number_of_certified_beds") > 0,
            col("total_nurse_staffing_hours") / col("number_of_certified_beds")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "rn_ratio",
        when(
            col("total_nurse_staffing_hours") > 0,
            col("total_rn_hours") / col("total_nurse_staffing_hours")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "lpn_ratio",
        when(
            col("total_nurse_staffing_hours") > 0,
            col("total_lpn_hours") / col("total_nurse_staffing_hours")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "cna_ratio",
        when(
            col("total_nurse_staffing_hours") > 0,
            col("total_cna_hours") / col("total_nurse_staffing_hours")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "staffing_vs_reported_hprd_delta",
        when(
            col("reported_total_hprd").isNotNull(),
            col("staffing_hprd") - col("reported_total_hprd")
        ).otherwise(None)
    )

    gold = gold.withColumn(
        "staffing_risk_level",
        when(col("staffing_hprd") < 3.5, "HIGH_RISK")
        .when(col("staffing_hprd") < 4.0, "MEDIUM_RISK")
        .otherwise("LOW_RISK")
    )

    gold = gold.withColumn(
        "occupancy_risk_level",
        when(col("occupancy_rate") >= 0.95, "HIGH_OCCUPANCY")
        .when(col("occupancy_rate") >= 0.85, "MEDIUM_OCCUPANCY")
        .otherwise("NORMAL_OCCUPANCY")
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

    print("Gold staffing daily completed")
    print(f"Records written: {records_written}")

except Exception as e:
    error_message = str(e)
    print(f"FAILED: {error_message}")

    write_control("FAILED", 0, error_message)

    raise e


job.commit()