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
    count,
    sum as spark_sum,
    max as spark_max,
    first,
    when,
    coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, LongType


print("RUNNING GOLD QUALITY SUMMARY VERSION 001")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "provider_silver_path",
        "surveysummary_silver_path",
        "healthcitations_silver_path",
        "target_path",
        "control_path",
        "load_date"
    ]
)

JOB_NAME = args["JOB_NAME"]
PROVIDER_SILVER_PATH = args["provider_silver_path"].rstrip("/")
SURVEYSUMMARY_SILVER_PATH = args["surveysummary_silver_path"].rstrip("/")
HEALTHCITATIONS_SILVER_PATH = args["healthcitations_silver_path"].rstrip("/")
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
        "table_name": "gold_quality_summary",
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
    provider = spark.read.format("delta").load(PROVIDER_SILVER_PATH)
    surveysummary = spark.read.format("delta").load(SURVEYSUMMARY_SILVER_PATH)
    healthcitations = spark.read.format("delta").load(HEALTHCITATIONS_SILVER_PATH)

    provider_cols = [
        c for c in [
            "provider_id",
            "provider_name",
            "legal_business_name",
            "provider_address",
            "city",
            "state",
            "zip_code",
            "county",
            "ownership_type",
            "provider_type",
            "number_of_certified_beds",
            "avg_residents_per_day",
            "overall_rating",
            "health_inspection_rating",
            "quality_measure_rating",
            "staffing_rating",
            "reported_total_hprd",
            "reported_rn_hprd",
            "reported_lpn_hprd",
            "reported_cna_hprd",
            "total_staff_turnover",
            "rn_turnover",
            "cycle1_total_deficiencies",
            "cycle2_total_deficiencies",
            "cycle3_total_deficiencies",
            "total_health_score",
            "reported_incidents",
            "substantiated_complaints",
            "infection_control_citations",
            "number_of_fines",
            "total_fines_amount",
            "total_penalties",
            "latitude",
            "longitude",
            "processing_date"
        ]
        if c in provider.columns
    ]

    provider_dim = provider.select(*provider_cols).dropDuplicates(["provider_id"])

    survey_agg_exprs = []

    if "total_number_of_health_deficiencies" in surveysummary.columns:
        survey_agg_exprs.append(
            spark_max("total_number_of_health_deficiencies").alias("latest_total_health_deficiencies")
        )

    if "total_number_of_fire_safety_deficiencies" in surveysummary.columns:
        survey_agg_exprs.append(
            spark_max("total_number_of_fire_safety_deficiencies").alias("latest_total_fire_safety_deficiencies")
        )

    if "count_of_infection_control_deficiencies" in surveysummary.columns:
        survey_agg_exprs.append(
            spark_max("count_of_infection_control_deficiencies").alias("latest_infection_control_deficiencies")
        )

    if "count_of_nursing_and_physician_services_deficiencies" in surveysummary.columns:
        survey_agg_exprs.append(
            spark_max("count_of_nursing_and_physician_services_deficiencies").alias("latest_nursing_services_deficiencies")
        )

    if "count_of_resident_rights_deficiencies" in surveysummary.columns:
        survey_agg_exprs.append(
            spark_max("count_of_resident_rights_deficiencies").alias("latest_resident_rights_deficiencies")
        )

    if "inspection_cycle" in surveysummary.columns:
        survey_agg_exprs.append(spark_max("inspection_cycle").alias("latest_inspection_cycle"))

    if "health_survey_date" in surveysummary.columns:
        survey_agg_exprs.append(spark_max("health_survey_date").alias("latest_health_survey_date"))

    if "fire_safety_survey_date" in surveysummary.columns:
        survey_agg_exprs.append(spark_max("fire_safety_survey_date").alias("latest_fire_safety_survey_date"))

    if len(survey_agg_exprs) > 0:
        survey_summary = (
            surveysummary
            .groupBy("provider_id")
            .agg(*survey_agg_exprs)
        )
    else:
        survey_summary = surveysummary.select("provider_id").dropDuplicates(["provider_id"])

    citation_agg_exprs = [
        count(lit(1)).alias("total_health_citations")
    ]

    if "standard_deficiency" in healthcitations.columns:
        citation_agg_exprs.append(
            spark_sum(
                when(col("standard_deficiency").isin("Y", "YES", "TRUE", "1"), 1).otherwise(0)
            ).alias("standard_deficiency_count")
        )

    if "complaint_deficiency" in healthcitations.columns:
        citation_agg_exprs.append(
            spark_sum(
                when(col("complaint_deficiency").isin("Y", "YES", "TRUE", "1"), 1).otherwise(0)
            ).alias("complaint_deficiency_count")
        )

    if "infection_control_inspection_deficiency" in healthcitations.columns:
        citation_agg_exprs.append(
            spark_sum(
                when(col("infection_control_inspection_deficiency").isin("Y", "YES", "TRUE", "1"), 1).otherwise(0)
            ).alias("infection_control_citation_count")
        )

    if "citation_under_idr" in healthcitations.columns:
        citation_agg_exprs.append(
            spark_sum(
                when(col("citation_under_idr").isin("Y", "YES", "TRUE", "1"), 1).otherwise(0)
            ).alias("idr_citation_count")
        )

    if "citation_under_iidr" in healthcitations.columns:
        citation_agg_exprs.append(
            spark_sum(
                when(col("citation_under_iidr").isin("Y", "YES", "TRUE", "1"), 1).otherwise(0)
            ).alias("iidr_citation_count")
        )

    if "survey_date" in healthcitations.columns:
        citation_agg_exprs.append(spark_max("survey_date").alias("latest_citation_survey_date"))

    citations_summary = (
        healthcitations
        .groupBy("provider_id")
        .agg(*citation_agg_exprs)
    )

    gold = (
        provider_dim
        .join(survey_summary, on="provider_id", how="left")
        .join(citations_summary, on="provider_id", how="left")
    )

    gold = gold.withColumn(
        "combined_cycle_deficiencies",
        coalesce(col("cycle1_total_deficiencies"), lit(0.0))
        + coalesce(col("cycle2_total_deficiencies"), lit(0.0))
        + coalesce(col("cycle3_total_deficiencies"), lit(0.0))
    )

    if "latest_total_health_deficiencies" in gold.columns and "latest_total_fire_safety_deficiencies" in gold.columns:
        gold = gold.withColumn(
            "latest_total_deficiencies",
            coalesce(col("latest_total_health_deficiencies"), lit(0.0))
            + coalesce(col("latest_total_fire_safety_deficiencies"), lit(0.0))
        )
    else:
        gold = gold.withColumn("latest_total_deficiencies", lit(None).cast("double"))

    gold = gold.withColumn(
        "quality_risk_score",
        coalesce(col("combined_cycle_deficiencies"), lit(0.0))
        + coalesce(col("total_health_citations"), lit(0))
        + coalesce(col("substantiated_complaints"), lit(0.0))
        + coalesce(col("infection_control_citations"), lit(0.0))
        + coalesce(col("total_penalties"), lit(0.0))
    )

    gold = gold.withColumn(
        "quality_risk_level",
        when(col("quality_risk_score") >= 50, "HIGH_RISK")
        .when(col("quality_risk_score") >= 20, "MEDIUM_RISK")
        .otherwise("LOW_RISK")
    )

    gold = gold.withColumn(
        "rating_risk_level",
        when(col("overall_rating") <= 2, "HIGH_RISK")
        .when(col("overall_rating") == 3, "MEDIUM_RISK")
        .when(col("overall_rating") >= 4, "LOW_RISK")
        .otherwise("UNKNOWN")
    )

    gold = gold.withColumn(
        "inspection_risk_level",
        when(col("latest_total_deficiencies") >= 20, "HIGH_RISK")
        .when(col("latest_total_deficiencies") >= 10, "MEDIUM_RISK")
        .otherwise("LOW_RISK")
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

    print("Gold quality summary completed")
    print(f"Records written: {records_written}")

except Exception as e:
    error_message = str(e)
    print(f"FAILED: {error_message}")

    write_control("FAILED", 0, error_message)

    raise e


job.commit()