import sys
import re
import uuid
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    upper,
    regexp_replace,
    current_timestamp,
    sha2,
    concat_ws,
    to_date
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark import SparkConf

print("RUNNING SILVER NH PROVIDERINFO VERSION 001")


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_path",
        "target_path",
        "control_path",
        "load_date"
    ]
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
        df = df.withColumnRenamed(old_name, new_name)
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
        "table_name": "silver_nh_providerinfo",
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

    # Common CMS provider info column mappings
    rename_candidates = {
        "cms_certification_number_ccn": "provider_id",
        "federal_provider_number": "provider_id",
        "provider_number": "provider_id",
        "ccn": "provider_id",
    
        "provider_name": "provider_name",
        "legal_business_name": "legal_business_name",
        "provider_address": "provider_address",
        "city_town": "city",
        "state": "state",
        "zip_code": "zip_code",
        "telephone_number": "phone_number",
    
        "provider_ssa_county_code": "ssa_county_code",
        "county_parish": "county",
        "location": "location",
        "latitude": "latitude",
        "longitude": "longitude",
    
        "ownership_type": "ownership_type",
        "provider_type": "provider_type",
        "provider_resides_in_hospital": "provider_resides_in_hospital",
        "affiliated_entity_name": "affiliated_entity_name",
        "affiliated_entity_id": "affiliated_entity_id",
        "continuing_care_retirement_community": "ccrc_flag",

        "date_first_approved_to_provide_medicare_and_medicaid_services": "medicare_start_date",
        "processing_date": "processing_date",
    
        "number_of_certified_beds": "number_of_certified_beds",
        "average_number_of_residents_per_day": "avg_residents_per_day",
    
        "overall_rating": "overall_rating",
        "health_inspection_rating": "health_inspection_rating",
        "qm_rating": "quality_measure_rating",
        "long_stay_qm_rating": "long_stay_qm_rating",
        "short_stay_qm_rating": "short_stay_qm_rating",
        "staffing_rating": "staffing_rating",
    
        "reported_nurse_aide_staffing_hours_per_resident_per_day": "reported_cna_hprd",
        "reported_lpn_staffing_hours_per_resident_per_day": "reported_lpn_hprd",
        "reported_rn_staffing_hours_per_resident_per_day": "reported_rn_hprd",
        "reported_licensed_staffing_hours_per_resident_per_day": "reported_licensed_hprd",
        "reported_total_nurse_staffing_hours_per_resident_per_day": "reported_total_hprd",
        "total_number_of_nurse_staff_hours_per_resident_per_day_on_the_weekend": "weekend_total_hprd",
        "registered_nurse_hours_per_resident_per_day_on_the_weekend": "weekend_rn_hprd",
        "reported_physical_therapist_staffing_hours_per_resident_per_day": "reported_pt_hprd",
    
        "total_nursing_staff_turnover": "total_staff_turnover",
        "registered_nurse_turnover": "rn_turnover",
        "number_of_administrators_who_have_left_the_nursing_home": "admin_turnover",
    
        "nursing_case_mix_index": "case_mix_index",
        "nursing_case_mix_index_ratio": "case_mix_index_ratio",
        "case_mix_nurse_aide_staffing_hours_per_resident_per_day": "case_mix_cna_hprd",
        "case_mix_lpn_staffing_hours_per_resident_per_day": "case_mix_lpn_hprd",
        "case_mix_rn_staffing_hours_per_resident_per_day": "case_mix_rn_hprd",
        "case_mix_total_nurse_staffing_hours_per_resident_per_day": "case_mix_total_hprd",
        "case_mix_weekend_total_nurse_staffing_hours_per_resident_per_day": "case_mix_weekend_hprd",
    
        "adjusted_nurse_aide_staffing_hours_per_resident_per_day": "adj_cna_hprd",
        "adjusted_lpn_staffing_hours_per_resident_per_day": "adj_lpn_hprd",
        "adjusted_rn_staffing_hours_per_resident_per_day": "adj_rn_hprd",
        "adjusted_total_nurse_staffing_hours_per_resident_per_day": "adj_total_hprd",
        "adjusted_weekend_total_nurse_staffing_hours_per_resident_per_day": "adj_weekend_hprd",
    
        "rating_cycle_1_standard_survey_health_date": "cycle1_survey_date",
        "rating_cycle_1_total_number_of_health_deficiencies": "cycle1_total_deficiencies",
        "rating_cycle_1_number_of_standard_health_deficiencies": "cycle1_standard_deficiencies",
        "rating_cycle_1_number_of_complaint_health_deficiencies": "cycle1_complaint_deficiencies",
        "rating_cycle_1_health_deficiency_score": "cycle1_deficiency_score",
        "rating_cycle_1_number_of_health_revisits": "cycle1_revisits",
        "rating_cycle_1_health_revisit_score": "cycle1_revisit_score",
        "rating_cycle_1_total_health_score": "cycle1_total_score",
    
        "rating_cycle_2_standard_health_survey_date": "cycle2_survey_date",
        "rating_cycle_2_total_number_of_health_deficiencies": "cycle2_total_deficiencies",
        "rating_cycle_2_number_of_standard_health_deficiencies": "cycle2_standard_deficiencies",
        "rating_cycle_2_number_of_complaint_health_deficiencies": "cycle2_complaint_deficiencies",
        "rating_cycle_2_health_deficiency_score": "cycle2_deficiency_score",
        "rating_cycle_2_number_of_health_revisits": "cycle2_revisits",
        "rating_cycle_2_health_revisit_score": "cycle2_revisit_score",
        "rating_cycle_2_total_health_score": "cycle2_total_score",
    
        "rating_cycle_3_standard_health_survey_date": "cycle3_survey_date",
        "rating_cycle_3_total_number_of_health_deficiencies": "cycle3_total_deficiencies",
        "rating_cycle_3_number_of_standard_health_deficiencies": "cycle3_standard_deficiencies",
        "rating_cycle_3_number_of_complaint_health_deficiencies": "cycle3_complaint_deficiencies",
        "rating_cycle_3_health_deficiency_score": "cycle3_deficiency_score",
        "rating_cycle_3_number_of_health_revisits": "cycle3_revisits",
        "rating_cycle_3_health_revisit_score": "cycle3_revisit_score",
        "rating_cycle_3_total_health_score": "cycle3_total_score",
    
        "total_weighted_health_survey_score": "total_health_score",
    
        "number_of_facility_reported_incidents": "reported_incidents",
        "number_of_substantiated_complaints": "substantiated_complaints",
        "number_of_citations_from_infection_control_inspections": "infection_control_citations",
    
        "number_of_fines": "number_of_fines",
        "total_amount_of_fines_in_dollars": "total_fines_amount",
        "number_of_payment_denials": "payment_denials",
        "total_number_of_penalties": "total_penalties"
    }

    for old_name, new_name in rename_candidates.items():
        df = rename_if_exists(df, old_name, new_name)

    string_columns = [
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
        "provider_resides_in_hospital",
        "affiliated_entity_name",
        "affiliated_entity_id",
        "ccrc_flag",
        "location",
        "phone_number"
    ]

    for c in string_columns:
        df = clean_string(df, c)

    df = clean_upper_string(df, "state")

    numeric_columns = [
        "ssa_county_code",
        "number_of_certified_beds",
        "avg_residents_per_day",
        "overall_rating",
        "health_inspection_rating",
        "quality_measure_rating",
        "long_stay_qm_rating",
        "short_stay_qm_rating",
        "staffing_rating",
        "reported_cna_hprd",
        "reported_lpn_hprd",
        "reported_rn_hprd",
        "reported_licensed_hprd",
        "reported_total_hprd",
        "weekend_total_hprd",
        "weekend_rn_hprd",
        "reported_pt_hprd",
        "total_staff_turnover",
        "rn_turnover",
        "admin_turnover",
        "case_mix_index",
        "case_mix_index_ratio",
        "case_mix_cna_hprd",
        "case_mix_lpn_hprd",
        "case_mix_rn_hprd",
        "case_mix_total_hprd",
        "case_mix_weekend_hprd",
        "adj_cna_hprd",
        "adj_lpn_hprd",
        "adj_rn_hprd",
        "adj_total_hprd",
        "adj_weekend_hprd",
        "cycle1_total_deficiencies",
        "cycle1_standard_deficiencies",
        "cycle1_complaint_deficiencies",
        "cycle1_deficiency_score",
        "cycle1_revisits",
        "cycle1_revisit_score",
        "cycle1_total_score",
        "cycle2_total_deficiencies",
        "cycle2_standard_deficiencies",
        "cycle2_complaint_deficiencies",
        "cycle2_deficiency_score",
        "cycle2_revisits",
        "cycle2_revisit_score",
        "cycle2_total_score",
        "cycle3_total_deficiencies",
        "cycle3_standard_deficiencies",
        "cycle3_complaint_deficiencies",
        "cycle3_deficiency_score",
        "cycle3_revisits",
        "cycle3_revisit_score",
        "cycle3_total_score",
        "total_health_score",
        "reported_incidents",
        "substantiated_complaints",
        "infection_control_citations",
        "number_of_fines",
        "total_fines_amount",
        "payment_denials",
        "total_penalties",
        "latitude",
        "longitude"
    ]

    for c in numeric_columns:
        df = cast_if_exists(df, c, "double")
    
    date_columns = [
        "medicare_start_date",
        "cycle1_survey_date",
        "cycle2_survey_date",
        "cycle3_survey_date",
        "processing_date"
    ]
    
    for c in date_columns:
        df = parse_date_if_exists(df, c)

    if "provider_id" in df.columns:
        df = df.dropDuplicates(["provider_id"])
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

    write_control(
        status="SUCCESS",
        records_read=records_read,
        records_written=records_written
    )

    print("Silver provider info completed")
    print(f"Records written: {records_written}")

except Exception as e:
    error_message = str(e)
    print(f"FAILED: {error_message}")

    write_control(
        status="FAILED",
        error_message=error_message
    )

    raise e


job.commit()