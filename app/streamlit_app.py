import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable
import os
from dotenv import load_dotenv
from pathlib import Path


st.set_page_config(
    page_title="Healthcare Staffing & Quality Dashboard",
    layout="wide"
)

st.title("Healthcare Staffing & Quality Dashboard")

load_dotenv(Path(__file__).with_name(".env"))


def get_secret(name):
    try:
        return st.secrets[name]
    except Exception:
        return None


def get_config(name):
    return os.getenv(name) or get_secret(name)


def has_columns(df, columns):
    return all(column in df.columns for column in columns)


def format_mean(df, column, fmt):
    if column not in df.columns:
        return "N/A"

    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return "N/A"

    return format(values.mean(), fmt)


STAFFING_PATH = get_config("STAFFING_PATH")
QUALITY_PATH = get_config("QUALITY_PATH")
CORR_PATH = get_config("CORR_PATH")

AWS_ACCESS_KEY_ID = get_config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = get_config("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = get_config("AWS_SESSION_TOKEN")
AWS_REGION = get_config("AWS_REGION") or get_config("AWS_DEFAULT_REGION") or "us-east-1"

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_REGION"] = AWS_REGION
os.environ["AWS_DEFAULT_REGION"] = AWS_REGION

if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

if AWS_SESSION_TOKEN:
    os.environ["AWS_SESSION_TOKEN"] = AWS_SESSION_TOKEN


def build_storage_options():
    options = {
        "AWS_REGION": AWS_REGION,
        "AWS_DEFAULT_REGION": AWS_REGION,
        "AWS_EC2_METADATA_DISABLED": "true"
    }

    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        options.update({
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        })

    if AWS_SESSION_TOKEN:
        options.update({
            "AWS_SESSION_TOKEN": AWS_SESSION_TOKEN,
        })

    return options


missing_paths = [
    name
    for name, value in {
        "STAFFING_PATH": STAFFING_PATH,
        "QUALITY_PATH": QUALITY_PATH,
        "CORR_PATH": CORR_PATH,
    }.items()
    if not value
]

if missing_paths:
    st.error("Missing required data path configuration.")
    st.write(
        "Add these values to Streamlit secrets or environment variables before "
        "deploying the app."
    )
    st.code(
        "\n".join(
            [
                "STAFFING_PATH = \"s3://your-bucket/gold/staffing_daily/\"",
                "QUALITY_PATH = \"s3://your-bucket/gold/quality_summary/\"",
                "CORR_PATH = \"s3://your-bucket/gold/staffing_quality_correlation/\"",
                "AWS_REGION = \"us-east-1\"",
            ]
        ),
        language="toml",
    )
    st.stop()

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"


@st.cache_data(show_spinner="Loading Delta table...")
def load_delta(path, storage_options=None):
    if not path:
        st.error("Missing Delta table path. Check Streamlit secrets or .env configuration.")
        st.stop()

    try:
        if path.startswith("s3://") and storage_options:
            return DeltaTable(path, storage_options=storage_options).to_pandas()

        return DeltaTable(path).to_pandas()

    except Exception as e:
        st.error(f"Failed to load Delta table from: {path}")
        st.exception(e)
        st.stop()


def beautify_columns(df):
    rename_map = {
        "work_date": "Work date",
        "year_month": "Year month",
        "provider_name": "Provider name",
        "state": "State",
        "city": "City",

        "owner_type": "Owner type",
        "primary_owner_name": "Primary owner name",
        "staffing_risk_level": "Staffing risk level",
        "occupancy_risk_level": "Occupancy risk level",

        "staffing_hprd": "Staffing HPRD",
        "avg_staffing_hprd": "Avg Staffing HPRD",
        "occupancy_rate": "Occupancy rate",
        "avg_occupancy_rate": "Avg Occupancy rate",
        "rn_ratio": "RN ratio",
        "lpn_ratio": "LPN ratio",
        "cna_ratio": "CNA ratio",

        "total_nurse_staffing_hours": "Total nursing hours",
        "contract_staff_ratio": "Contract staff ratio",
        "understaffing_pressure_score": "Understaffing pressure score",
        "weekend_staffing_gap": "Weekend staffing gap",
        "turnover_risk_level": "Turnover risk level",

        "quality_risk_score": "Quality risk score",
        "quality_risk_level": "Quality risk level",
        "correlation_bucket": "Correlation category",
    }
    return df.rename(columns=rename_map)


def to_numeric_if_exists(df, col_name):
    if col_name in df.columns:
        df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    return df


storage_options = build_storage_options()

try:
    staffing_df = beautify_columns(load_delta(STAFFING_PATH, storage_options))
    quality_df = beautify_columns(load_delta(QUALITY_PATH, storage_options))
    corr_df = beautify_columns(load_delta(CORR_PATH, storage_options))
except Exception as exc:
    st.error("Unable to load one or more Delta tables.")
    st.write(
        "Check that the configured paths exist and that the deployed app has "
        "permission to read from the storage location."
    )
    st.exception(exc)
    st.stop()


if "Work date" in staffing_df.columns:
    staffing_df["Work date"] = pd.to_datetime(staffing_df["Work date"], errors="coerce")

for c in [
    "Staffing HPRD",
    "Occupancy rate",
    "RN ratio",
    "LPN ratio",
    "CNA ratio",
    "Total nursing hours",
    "Contract staff ratio",
    "Understaffing pressure score",
    "Weekend staffing gap",
]:
    staffing_df = to_numeric_if_exists(staffing_df, c)

for c in ["Quality risk score"]:
    quality_df = to_numeric_if_exists(quality_df, c)
    corr_df = to_numeric_if_exists(corr_df, c)

for c in ["Avg Staffing HPRD", "Avg Occupancy rate"]:
    corr_df = to_numeric_if_exists(corr_df, c)

if "Avg Occupancy rate" in corr_df.columns:
    corr_df["Avg Occupancy rate"] = corr_df["Avg Occupancy rate"].fillna(0.01)


st.sidebar.header("Filters")

#State filter
states = sorted(staffing_df["State"].dropna().unique()) if "State" in staffing_df.columns else []
selected_states = st.sidebar.multiselect("Select State", states)

if selected_states:
    staffing_df = staffing_df[staffing_df["State"].isin(selected_states)]
    if "State" in quality_df.columns:
        quality_df = quality_df[quality_df["State"].isin(selected_states)]
    if "State" in corr_df.columns:
        corr_df = corr_df[corr_df["State"].isin(selected_states)]

# Date filter
if "Work date" in staffing_df.columns:
    valid_dates = staffing_df["Work date"].dropna()

    if not valid_dates.empty:
        min_date = valid_dates.min().date()
        max_date = valid_dates.max().date()

        selected_date_range = st.sidebar.date_input(
            "Select Work Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
            start_date, end_date = selected_date_range

            staffing_df = staffing_df[
                (staffing_df["Work date"].dt.date >= start_date) &
                (staffing_df["Work date"].dt.date <= end_date)
            ]


st.subheader("Executive Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Avg Staffing HPRD", format_mean(staffing_df, "Staffing HPRD", ".2f"))
col2.metric("Avg Occupancy Rate", format_mean(staffing_df, "Occupancy rate", ".1%"))
col3.metric("Avg RN Ratio", format_mean(staffing_df, "RN ratio", ".1%"))

high_risk = (
    quality_df[quality_df["Quality risk level"] == "HIGH_RISK"].shape[0]
    if "Quality risk level" in quality_df.columns
    else 0
)
col4.metric("High Risk Facilities", high_risk)


st.subheader("Staffing Overview")

if has_columns(staffing_df, ["Work date", "Staffing HPRD"]):
    trend_df = staffing_df.dropna(subset=["Work date", "Staffing HPRD"])
else:
    trend_df = pd.DataFrame()

if trend_df.empty:
    st.warning("No valid staffing trend data available.")
else:
    trend = (
        trend_df
        .groupby("Work date", as_index=False)
        .agg({"Staffing HPRD": "mean"})
        .rename(columns={"Staffing HPRD": "Avg Staffing HPRD"})
        .sort_values("Work date")
    )

    fig = px.line(
        trend,
        x="Work date",
        y="Avg Staffing HPRD",
        markers=True,
        title="Average Staffing Hours per Resident per Day",
        labels={
            "Work date": "Work Date",
            "Avg Staffing HPRD": "Hours per Resident Day (HPRD)"
        }
    )
    st.plotly_chart(fig, use_container_width=True)


st.subheader("Facility Comparison")

col1, col2 = st.columns(2)

if has_columns(staffing_df, ["Provider name", "State", "Staffing HPRD"]):
    top_df = (
        staffing_df
        .groupby(["Provider name", "State"], as_index=False)
        .agg({"Staffing HPRD": "mean"})
        .sort_values("Staffing HPRD", ascending=False)
        .head(10)
    )

    fig = px.bar(
        top_df,
        x="Provider name",
        y="Staffing HPRD",
        color="State",
        title="Top 10 Facilities by Staffing",
        labels={"Provider name": "Provider Name", "Staffing HPRD": "Staffing HPRD"}
    )
    col1.plotly_chart(fig, use_container_width=True)

    low_df = (
        staffing_df
        .groupby(["Provider name", "State"], as_index=False)
        .agg({"Staffing HPRD": "mean"})
        .sort_values("Staffing HPRD", ascending=True)
        .head(10)
    )

    fig = px.bar(
        low_df,
        x="Provider name",
        y="Staffing HPRD",
        color="State",
        title="Lowest 10 Facilities by Staffing",
        labels={"Provider name": "Provider Name", "Staffing HPRD": "Staffing HPRD"}
    )
    col2.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Facility comparison data is not available.")


st.subheader("Nurse Mix")

if has_columns(staffing_df, ["State", "RN ratio", "LPN ratio", "CNA ratio"]):
    mix_df = (
        staffing_df
        .groupby("State", as_index=False)
        .agg({
            "RN ratio": "mean",
            "LPN ratio": "mean",
            "CNA ratio": "mean"
        })
    )

    fig = px.bar(
        mix_df,
        x="State",
        y=["RN ratio", "LPN ratio", "CNA ratio"],
        barmode="stack",
        title="Nurse Mix by State",
        labels={
            "value": "Average Ratio",
            "variable": "Nurse Type"
        }
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Nurse mix data is not available.")


st.subheader("Quality Overview")

col1, col2 = st.columns(2)

if "Quality risk level" in quality_df.columns:
    fig = px.pie(
        quality_df,
        names="Quality risk level",
        title="Facility Risk Distribution"
    )
    col1.plotly_chart(fig, use_container_width=True)

if has_columns(quality_df, ["Provider name", "State", "Quality risk score"]):
    top_quality_risk = (
        quality_df
        .dropna(subset=["Quality risk score"])
        .sort_values("Quality risk score", ascending=False)
        .head(10)
    )

    if top_quality_risk.empty:
        col2.warning("No quality risk score data available.")
    else:
        fig = px.bar(
            top_quality_risk,
            x="Provider name",
            y="Quality risk score",
            color="State",
            title="Top 10 Facilities by Quality Risk Score",
            labels={
                "Provider name": "Provider Name",
                "Quality risk score": "Quality Risk Score"
            }
        )
        col2.plotly_chart(fig, use_container_width=True)


st.subheader("Staffing vs Quality Correlation")

if has_columns(corr_df, ["Avg Staffing HPRD", "Quality risk score"]):
    corr_plot_df = corr_df.dropna(subset=["Avg Staffing HPRD", "Quality risk score"])
else:
    corr_plot_df = pd.DataFrame()

if corr_plot_df.empty:
    st.warning("No correlation data available.")
else:
    scatter_kwargs = {
        "data_frame": corr_plot_df,
        "x": "Avg Staffing HPRD",
        "y": "Quality risk score",
        "title": "Staffing vs Quality Risk",
        "labels": {
            "Avg Staffing HPRD": "Staffing (HPRD)",
            "Quality risk score": "Quality Risk Score",
            "Correlation category": "Correlation Category"
        }
    }

    if "Correlation category" in corr_plot_df.columns:
        scatter_kwargs["color"] = "Correlation category"
    if "Avg Occupancy rate" in corr_plot_df.columns:
        scatter_kwargs["size"] = "Avg Occupancy rate"
    if has_columns(corr_plot_df, ["Provider name", "State"]):
        scatter_kwargs["hover_data"] = ["Provider name", "State"]

    fig = px.scatter(
        **scatter_kwargs
    )
    st.plotly_chart(fig, use_container_width=True)


st.subheader("Operational Metrics")

col1, col2 = st.columns(2)

if has_columns(staffing_df, ["Provider name", "State", "Contract staff ratio"]):
    top_contract = (
        staffing_df
        .dropna(subset=["Contract staff ratio"])
        .groupby(["Provider name", "State"], as_index=False)
        .agg({"Contract staff ratio": "mean"})
        .sort_values("Contract staff ratio", ascending=False)
        .head(10)
    )

    fig = px.bar(
        top_contract,
        x="Provider name",
        y="Contract staff ratio",
        color="State",
        title="Top Facilities by Contract Staff Ratio",
        labels={
            "Provider name": "Provider Name",
            "Contract staff ratio": "Contract Staff Ratio"
        }
    )
    col1.plotly_chart(fig, use_container_width=True)

if has_columns(staffing_df, ["Provider name", "State", "Understaffing pressure score"]):
    pressure_df = (
        staffing_df
        .dropna(subset=["Understaffing pressure score"])
        .groupby(["Provider name", "State"], as_index=False)
        .agg({"Understaffing pressure score": "mean"})
        .sort_values("Understaffing pressure score", ascending=False)
        .head(10)
    )

    fig = px.bar(
        pressure_df,
        x="Provider name",
        y="Understaffing pressure score",
        color="State",
        title="Facilities Under Highest Staffing Pressure",
        labels={
            "Provider name": "Provider Name",
            "Understaffing pressure score": "Pressure Score"
        }
    )
    col2.plotly_chart(fig, use_container_width=True)


st.subheader("Monthly Staffing Hours")

if "Year month" in staffing_df.columns and "Total nursing hours" in staffing_df.columns:
    monthly = (
        staffing_df
        .dropna(subset=["Year month", "Total nursing hours"])
        .groupby("Year month", as_index=False)
        .agg({"Total nursing hours": "sum"})
        .sort_values("Year month")
    )

    if monthly.empty:
        st.warning("No monthly staffing data available.")
    else:
        fig = px.line(
            monthly,
            x="Year month",
            y="Total nursing hours",
            markers=True,
            title="Total Nursing Hours per Month",
            labels={
                "Year month": "Month",
                "Total nursing hours": "Total Nursing Hours"
            }
        )
        st.plotly_chart(fig, use_container_width=True)


if "Weekend staffing gap" in staffing_df.columns:
    gap_df = staffing_df.dropna(subset=["Weekend staffing gap"])

    if not gap_df.empty:
        fig = px.histogram(
            gap_df,
            x="Weekend staffing gap",
            nbins=50,
            title="Distribution of Weekend Staffing Gap",
            labels={"Weekend staffing gap": "Weekend Staffing Gap"}
        )
        st.plotly_chart(fig, use_container_width=True)

if "Turnover risk level" in staffing_df.columns:
    fig = px.pie(
        staffing_df,
        names="Turnover risk level",
        title="Staff Turnover Risk Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)


with st.expander("Preview Data"):
    st.write("Staffing Data")
    st.dataframe(staffing_df.head(50))

    st.write("Quality Data")
    st.dataframe(quality_df.head(50))

    st.write("Correlation Data")
    st.dataframe(corr_df.head(50))
