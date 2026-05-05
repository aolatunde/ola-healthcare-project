# Healthcare Staffing and Quality Analytics Platform

This is a data engineering and analytics project for healthcare facility staffing, operational performance, and quality outcomes.

The platform ingests raw CMS datasets, transforms them with a Delta Lake architecture on AWS, orchestrates ETL with Step Functions, and presents analytics in a Streamlit dashboard.

## Architecture

External CMS datasets
        |
AWS Glue ingestion
        |
S3 data lake with Delta tables
  |-- Bronze: raw ingested data
  |-- Silver: cleaned and standardized data
  |-- Gold: analytics-ready aggregates
        |
AWS Step Functions orchestration
        |
Amazon EventBridge scheduling
        |
Amazon SNS notifications
        |
Streamlit dashboard

## Tech Stack

- AWS Glue and PySpark
- Amazon S3
- Delta Lake
- AWS Step Functions
- Amazon EventBridge
- Amazon SNS
- Terraform
- Streamlit, Pandas, and Plotly

## Pipeline Layers

- Bronze: raw source data stored in S3 with load metadata.
- Silver: cleaned tables with normalized columns, parsed dates, and enforced data types.
- Gold: business-level aggregates for staffing, quality, and correlation analytics.

## Dashboard Features

- State and date range filters
- KPI summary cards
- Staffing trend visualization
- Facility comparison charts
- Nurse mix breakdown
- Quality risk analysis
- Staffing and quality correlation
- Operational insights

Install dependencies and run Streamlit:
```powershell
python -m pip install -r requirements.txt
streamlit run app/streamlit_app.py
```

## Infrastructure Deployment

From the Terraform environment:

```powershell
cd infra/terraform/envs/dev
terraform init
terraform plan
terraform apply
```

Provide environment-specific values in a local `terraform.tfvars` file. Terraform state and tfvars files are ignored by Git.
