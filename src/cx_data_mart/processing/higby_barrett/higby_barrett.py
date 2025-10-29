"""Script to clean Higby Barret County acreage data and upload to Google Cloud Storage."""

import os

import klib
import pandas as pd
from bayer_api.bayer_auth import get_gat_np_bq_credential
from bayer_api.bigquery import gen_bigquery_client
from bayer_api.gcs_funcs import GCSManager
from pandas_gbq import to_gbq

from cx_data_mart.constants import cx_data_mart_proj_path
from cx_data_mart.funcs import janitor_df_cleaning, set_pandas_setup

# Pandas setup
set_pandas_setup()

raw_data_path = (
    cx_data_mart_proj_path
    / "src/cx_data_mart/processing/higby_barrett/raw_data/Master All Data File October 2025 - Final.xlsx"
)

df = pd.read_excel(
    raw_data_path,
    sheet_name="All Data",
    dtype={"5-DigitFIPS ": str, "StateFIPS": str, "CountyFIPS": str},
)
print(df.head())
df = janitor_df_cleaning(
    df=df,
    truncate_limit=100,
    drop_empty_cols=False,
    drop_duplicated_cols=True,
)


col_exclude = ["lastupdate"]
df = klib.data_cleaning(
    data=df,
    col_exclude=col_exclude,
    drop_threshold_cols=1,
    drop_threshold_rows=1,
)
keep_columns = [
    "lastupdate",
    "county",
    "state",
    "5_digitfips",
    "statefips",
    "districtfips",
    "countyfips",
    "cropname",
    "cropcode",
    "type",
    "year",
    "acres",
]
df = df[keep_columns]

df["lastupdate"] = pd.to_datetime(df["lastupdate"]).dt.strftime("%Y-%m-%d")
df["fips_code"] = df["5_digitfips"].astype("string").str.zfill(5)  # fix fips_code

# Drop columns
df = df.drop(
    columns=["5_digitfips", "statefips", "districtfips", "countyfips"],
)

# Save to GCS
bucket_name = "market-insights-data"
np_project_id = "bcs-grower-analytics-warehouse"
gat_np_bq_credential = get_gat_np_bq_credential()
np_bigquery_client = gen_bigquery_client(
    project=np_project_id, credentials=gat_np_bq_credential
)
gcs = GCSManager(bucket_name=bucket_name, bigquery_client=np_bigquery_client)
local_file_path = "higby_barrett.parquet"
gcs_fpath = "higby-barrett/higby_barrett.parquet"
df.to_parquet(local_file_path, index=False)
gcs.upload_file(local_file_path=local_file_path, gcs_file_name=gcs_fpath)
os.remove(local_file_path)

# Save to bigquery table
dataset_id = "bcs-grower-analytics-warehouse.staging_test"
table_id = f"{dataset_id}.higby_barrett"
to_gbq(
    df,
    destination_table=table_id,
    project_id=np_project_id,
    if_exists="replace",  # "fail" | "replace" | "append"
    credentials=gat_np_bq_credential,
)
