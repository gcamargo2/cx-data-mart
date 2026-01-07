"""Workflow to get latest FSA county data."""

import pandas as pd
from bayer_api.bayer_auth import get_gat_np_bq_credential, get_gat_prod_bq_credential
from bayer_api.bigquery import gen_bigquery_client
from bayer_api.gcs_funcs import GCSManager
from pandas_gbq import to_gbq

from cx_data_mart.constants import cx_data_mart_proj_path
from cx_data_mart.funcs import (
    lowercase_str_col,
    remove_accents_and_special_chars,
    set_pandas_setup,
)

set_pandas_setup()

local_file_path = (
    cx_data_mart_proj_path
    / "src/cx_data_mart/processing/county_fsa/county_fsa_data.parquet"
)
df = pd.read_parquet(local_file_path)
df["fips_code"] = df["fips_code"].str.split(".", expand=True)[0]
df["crop_code"] = df["crop_code"].str.split(".", expand=True)[0]

text_cols = ["crop"]
df = lowercase_str_col(df=df, text_cols=text_cols)
df = remove_accents_and_special_chars(df=df, text_cols=text_cols)

# Save to GCS
bucket_name = "market-insights-data"
np_project_id = "bcs-grower-analytics-warehouse"
gat_np_bq_credential = get_gat_np_bq_credential()
np_bigquery_client = gen_bigquery_client(
    project=np_project_id, credentials=gat_np_bq_credential
)
gcs = GCSManager(bucket_name=bucket_name, bigquery_client=np_bigquery_client)
gcs_fpath = "county_fsa_data/county_fsa_data.parquet"
gcs.upload_file(local_file_path=local_file_path, gcs_file_name=gcs_fpath)

# Save to BQ
dataset_id = "bcs-grower-analytics-warehouse.imported_files"
table_id = f"{dataset_id}.county_fsa_data"
to_gbq(
    df,
    destination_table=table_id,
    project_id=np_project_id,
    if_exists="replace",  # "fail" | "replace" | "append"
    credentials=gat_np_bq_credential,
)

prod_project_id = "bcs-grower-analytics-wh-prod"
dataset_id = "bcs-grower-analytics-wh-prod.imported_files"
table_id = f"{dataset_id}.county_fsa_data"
gat_prod_bq_credential = get_gat_prod_bq_credential()
to_gbq(
    df,
    destination_table=table_id,
    project_id=prod_project_id,
    if_exists="replace",  # "fail" | "replace" | "append"
    credentials=gat_prod_bq_credential,
)
