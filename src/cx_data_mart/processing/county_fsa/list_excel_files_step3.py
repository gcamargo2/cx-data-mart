"""List excel files in directory."""

from collections.abc import Iterable
from pathlib import Path

import klib
import pandas as pd

from cx_data_mart.constants import cx_data_mart_proj_path
from cx_data_mart.funcs import (
    add_type_to_pd_cols,
    lowercase_str_col,
    remove_accents_and_special_chars,
    remove_whitespaces_str_col,
)

EXTENSIONS = {".xls", ".xlsx", ".xlsm", ".xlsb"}


def iter_excel_files(root: Path) -> Iterable[Path]:
    """Yield all nested Excel files under root."""
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in EXTENSIONS:
            yield path


def list_excel_files(root: Path) -> list[Path]:
    """Return a list of all nested Excel files."""
    return list(iter_excel_files(root))


def read_with_detected_header(
    path, sheet_name="county_data", header_keywords=("State Code", "County Code")
):
    # Read without headers so we can scan rows
    tmp = pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")

    header_row_idx = None
    for i, row in tmp.iterrows():
        values = row.astype(str).str.strip().tolist()
        # Check if all keywords appear in this row (case-insensitive contains)
        if all(any(k.lower() == v.lower() for v in values) for k in header_keywords):
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError(
            "Could not locate the header row. Adjust header_keywords or inspect the sheet."
        )

    # Now read again, setting the detected row as header
    df = pd.read_excel(
        path, sheet_name=sheet_name, header=header_row_idx, engine="openpyxl"
    )
    return df


if __name__ == "__main__":
    ROOT_DIR = Path(
        cx_data_mart_proj_path
        / "src/cx_data_mart/processing/county_fsa/county_fsa_downloads"
    )
    excel_files = list_excel_files(ROOT_DIR)
    dfs = []
    for excel_file in excel_files:
        print(excel_file)
        crop_year = str(excel_file).split("/")[-1].split("_")[0]
        print(crop_year)
        header = 1 if crop_year in {"2025", "2024"} else 0
        df = pd.read_excel(
            excel_file, sheet_name="county_data", header=header, engine="openpyxl"
        )
        df["crop_year"] = crop_year
        dfs.append(df)
    df = pd.concat(dfs)
    df_clean = klib.data_cleaning(
        data=df, drop_threshold_cols=1, drop_threshold_rows=1, category=False
    )
    text_cols = ["county", "crop_type"]
    df_clean = remove_whitespaces_str_col(df=df_clean, text_cols=text_cols)
    df_clean = lowercase_str_col(df=df_clean, text_cols=text_cols)
    df_clean = remove_accents_and_special_chars(df=df_clean, text_cols=text_cols)

    df_clean = klib.data_cleaning(
        data=df_clean, drop_threshold_cols=1, drop_threshold_rows=1, category=False
    )

    dtype = {
        "state_code": "Int64",
        "county_code": "Int64",
        "crop_code": "Int64",
    }
    df_clean = add_type_to_pd_cols(df=df_clean, dtype=dtype)

    df_clean.to_parquet(
        cx_data_mart_proj_path
        / "src/cx_data_mart/processing/county_fsa/county_fsa_data.parquet"
    )
