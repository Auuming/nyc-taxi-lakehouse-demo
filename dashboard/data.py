from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
CATALOG_DB = DATA_DIR / "catalog.db"
SQL_DIR = PROJECT_ROOT / "sql"

DATA_VARIANTS = (
    "all",
    "exclude_zero_duration",
    "exclude_zero_distance",
    "exclude_both",
)

VARIANT_LABELS = {
    "all": "Include all valid Silver rows",
    "exclude_zero_duration": "Exclude zero-duration trips",
    "exclude_zero_distance": "Exclude zero-distance trips",
    "exclude_both": "Exclude zero-duration or zero-distance trips",
}

MONTH_BASES = ("pickup_month", "source_month")

MONTH_BASIS_LABELS = {
    "pickup_month": "Pickup month",
    "source_month": "Raw source-file month",
}


@lru_cache(maxsize=1)
def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{CATALOG_DB.resolve()}",
        warehouse=WAREHOUSE_DIR.resolve().as_uri(),
    )


@st.cache_data(show_spinner=False)
def load_gold_table(identifier: str) -> pd.DataFrame:
    try:
        table = get_catalog().load_table(identifier)
    except NoSuchTableError as exc:
        raise RuntimeError(
            f"Gold table {identifier} does not exist. "
            "Run `python pipelines/build_gold.py` first."
        ) from exc

    if table.current_snapshot() is None:
        return pd.DataFrame()

    return table.scan().to_arrow().to_pandas()


def clear_dashboard_cache() -> None:
    load_gold_table.clear()


def read_sql(filename: str) -> str:
    path = SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def query_dataframe(
    table_name: str,
    dataframe: pd.DataFrame,
    sql_filename: str,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    try:
        con.register(table_name, dataframe)
        return con.execute(read_sql(sql_filename), parameters).df()
    finally:
        con.close()


def variant_selector(*, key: str, sidebar: bool = True) -> str:
    target = st.sidebar if sidebar else st
    return target.selectbox(
        "Data variant",
        options=list(DATA_VARIANTS),
        format_func=lambda value: VARIANT_LABELS[value],
        key=key,
        help=(
            "All variants come from the same canonical Silver table. "
            "They differ only in zero-duration and zero-distance filtering."
        ),
    )


def month_basis_selector(*, key: str, sidebar: bool = True) -> str:
    target = st.sidebar if sidebar else st
    return target.selectbox(
        "Month basis",
        options=list(MONTH_BASES),
        format_func=lambda value: MONTH_BASIS_LABELS[value],
        key=key,
        help=(
            "Pickup month uses pickup_datetime. Raw source-file month uses "
            "the YYYY-MM in the original Parquet filename."
        ),
    )


def require_nonempty(df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        st.warning(f"`{table_name}` is empty. Rebuild the Gold layer.")
        st.stop()


def filter_variant_and_month_basis(
    df: pd.DataFrame,
    *,
    data_variant: str,
    month_basis: str | None = None,
) -> pd.DataFrame:
    result = df[df["data_variant"] == data_variant].copy()
    if month_basis is not None and "month_basis" in result.columns:
        result = result[result["month_basis"] == month_basis].copy()
    return result


def as_category_strings(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column in result.columns:
            numeric = pd.to_numeric(result[column], errors="coerce").astype("Int64")
            result[column] = numeric.astype(str).replace("<NA>", "Unknown")
    return result
