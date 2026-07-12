import re
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG = tomllib.loads((PROJECT_ROOT / "configs" / "lakehouse.toml").read_text())

DEFAULT_WAREHOUSE_DIR = PROJECT_ROOT / CONFIG["paths"]["warehouse_dir"]
CATALOG_DB = PROJECT_ROOT / CONFIG["paths"]["catalog_db"]
CATALOG_NAME = CONFIG["catalog"]["name"]

BRONZE_TABLE_IDENTIFIER = CONFIG["tables"]["bronze"]
SILVER_TABLE_IDENTIFIER = CONFIG["tables"]["silver"]
QUARANTINE_TABLE_IDENTIFIER = CONFIG["tables"]["quarantine"]
TRANSFORM_LOG_IDENTIFIER = CONFIG["tables"]["transform_log"]

GOLD_NAMESPACE = CONFIG["tables"]["gold_namespace"]
GOLD_LOG_IDENTIFIER = CONFIG["tables"]["gold_transform_log"]
GOLD_LOG_NAMESPACE, _GOLD_LOG_NAME = GOLD_LOG_IDENTIFIER.split(".", 1)
GOLD_LOG_LOCATION = DEFAULT_WAREHOUSE_DIR / GOLD_LOG_NAMESPACE / _GOLD_LOG_NAME

SOURCE_MONTH_PATTERN = re.compile(r"_(\d{4}-\d{2})\.parquet$")

SILVER_FIELDS = (
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "trip_duration_min",
    "avg_speed_mph",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "vendor_id",
    "ratecode_id",
    "airport_fee",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "_silver_source_month_offset",
    "_silver_zero_duration",
    "_silver_zero_distance",
    "_silver_zero_fare",
    "_silver_zero_total",
    "_silver_total_less_than_fare",
    "_bronze_source_file",
)

REJECTION_FLAGS = (
    "_invalid_timestamp_order",
    "_outside_pickup_range",
    "_invalid_distance",
    "_invalid_amount",
    "_invalid_passenger_count",
)

QUARANTINE_FIELDS = (
    "pickup_datetime",
    "dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "_bronze_source_file",
    *REJECTION_FLAGS,
)

WARNING_FLAGS = (
    "_silver_zero_duration",
    "_silver_zero_distance",
    "_silver_zero_fare",
    "_silver_zero_total",
    "_silver_total_less_than_fare",
)

PAYMENT_NULL_PROFILE_FIELDS = (
    "passenger_count",
    "ratecode_id",
    "airport_fee",
    "store_and_fwd_flag",
    "congestion_surcharge",
)

DATA_VARIANTS = (
    "all",
    "exclude_zero_duration",
    "exclude_zero_distance",
    "exclude_both",
)

MONTH_BASES = {
    "pickup_month": "pickup_month",
    "source_month": "source_month",
}


def filter_for_variant(silver: pa.Table, data_variant: str) -> pa.Table:
    if data_variant == "all":
        return silver

    keep = pa.array([True] * silver.num_rows, type=pa.bool_())
    if data_variant in ("exclude_zero_duration", "exclude_both"):
        keep = pc.and_(
            keep,
            pc.invert(pc.fill_null(silver["_silver_zero_duration"], False)),
        )
    if data_variant in ("exclude_zero_distance", "exclude_both"):
        keep = pc.and_(
            keep,
            pc.invert(pc.fill_null(silver["_silver_zero_distance"], False)),
        )
    return silver.filter(keep)


def with_data_variant(table: pa.Table, data_variant: str) -> pa.Table:
    variant_column = pa.repeat(
        pa.scalar(data_variant, pa.string()),
        table.num_rows,
    )
    return table.add_column(
        0,
        pa.field("data_variant", pa.string(), nullable=False),
        variant_column,
    )


def build_all_variants(silver: pa.Table, builder: Callable[[pa.Table], pa.Table]) -> pa.Table:
    outputs = [
        with_data_variant(
            builder(filter_for_variant(silver, data_variant)),
            data_variant,
        )
        for data_variant in DATA_VARIANTS
    ]
    return pa.concat_tables(outputs)


def with_month_basis(table: pa.Table, month_basis: str) -> pa.Table:
    column = pa.repeat(
        pa.scalar(month_basis, pa.string()),
        table.num_rows,
    )

    return table.add_column(
        0,
        pa.field("month_basis", pa.string(), nullable=False),
        column,
    )


def build_all_month_bases(silver: pa.Table, builder: Callable[[pa.Table, str, str], pa.Table]) -> pa.Table:
    outputs = [
        builder(silver, month_basis, month_column)
        for month_basis, month_column in MONTH_BASES.items()
    ]
    return pa.concat_tables(outputs)


def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        CATALOG_NAME,
        uri=f"sqlite:///{CATALOG_DB.resolve()}",
        warehouse=DEFAULT_WAREHOUSE_DIR.resolve().as_uri(),
    )


def count_all(column: str) -> tuple:
    return (column, "count", pc.CountOptions(mode="all"))


def count_nulls(column: str) -> tuple:
    return (column, "count", pc.CountOptions(mode="only_null"))


def safe_flag_count(table: pa.Table, column: str) -> int:
    if table.num_rows == 0 or column not in table.column_names:
        return 0
    values = pc.fill_null(table[column], False)
    return int(pc.sum(pc.cast(values, pa.int64())).as_py() or 0)


def safe_true_count(mask: pa.Array | pa.ChunkedArray) -> int:
    return int(
        pc.sum(pc.cast(pc.fill_null(mask, False), pa.int64())).as_py() or 0
    )


def percent_of(part: float, whole: float) -> float | None:
    return (float(part) / float(whole)) * 100.0 if whole else None


def source_month_label(source_file: str | None) -> str | None:
    if source_file is None:
        return None
    match = SOURCE_MONTH_PATTERN.search(source_file)
    return match.group(1) if match else None


def get_gold_log_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("source_table", pa.string(), nullable=False),
            pa.field("target_table", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("rows_read", pa.int64(), nullable=False),
            pa.field("rows_written", pa.int64(), nullable=False),
            pa.field("snapshot_id", pa.int64(), nullable=True),
            pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("error_message", pa.string(), nullable=True),
        ]
    )


def get_daily_trip_summary_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("trip_date", pa.date32()),
            pa.field("trip_count", pa.int64()),
            pa.field("passenger_count_total", pa.int64()),
            pa.field("total_trip_distance", pa.float64()),
            pa.field("total_fare_amount", pa.float64()),
            pa.field("total_tip_amount", pa.float64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
            pa.field("avg_speed_mph", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
        ]
    )


def get_monthly_kpi_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("month_basis", pa.string(), nullable=False),
            pa.field("year_month", pa.string()),
            pa.field("trip_count", pa.int64()),
            pa.field("passenger_count_total", pa.int64()),
            pa.field("total_trip_distance", pa.float64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("total_fare_amount", pa.float64()),
            pa.field("total_tip_amount", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
            pa.field("avg_speed_mph", pa.float64()),
        ]
    )


def get_zone_monthly_schema(zone_column: str) -> pa.Schema:
    return pa.schema(
        [
            pa.field("month_basis", pa.string(), nullable=False),
            pa.field("year_month", pa.string()),
            pa.field(zone_column, pa.int32()),
            pa.field("trip_count", pa.int64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_tip_amount", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
            pa.field("avg_speed_mph", pa.float64()),
        ]
    )


def get_zone_flow_schema(extra_dimension: str | None = None) -> pa.Schema:
    fields = [
        pa.field("month_basis", pa.string(), nullable=False),
        pa.field("year_month", pa.string()),
    ]
    if extra_dimension == "payment_type":
        fields.append(pa.field("payment_type", pa.int64()))
    elif extra_dimension == "vendor_id":
        fields.append(pa.field("vendor_id", pa.int32()))
    fields.extend(
        [
            pa.field("pickup_location_id", pa.int32()),
            pa.field("dropoff_location_id", pa.int32()),
            pa.field("trip_count", pa.int64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_tip_amount", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
            pa.field("avg_speed_mph", pa.float64()),
        ]
    )
    return pa.schema(fields)


def get_payment_monthly_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("month_basis", pa.string(), nullable=False),
            pa.field("year_month", pa.string()),
            pa.field("payment_type", pa.int64()),
            pa.field("trip_count", pa.int64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("total_fare_amount", pa.float64()),
            pa.field("total_tip_amount", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_tip_amount", pa.float64()),
            pa.field("recorded_tip_to_fare_rate", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
        ]
    )


def get_vendor_monthly_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("month_basis", pa.string(), nullable=False),
            pa.field("year_month", pa.string()),
            pa.field("vendor_id", pa.int32()),
            pa.field("trip_count", pa.int64()),
            pa.field("total_revenue", pa.float64()),
            pa.field("avg_total_amount", pa.float64()),
            pa.field("avg_fare_amount", pa.float64()),
            pa.field("avg_tip_amount", pa.float64()),
            pa.field("avg_trip_distance", pa.float64()),
            pa.field("avg_trip_duration_min", pa.float64()),
            pa.field("avg_speed_mph", pa.float64()),
        ]
    )


def get_payment_null_profile_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("payment_type", pa.int64()),
            pa.field("field_name", pa.string()),
            pa.field("row_count", pa.int64()),
            pa.field("null_count", pa.int64()),
            pa.field("null_percent", pa.float64()),
            pa.field("all_profile_fields_null_count", pa.int64()),
            pa.field("all_profile_fields_null_percent", pa.float64()),
        ]
    )


def get_data_quality_summary_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("month_basis", pa.string(), nullable=False),
            pa.field("metric_date", pa.date32()),
            pa.field("source_file", pa.string()),
            pa.field("source_month", pa.string()),
            pa.field("metric_category", pa.string()),
            pa.field("metric_name", pa.string()),
            pa.field("metric_value", pa.float64()),
            pa.field("metric_percent", pa.float64()),
            pa.field("percent_denominator", pa.string()),
            pa.field("generated_at", pa.timestamp("us", tz="UTC")),
        ]
    )


def source_month_array(source_files: pa.ChunkedArray) -> pa.Array:
    values = [
        source_month_label(source_file)
        for source_file in source_files.to_pylist()
    ]
    return pa.array(values, type=pa.string())


def add_derived_columns(silver: pa.Table) -> pa.Table:
    pickup = silver["pickup_datetime"]

    silver = silver.append_column(
        pa.field("trip_date", pa.date32()),
        pc.cast(pickup, pa.date32()),
    )
    silver = silver.append_column(
        pa.field("pickup_month", pa.string()),
        pc.strftime(pickup, format="%Y-%m"),
    )
    silver = silver.append_column(
        pa.field("source_month", pa.string()),
        source_month_array(silver["_bronze_source_file"]),
    )

    speed = silver["avg_speed_mph"]
    finite_speed = pc.if_else(
        pc.and_(pc.is_valid(speed), pc.is_finite(speed)),
        speed,
        pa.scalar(None, pa.float64()),
    )
    silver = silver.append_column(
        pa.field("finite_speed_mph", pa.float64()),
        finite_speed,
    )
    return silver


def build_daily_trip_summary(silver: pa.Table) -> pa.Table:
    grouped = silver.group_by("trip_date").aggregate(
        [
            count_all("pickup_datetime"),
            ("passenger_count", "sum"),
            ("trip_distance", "sum"),
            ("fare_amount", "sum"),
            ("tip_amount", "sum"),
            ("total_amount", "sum"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
            ("finite_speed_mph", "mean"),
            ("fare_amount", "mean"),
            ("total_amount", "mean"),
        ]
    )
    result = pa.table(
        {
            "trip_date": grouped["trip_date"],
            "trip_count": grouped["pickup_datetime_count"],
            "passenger_count_total": pc.fill_null(
                grouped["passenger_count_sum"], 0
            ),
            "total_trip_distance": grouped["trip_distance_sum"],
            "total_fare_amount": grouped["fare_amount_sum"],
            "total_tip_amount": grouped["tip_amount_sum"],
            "total_revenue": grouped["total_amount_sum"],
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
            "avg_speed_mph": grouped["finite_speed_mph_mean"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_total_amount": grouped["total_amount_mean"],
        }
    )
    return result.sort_by("trip_date").cast(get_daily_trip_summary_schema())


def build_monthly_kpi(silver: pa.Table, month_basis: str, month_column: str) -> pa.Table:
    grouped = silver.group_by(month_column).aggregate(
        [
            count_all("pickup_datetime"),
            ("passenger_count", "sum"),
            ("trip_distance", "sum"),
            ("total_amount", "sum"),
            ("fare_amount", "sum"),
            ("tip_amount", "sum"),
            ("fare_amount", "mean"),
            ("total_amount", "mean"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
            ("finite_speed_mph", "mean"),
        ]
    )
    result = pa.table(
        {
            "month_basis": pa.repeat(pa.scalar(month_basis), grouped.num_rows),
            "year_month": grouped[month_column],
            "trip_count": grouped["pickup_datetime_count"],
            "passenger_count_total": pc.fill_null(grouped["passenger_count_sum"], 0),
            "total_trip_distance": grouped["trip_distance_sum"],
            "total_revenue": grouped["total_amount_sum"],
            "total_fare_amount": grouped["fare_amount_sum"],
            "total_tip_amount": grouped["tip_amount_sum"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_total_amount": grouped["total_amount_mean"],
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
            "avg_speed_mph": grouped["finite_speed_mph_mean"],
        }
    )
    return result.sort_by("year_month").cast(get_monthly_kpi_schema())


def build_zone_monthly(silver: pa.Table, month_basis: str, month_column: str, zone_column: str) -> pa.Table:
    grouped = silver.group_by([month_column, zone_column]).aggregate(
        [
            count_all("pickup_datetime"),
            ("total_amount", "sum"),
            ("total_amount", "mean"),
            ("fare_amount", "mean"),
            ("tip_amount", "mean"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
            ("finite_speed_mph", "mean"),
        ]
    )
    result = pa.table(
        {
            "month_basis": pa.repeat(pa.scalar(month_basis), grouped.num_rows),
            "year_month": grouped[month_column],
            zone_column: grouped[zone_column],
            "trip_count": grouped["pickup_datetime_count"],
            "total_revenue": grouped["total_amount_sum"],
            "avg_total_amount": grouped["total_amount_mean"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_tip_amount": grouped["tip_amount_mean"],
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
            "avg_speed_mph": grouped["finite_speed_mph_mean"],
        }
    )
    return result.sort_by(
        [("year_month", "ascending"), ("trip_count", "descending")]
    ).cast(get_zone_monthly_schema(zone_column))


def build_zone_flow(silver: pa.Table, month_basis: str, month_column: str, extra_dimension: str | None = None) -> pa.Table:
    dimensions = [month_column]
    if extra_dimension is not None:
        dimensions.append(extra_dimension)
    dimensions.extend(["pickup_location_id", "dropoff_location_id"])

    grouped = silver.group_by(dimensions).aggregate(
        [
            count_all("pickup_datetime"),
            ("total_amount", "sum"),
            ("total_amount", "mean"),
            ("fare_amount", "mean"),
            ("tip_amount", "mean"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
            ("finite_speed_mph", "mean"),
        ]
    )

    columns: dict[str, pa.Array | pa.ChunkedArray] = {
        "month_basis": pa.repeat(pa.scalar(month_basis), grouped.num_rows),
        "year_month": grouped[month_column],
    }
    if extra_dimension is not None:
        columns[extra_dimension] = grouped[extra_dimension]
    columns.update(
        {
            "pickup_location_id": grouped["pickup_location_id"],
            "dropoff_location_id": grouped["dropoff_location_id"],
            "trip_count": grouped["pickup_datetime_count"],
            "total_revenue": grouped["total_amount_sum"],
            "avg_total_amount": grouped["total_amount_mean"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_tip_amount": grouped["tip_amount_mean"],
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
            "avg_speed_mph": grouped["finite_speed_mph_mean"],
        }
    )

    sort_columns = [("year_month", "ascending")]
    if extra_dimension is not None:
        sort_columns.append((extra_dimension, "ascending"))
    sort_columns.append(("trip_count", "descending"))
    return pa.table(columns).sort_by(sort_columns).cast(
        get_zone_flow_schema(extra_dimension)
    )


def build_payment_monthly(silver: pa.Table, month_basis: str, month_column: str) -> pa.Table:
    grouped = silver.group_by([month_column, "payment_type"]).aggregate(
        [
            count_all("pickup_datetime"),
            ("total_amount", "sum"),
            ("fare_amount", "sum"),
            ("tip_amount", "sum"),
            ("total_amount", "mean"),
            ("fare_amount", "mean"),
            ("tip_amount", "mean"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
        ]
    )

    fare_sum = grouped["fare_amount_sum"]
    tip_rate = pc.if_else(
        pc.and_(pc.is_valid(fare_sum), pc.not_equal(fare_sum, 0.0)),
        pc.divide(grouped["tip_amount_sum"], fare_sum),
        pa.scalar(None, pa.float64()),
    )

    result = pa.table(
        {
            "month_basis": pa.repeat(pa.scalar(month_basis), grouped.num_rows),
            "year_month": grouped[month_column],
            "payment_type": grouped["payment_type"],
            "trip_count": grouped["pickup_datetime_count"],
            "total_revenue": grouped["total_amount_sum"],
            "total_fare_amount": grouped["fare_amount_sum"],
            "total_tip_amount": grouped["tip_amount_sum"],
            "avg_total_amount": grouped["total_amount_mean"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_tip_amount": grouped["tip_amount_mean"],
            "recorded_tip_to_fare_rate": tip_rate,
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
        }
    )
    return result.sort_by(
        [("year_month", "ascending"), ("payment_type", "ascending")]
    ).cast(get_payment_monthly_schema())


def build_vendor_monthly(silver: pa.Table, month_basis: str, month_column: str) -> pa.Table:
    grouped = silver.group_by([month_column, "vendor_id"]).aggregate(
        [
            count_all("pickup_datetime"),
            ("total_amount", "sum"),
            ("total_amount", "mean"),
            ("fare_amount", "mean"),
            ("tip_amount", "mean"),
            ("trip_distance", "mean"),
            ("trip_duration_min", "mean"),
            ("finite_speed_mph", "mean"),
        ]
    )
    result = pa.table(
        {
            "month_basis": pa.repeat(pa.scalar(month_basis), grouped.num_rows),
            "year_month": grouped[month_column],
            "vendor_id": grouped["vendor_id"],
            "trip_count": grouped["pickup_datetime_count"],
            "total_revenue": grouped["total_amount_sum"],
            "avg_total_amount": grouped["total_amount_mean"],
            "avg_fare_amount": grouped["fare_amount_mean"],
            "avg_tip_amount": grouped["tip_amount_mean"],
            "avg_trip_distance": grouped["trip_distance_mean"],
            "avg_trip_duration_min": grouped["trip_duration_min_mean"],
            "avg_speed_mph": grouped["finite_speed_mph_mean"],
        }
    )
    return result.sort_by(
        [("year_month", "ascending"), ("vendor_id", "ascending")]
    ).cast(get_vendor_monthly_schema())


def all_fields_null_count(table: pa.Table, fields: tuple[str, ...]) -> int:
    if table.num_rows == 0:
        return 0

    mask = pc.is_null(table[fields[0]])
    for field in fields[1:]:
        mask = pc.and_(mask, pc.is_null(table[field]))
    return safe_true_count(mask)


def build_payment_null_profile(silver: pa.Table) -> pa.Table:
    payment_totals = {
        row["payment_type"]: int(row["pickup_datetime_count"])
        for row in silver.group_by("payment_type")
        .aggregate([count_all("pickup_datetime")])
        .to_pylist()
    }

    grouped_nulls = silver.group_by("payment_type").aggregate(
        [count_nulls(field) for field in PAYMENT_NULL_PROFILE_FIELDS]
    )

    all_null_by_payment: dict[int | None, int] = {}
    for payment_type in payment_totals:
        if payment_type is None:
            subset = silver.filter(pc.is_null(silver["payment_type"]))
        else:
            subset = silver.filter(
                pc.equal(silver["payment_type"], payment_type)
            )
        all_null_by_payment[payment_type] = all_fields_null_count(
            subset, PAYMENT_NULL_PROFILE_FIELDS
        )

    rows: list[dict] = []
    for entry in grouped_nulls.to_pylist():
        payment_type = entry["payment_type"]
        row_count = payment_totals[payment_type]
        all_null_count = all_null_by_payment[payment_type]

        for field in PAYMENT_NULL_PROFILE_FIELDS:
            null_count = int(entry[f"{field}_count"])
            rows.append(
                {
                    "payment_type": payment_type,
                    "field_name": field,
                    "row_count": row_count,
                    "null_count": null_count,
                    "null_percent": percent_of(null_count, row_count),
                    "all_profile_fields_null_count": all_null_count,
                    "all_profile_fields_null_percent": percent_of(
                        all_null_count, row_count
                    ),
                }
            )

    return pa.Table.from_pylist(
        rows,
        schema=get_payment_null_profile_schema(),
    ).sort_by(
        [("payment_type", "ascending"), ("field_name", "ascending")]
    )


def build_data_quality_summary(silver: pa.Table, quarantine: pa.Table, bronze_rows: int, transform_log: pa.Table) -> pa.Table:
    generated_at = datetime.now(timezone.utc)
    rows: list[dict] = []

    def add(
        category: str,
        name: str,
        value: float,
        *,
        percent: float | None = None,
        denominator: str | None = None,
        source_file: str | None = None,
        month_basis: str = "overall",
        month_value: str | None = None,
    ) -> None:
        rows.append(
            {
                "month_basis": month_basis,
                "metric_date": generated_at.date(),
                "source_file": source_file,
                "source_month": month_value if month_value is not None else source_month_label(source_file),
                "metric_category": category,
                "metric_name": name,
                "metric_value": float(value),
                "metric_percent": percent,
                "percent_denominator": denominator,
                "generated_at": generated_at,
            }
        )

    silver_rows = silver.num_rows
    rejected_unique = quarantine.num_rows
    accounted_rows = silver_rows + rejected_unique

    add("pipeline", "bronze_rows", bronze_rows, percent=100.0, denominator="bronze_rows")
    add(
        "pipeline",
        "silver_rows",
        silver_rows,
        percent=percent_of(silver_rows, bronze_rows),
        denominator="bronze_rows",
    )
    add(
        "pipeline",
        "rows_rejected_unique",
        rejected_unique,
        percent=percent_of(rejected_unique, bronze_rows),
        denominator="bronze_rows",
    )
    add(
        "pipeline",
        "rows_accounted_for",
        accounted_rows,
        percent=percent_of(accounted_rows, bronze_rows),
        denominator="bronze_rows",
    )

    for entry in transform_log.to_pylist():
        source_file = entry["source_file"]
        rows_read = int(entry["rows_read"])
        rows_written = int(entry["rows_written"])
        rows_rejected = int(entry["rows_rejected"])

        add(
            "pipeline_file",
            "rows_read",
            rows_read,
            percent=100.0,
            denominator="file_rows_read",
            source_file=source_file,
        )
        add(
            "pipeline_file",
            "rows_written",
            rows_written,
            percent=percent_of(rows_written, rows_read),
            denominator="file_rows_read",
            source_file=source_file,
        )
        add(
            "pipeline_file",
            "rows_rejected",
            rows_rejected,
            percent=percent_of(rows_rejected, rows_read),
            denominator="file_rows_read",
            source_file=source_file,
        )

    rejection_names = {
        "_invalid_timestamp_order": "dropoff_before_pickup_or_invalid_timestamp",
        "_outside_pickup_range": "pickup_outside_project_range",
        "_invalid_distance": "negative_or_null_distance",
        "_invalid_amount": "negative_or_null_required_amount",
        "_invalid_passenger_count": "nonpositive_passenger_count",
    }
    for flag in REJECTION_FLAGS:
        count = safe_flag_count(quarantine, flag)
        add(
            "rejection_reason",
            rejection_names[flag],
            count,
            percent=percent_of(count, rejected_unique),
            denominator="rejected_rows",
        )

    if quarantine.num_rows:
        negative_distance = safe_true_count(
            pc.less(quarantine["trip_distance"], 0.0)
        )
        negative_fare = safe_true_count(
            pc.less(quarantine["fare_amount"], 0.0)
        )
        negative_total = safe_true_count(
            pc.less(quarantine["total_amount"], 0.0)
        )
        dropoff_before_pickup = safe_true_count(
            pc.less(
                quarantine["dropoff_datetime"],
                quarantine["pickup_datetime"],
            )
        )
    else:
        negative_distance = negative_fare = negative_total = 0
        dropoff_before_pickup = 0

    detailed_rejections = {
        "negative_trip_distance": negative_distance,
        "negative_fare_amount": negative_fare,
        "negative_total_amount": negative_total,
        "dropoff_before_pickup": dropoff_before_pickup,
    }
    for name, count in detailed_rejections.items():
        add(
            "rejection_detail",
            name,
            count,
            percent=percent_of(count, rejected_unique),
            denominator="rejected_rows",
        )

    warning_names = {
        "_silver_zero_duration": "zero_duration",
        "_silver_zero_distance": "zero_distance",
        "_silver_zero_fare": "zero_fare_amount",
        "_silver_zero_total": "zero_total_amount",
        "_silver_total_less_than_fare": "total_less_than_fare",
    }
    for flag in WARNING_FLAGS:
        count = safe_flag_count(silver, flag)
        add(
            "silver_warning",
            warning_names[flag],
            count,
            percent=percent_of(count, silver_rows),
            denominator="silver_rows",
        )

    offsets = silver.group_by("_silver_source_month_offset").aggregate(
        [count_all("pickup_datetime")]
    )
    for entry in sorted(
        offsets.to_pylist(),
        key=lambda row: row["_silver_source_month_offset"],
    ):
        offset = int(entry["_silver_source_month_offset"])
        count = int(entry["pickup_datetime_count"])
        add(
            "month_offset",
            f"offset_{offset}",
            count,
            percent=percent_of(count, silver_rows),
            denominator="silver_rows",
        )

    file_totals = {
        row["_bronze_source_file"]: int(row["pickup_datetime_count"])
        for row in silver.group_by("_bronze_source_file")
        .aggregate([count_all("pickup_datetime")])
        .to_pylist()
    }
    per_file_offsets = silver.group_by(
        ["_bronze_source_file", "_silver_source_month_offset"]
    ).aggregate([count_all("pickup_datetime")])

    for entry in sorted(
        per_file_offsets.to_pylist(),
        key=lambda row: (
            row["_bronze_source_file"],
            row["_silver_source_month_offset"],
        ),
    ):
        source_file = entry["_bronze_source_file"]
        offset = int(entry["_silver_source_month_offset"])
        count = int(entry["pickup_datetime_count"])
        add(
            "month_offset_file",
            f"offset_{offset}",
            count,
            percent=percent_of(count, file_totals[source_file]),
            denominator="silver_rows_in_source_file",
            source_file=source_file,
        )

    zero_duration_mask = pc.fill_null(
        silver["_silver_zero_duration"], False
    )
    zero_distance_mask = pc.fill_null(
        silver["_silver_zero_distance"], False
    )
    either_zero_mask = pc.or_(zero_duration_mask, zero_distance_mask)
    both_zero_mask = pc.and_(zero_duration_mask, zero_distance_mask)

    zero_counts = {
        "zero_duration": safe_true_count(zero_duration_mask),
        "zero_distance": safe_true_count(zero_distance_mask),
        "zero_duration_or_distance": safe_true_count(either_zero_mask),
        "zero_duration_and_distance": safe_true_count(both_zero_mask),
    }
    for name, count in zero_counts.items():
        add(
            "zero_filter_impact",
            name,
            count,
            percent=percent_of(count, silver_rows),
            denominator="silver_rows",
        )

    for data_variant in DATA_VARIANTS:
        remaining = filter_for_variant(silver, data_variant).num_rows
        add(
            "zero_filter_remaining",
            data_variant,
            remaining,
            percent=percent_of(remaining, silver_rows),
            denominator="silver_rows",
        )

    for month_basis, month_column in MONTH_BASES.items():
        month_values = sorted(
            value for value in set(silver[month_column].to_pylist())
            if value is not None
        )
        for year_month in month_values:
            month_data = silver.filter(
                pc.equal(silver[month_column], year_month)
            )
            month_rows = month_data.num_rows
            month_zero_duration = pc.fill_null(
                month_data["_silver_zero_duration"], False
            )
            month_zero_distance = pc.fill_null(
                month_data["_silver_zero_distance"], False
            )
            month_masks = {
                "zero_duration": month_zero_duration,
                "zero_distance": month_zero_distance,
                "zero_duration_or_distance": pc.or_(
                    month_zero_duration, month_zero_distance
                ),
                "zero_duration_and_distance": pc.and_(
                    month_zero_duration, month_zero_distance
                ),
            }
            for name, mask in month_masks.items():
                count = safe_true_count(mask)
                add(
                    "zero_filter_impact_month",
                    name,
                    count,
                    percent=percent_of(count, month_rows),
                    denominator="silver_rows_in_month",
                    month_basis=month_basis,
                    month_value=year_month,
                )

            for data_variant in DATA_VARIANTS:
                remaining = filter_for_variant(
                    month_data, data_variant
                ).num_rows
                add(
                    "zero_filter_remaining_month",
                    data_variant,
                    remaining,
                    percent=percent_of(remaining, month_rows),
                    denominator="silver_rows_in_month",
                    month_basis=month_basis,
                    month_value=year_month,
                )


    return pa.Table.from_pylist(
        rows,
        schema=get_data_quality_summary_schema(),
    )


def comparable_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [
            field.with_type(pa.string())
            if pa.types.is_large_string(field.type)
            else field
            for field in schema
        ]
    )


def ensure_gold_table(catalog: SqlCatalog, identifier: str, schema: pa.Schema):
    catalog.create_namespace_if_not_exists(GOLD_NAMESPACE)
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        table_name = identifier.split(".", 1)[1]
        location = DEFAULT_WAREHOUSE_DIR / GOLD_NAMESPACE / table_name
        table = catalog.create_table(
            identifier,
            schema=schema,
            location=location.resolve().as_uri(),
        )
        print(f"Created Iceberg table {identifier} at {location}")
        return table

    existing_schema = table.schema().as_arrow()
    if comparable_schema(existing_schema) != comparable_schema(schema):
        raise ValueError(
            f"Schema mismatch for existing table {identifier}. "
            "This project is still in development, so drop the old Gold table "
            "or apply an explicit Iceberg schema migration before rerunning."
        )
    return table


def ensure_gold_log_table(catalog: SqlCatalog):
    catalog.create_namespace_if_not_exists(GOLD_LOG_NAMESPACE)
    try:
        return catalog.load_table(GOLD_LOG_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            GOLD_LOG_IDENTIFIER,
            schema=get_gold_log_schema(),
            location=GOLD_LOG_LOCATION.resolve().as_uri(),
        )
        print(
            f"Created Iceberg table {GOLD_LOG_IDENTIFIER} "
            f"at {GOLD_LOG_LOCATION}"
        )
        return table


def append_gold_log(
    log_table,
    *,
    source_table: str,
    target_table: str,
    status: str,
    rows_read: int,
    rows_written: int,
    snapshot_id: int | None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    log_data = pa.Table.from_pylist(
        [
            {
                "source_table": source_table,
                "target_table": target_table,
                "status": status,
                "rows_read": rows_read,
                "rows_written": rows_written,
                "snapshot_id": snapshot_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "error_message": error_message,
            }
        ],
        schema=get_gold_log_schema(),
    )
    log_table.append(log_data)
    log_table.refresh()


def load_bronze_row_count(catalog: SqlCatalog) -> int:
    try:
        bronze_table = catalog.load_table(BRONZE_TABLE_IDENTIFIER)
    except NoSuchTableError:
        return 0

    snapshot = bronze_table.current_snapshot()
    if snapshot is None:
        return 0
    return int(snapshot.summary.get("total-records", 0))


def load_silver_transform_log(catalog: SqlCatalog) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("source_file", pa.string()),
            pa.field("rows_read", pa.int64()),
            pa.field("rows_rejected", pa.int64()),
            pa.field("rows_written", pa.int64()),
        ]
    )
    empty = pa.Table.from_pylist([], schema=schema)

    try:
        table = catalog.load_table(TRANSFORM_LOG_IDENTIFIER)
    except NoSuchTableError:
        return empty
    if table.current_snapshot() is None:
        return empty

    return table.scan(
        row_filter=(
            f"status == 'success' and "
            f"target_table == '{SILVER_TABLE_IDENTIFIER}'"
        ),
        selected_fields=(
            "source_file",
            "rows_read",
            "rows_rejected",
            "rows_written",
        ),
    ).to_arrow()


def load_quarantine(catalog: SqlCatalog) -> pa.Table:
    empty_schema = pa.schema(
        [
            pa.field("pickup_datetime", pa.timestamp("us")),
            pa.field("dropoff_datetime", pa.timestamp("us")),
            pa.field("trip_distance", pa.float64()),
            pa.field("fare_amount", pa.float64()),
            pa.field("total_amount", pa.float64()),
            pa.field("_bronze_source_file", pa.string()),
            *[pa.field(flag, pa.bool_()) for flag in REJECTION_FLAGS],
        ]
    )
    empty = pa.Table.from_pylist([], schema=empty_schema)

    try:
        table = catalog.load_table(QUARANTINE_TABLE_IDENTIFIER)
    except NoSuchTableError:
        return empty
    if table.current_snapshot() is None:
        return empty

    return table.scan(selected_fields=QUARANTINE_FIELDS).to_arrow()


def validate_source_balance(
    bronze_rows: int,
    silver_rows: int,
    quarantine_rows: int,
) -> None:
    accounted = silver_rows + quarantine_rows
    if bronze_rows and accounted != bronze_rows:
        print(
            "Warning: current table counts are not balanced: "
            f"bronze={bronze_rows:,}, silver={silver_rows:,}, "
            f"quarantine={quarantine_rows:,}, accounted={accounted:,}. "
            "Check whether Bronze, Silver, and quarantine were built from "
            "the same source-file set.",
            file=sys.stderr,
        )


def main() -> int:
    catalog = get_catalog()

    try:
        silver_table = catalog.load_table(SILVER_TABLE_IDENTIFIER)
    except NoSuchTableError:
        print(
            f"Silver table {SILVER_TABLE_IDENTIFIER} does not exist. "
            "Run transform_silver_with_quarantine.py first.",
            file=sys.stderr,
        )
        return 1

    if silver_table.current_snapshot() is None:
        print(
            f"Silver table {SILVER_TABLE_IDENTIFIER} has no snapshots.",
            file=sys.stderr,
        )
        return 1

    silver_data = add_derived_columns(
        silver_table.scan(selected_fields=SILVER_FIELDS).to_arrow()
    )
    quarantine_data = load_quarantine(catalog)
    bronze_rows = load_bronze_row_count(catalog)
    transform_log = load_silver_transform_log(catalog)

    validate_source_balance(
        bronze_rows,
        silver_data.num_rows,
        quarantine_data.num_rows,
    )

    print(
        f"Loaded {silver_data.num_rows:,} Silver rows, "
        f"{quarantine_data.num_rows:,} quarantined rows, "
        f"and {bronze_rows:,} Bronze rows."
    )

    log_table = ensure_gold_log_table(catalog)

    business_rows_read = silver_data.num_rows
    quality_rows_read = silver_data.num_rows + quarantine_data.num_rows

    targets: list[
        tuple[str, str, int, Callable[[], pa.Table]]
    ] = [
        (
            f"{GOLD_NAMESPACE}.daily_trip_summary",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(silver_data, build_daily_trip_summary),
        ),
        (
            f"{GOLD_NAMESPACE}.monthly_kpi",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table, build_monthly_kpi
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.pickup_zone_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table,
                    lambda data, basis, column: build_zone_monthly(
                        data, basis, column, "pickup_location_id"
                    ),
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.dropoff_zone_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table,
                    lambda data, basis, column: build_zone_monthly(
                        data, basis, column, "dropoff_location_id"
                    ),
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.zone_flow_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table, build_zone_flow
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.payment_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table, build_payment_monthly
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.payment_zone_flow_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table,
                    lambda data, basis, column: build_zone_flow(
                        data,
                        basis,
                        column,
                        extra_dimension="payment_type",
                    ),
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.vendor_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table, build_vendor_monthly
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.vendor_zone_flow_monthly",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data,
                lambda table: build_all_month_bases(
                    table,
                    lambda data, basis, column: build_zone_flow(
                        data,
                        basis,
                        column,
                        extra_dimension="vendor_id",
                    ),
                ),
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.payment_null_profile",
            SILVER_TABLE_IDENTIFIER,
            business_rows_read,
            lambda: build_all_variants(
                silver_data, build_payment_null_profile
            ),
        ),
        (
            f"{GOLD_NAMESPACE}.data_quality_summary",
            f"{SILVER_TABLE_IDENTIFIER}+{QUARANTINE_TABLE_IDENTIFIER}",
            quality_rows_read,
            lambda: build_data_quality_summary(
                silver_data,
                quarantine_data,
                bronze_rows,
                transform_log,
            ),
        ),
    ]

    built = 0
    failed = 0

    for identifier, source_table, rows_read, builder in targets:
        started_at = datetime.now(timezone.utc)
        rows_written = 0

        try:
            data = builder()
            rows_written = data.num_rows
            gold_table = ensure_gold_table(
                catalog, identifier, data.schema
            )
            gold_table.overwrite(data)
            gold_table.refresh()
            snapshot = gold_table.current_snapshot()
            snapshot_id = (
                snapshot.snapshot_id if snapshot is not None else None
            )
        except Exception as exc:
            try:
                append_gold_log(
                    log_table,
                    source_table=source_table,
                    target_table=identifier,
                    status="failed",
                    rows_read=rows_read,
                    rows_written=0,
                    snapshot_id=None,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    error_message=str(exc),
                )
            except Exception as log_exc:
                print(
                    f"Could not write failure log for {identifier}: "
                    f"{log_exc}",
                    file=sys.stderr,
                )

            failed += 1
            print(
                f"Failed to build {identifier}: {exc}",
                file=sys.stderr,
            )
            continue

        try:
            append_gold_log(
                log_table,
                source_table=source_table,
                target_table=identifier,
                status="success",
                rows_read=rows_read,
                rows_written=rows_written,
                snapshot_id=snapshot_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            failed += 1
            print(
                f"{identifier} committed, but its success log could not "
                f"be written: {exc}",
                file=sys.stderr,
            )
            continue

        built += 1
        print(
            f"build {identifier}: {rows_written:,} rows, "
            f"snapshot={snapshot_id}"
        )

    print(
        f"\nDone. {built} Gold table(s) built, {failed} failed."
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
