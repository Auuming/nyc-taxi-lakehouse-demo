import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.transforms import MonthTransform


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_WAREHOUSE_DIR = DATA_DIR / "warehouse"
CATALOG_DB = DATA_DIR / "catalog.db"

BRONZE_TABLE_IDENTIFIER = "bronze.yellow_trips"
BRONZE_SOURCE_FILE_COLUMN = "_bronze_source_file"

NAMESPACE = "silver"
TABLE_IDENTIFIER_YELLOW = f"{NAMESPACE}.yellow_trips"
TABLE_LOCATION_YELLOW = DEFAULT_WAREHOUSE_DIR / NAMESPACE / "yellow_trips"
QUARANTINE_NAMESPACE = "quarantine"
REJECTED_TABLE_IDENTIFIER = f"{QUARANTINE_NAMESPACE}.yellow_trips_rejected"
REJECTED_TABLE_LOCATION = DEFAULT_WAREHOUSE_DIR / QUARANTINE_NAMESPACE / "yellow_trips_rejected"
PROCESSED_AT_COLUMN = "_silver_processed_at"
REJECTED_AT_COLUMN = "_silver_rejected_at"

INGESTION_LOG_IDENTIFIER = "metadata.ingestion_log"
TRANSFORM_LOG_NAMESPACE = "metadata"
TRANSFORM_LOG_IDENTIFIER = f"{TRANSFORM_LOG_NAMESPACE}.transform_log"
TRANSFORM_LOG_LOCATION = DEFAULT_WAREHOUSE_DIR / TRANSFORM_LOG_NAMESPACE / "transform_log"

COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",
}

MIN_PICKUP_DATETIME = datetime(2026, 1, 1)
MAX_PICKUP_DATETIME_UPPER_BOUND = datetime(2026, 4, 1)
SOURCE_MONTH_PATTERN = re.compile(r"_(\d{4})-(\d{2})\.parquet$")


def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{CATALOG_DB.resolve()}",
        warehouse=DEFAULT_WAREHOUSE_DIR.resolve().as_uri(),
    )


def get_transform_log_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field(
                "source_table",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "source_file",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "target_table",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "status",
                pa.string(),
                nullable=False,
            ),
            pa.field(
                "rows_read",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "rows_rejected",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "rows_written",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "snapshot_id",
                pa.int64(),
                nullable=True,
            ),
            pa.field(
                "started_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field(
                "finished_at",
                pa.timestamp("us", tz="UTC"),
                nullable=False,
            ),
            pa.field(
                "error_message",
                pa.string(),
                nullable=True,
            ),
        ]
    )


def source_month_bounds(source_file: str) -> tuple[datetime, datetime]:
    match = SOURCE_MONTH_PATTERN.search(source_file)
    if match is None:
        raise ValueError(
            f"Cannot determine month from source filename: {source_file}"
        )

    year = int(match.group(1))
    month = int(match.group(2))

    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)

    return start, end


def count_failures(mask: pa.ChunkedArray, rows: int) -> int:
    return rows - (pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)


def add_rejection_metadata(
    rejected: pa.Table,
    rejected_mask: pa.ChunkedArray,
    *,
    timestamps_valid: pa.ChunkedArray,
    distance_valid: pa.ChunkedArray,
    amounts_valid: pa.ChunkedArray,
    passengers_valid: pa.ChunkedArray,
) -> pa.Table:
    rejection_columns = {
        "_invalid_timestamp": timestamps_valid,
        "_invalid_distance": distance_valid,
        "_invalid_amount": amounts_valid,
        "_invalid_passenger_count": passengers_valid,
    }

    for column_name, valid_mask in rejection_columns.items():
        rejected = rejected.append_column(
            pa.field(column_name, pa.bool_(), nullable=False),
            pc.invert(valid_mask).filter(rejected_mask),
        )

    rejected = rejected.append_column(pa.field(REJECTED_AT_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False),
        pa.repeat(
            pa.scalar(datetime.now(timezone.utc), pa.timestamp("us", tz="UTC")),
            rejected.num_rows,
        ),
    )
    return rejected


def clean_batch(
    batch: pa.Table,
    source_file: str,
) -> tuple[pa.Table, pa.Table, dict]:
    rows_read = batch.num_rows
    batch = batch.rename_columns(
        [COLUMN_RENAMES.get(name, name) for name in batch.column_names]
    )

    pickup = batch["pickup_datetime"]
    dropoff = batch["dropoff_datetime"]

    timestamps_valid = pc.fill_null(
        pc.and_(
            pc.greater(dropoff, pickup),
            pc.and_(
                pc.greater_equal(pickup, pa.scalar(MIN_PICKUP_DATETIME, pickup.type)),
                pc.less(pickup, pa.scalar(MAX_PICKUP_DATETIME_UPPER_BOUND, pickup.type)),
            ),
        ),
        False,
    )

    source_month_start, source_month_end = source_month_bounds(source_file)
    source_month_valid = pc.fill_null(
        pc.and_(
            pc.greater_equal(pickup, pa.scalar(source_month_start, pickup.type)),
            pc.less(pickup, pa.scalar(source_month_end, pickup.type)),
        ),
        False,
    )

    distance_valid = pc.fill_null(
        pc.greater_equal(batch["trip_distance"], 0.0),
        False,
    )

    fare_nonnegative = pc.fill_null(
        pc.greater_equal(batch["fare_amount"], 0.0),
        False,
    )
    total_nonnegative = pc.fill_null(
        pc.greater_equal(batch["total_amount"], 0.0),
        False,
    )
    amounts_valid = pc.and_(fare_nonnegative, total_nonnegative)

    passengers_valid = pc.fill_null(
        pc.greater(batch["passenger_count"], 0),
        True,
    )

    rule_counts = {
        "timestamps": count_failures(timestamps_valid, rows_read),
        "source_month_warning": count_failures(source_month_valid, rows_read),
        "distance": count_failures(distance_valid, rows_read),
        "amounts": count_failures(amounts_valid, rows_read),
        "passengers": count_failures(passengers_valid, rows_read),
    }

    valid_mask = pc.and_(
        timestamps_valid,
        pc.and_(
            distance_valid,
            pc.and_(amounts_valid, passengers_valid),
        ),
    )

    rejected_mask = pc.invert(valid_mask)
    rejected = batch.filter(rejected_mask)
    rejected = add_rejection_metadata(
        rejected,
        rejected_mask,
        timestamps_valid=timestamps_valid,
        distance_valid=distance_valid,
        amounts_valid=amounts_valid,
        passengers_valid=passengers_valid,
    )

    clean = batch.filter(valid_mask)
    rows_valid = clean.num_rows

    trip_fields_missing = pc.and_(
        pc.is_null(batch["passenger_count"]),
        pc.and_(
            pc.is_null(batch["ratecode_id"]),
            pc.and_(
                pc.is_null(batch["congestion_surcharge"]),
                pc.and_(
                    pc.is_null(batch["store_and_fwd_flag"]),
                    pc.is_null(batch["airport_fee"]),
                ),
            ),
        ),
    )

    warning_columns = {
        "_silver_misstored_source_month": pc.invert(source_month_valid),
        "_silver_trip_fields_missing": trip_fields_missing,
        "_silver_zero_distance": pc.equal(batch["trip_distance"], 0.0),
        "_silver_zero_fare": pc.equal(batch["fare_amount"], 0.0),
        "_silver_zero_total": pc.equal(batch["total_amount"], 0.0),
        "_silver_total_less_than_fare": pc.less(
            batch["total_amount"],
            batch["fare_amount"],
        ),
    }

    for column_name, warning_mask in warning_columns.items():
        clean = clean.append_column(
            pa.field(column_name, pa.bool_(), nullable=False),
            pc.fill_null(warning_mask, False).filter(valid_mask),
        )

    clean = clean.group_by(clean.column_names, use_threads=False).aggregate([])

    duration_us = pc.cast(pc.subtract(clean["dropoff_datetime"], clean["pickup_datetime"]), pa.int64())
    duration_min = pc.divide(pc.cast(duration_us, pa.float64()), 60_000_000.0)
    avg_speed_mph = pc.divide(pc.multiply(clean["trip_distance"], 60.0), duration_min)
    clean = clean.append_column(pa.field("trip_duration_min", pa.float64()), duration_min)
    clean = clean.append_column(pa.field("avg_speed_mph", pa.float64()), avg_speed_mph)

    encoded_flag = pc.cast(
        pc.equal(clean["store_and_fwd_flag"], "Y"),
        pa.int8(),
    )
    clean = clean.set_column(
        clean.column_names.index("store_and_fwd_flag"),
        pa.field("store_and_fwd_flag", pa.int8(), nullable=True),
        encoded_flag,
    )

    clean = clean.append_column(
        pa.field(PROCESSED_AT_COLUMN, pa.timestamp("us", tz="UTC"), nullable=False),
        pa.repeat(
            pa.scalar(datetime.now(timezone.utc), pa.timestamp("us", tz="UTC")),
            clean.num_rows,
        ),
    )

    stats = {
        "rows_read": rows_read,
        "rows_rejected_invalid": rejected.num_rows,
        "rows_rejected_duplicate": rows_valid - clean.num_rows,
        "rows_written": clean.num_rows,
        "rows_quarantined": rejected.num_rows,
        "rule_counts": rule_counts,
    }
    return clean, rejected, stats


def ensure_silver_table(catalog: SqlCatalog, sample: pa.Table):
    catalog.create_namespace_if_not_exists(NAMESPACE)
    try:
        return catalog.load_table(TABLE_IDENTIFIER_YELLOW)
    except NoSuchTableError:
        table = catalog.create_table(
            TABLE_IDENTIFIER_YELLOW,
            schema=sample.schema,
            location=TABLE_LOCATION_YELLOW.resolve().as_uri(),
        )
        with table.update_spec() as spec:
            spec.add_field("pickup_datetime", MonthTransform(), "pickup_month")
        print(f"Created Iceberg table {TABLE_IDENTIFIER_YELLOW} at {TABLE_LOCATION_YELLOW}")
        return table


def ensure_rejected_table(catalog: SqlCatalog, sample: pa.Table):
    catalog.create_namespace_if_not_exists(QUARANTINE_NAMESPACE)
    try:
        return catalog.load_table(REJECTED_TABLE_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            REJECTED_TABLE_IDENTIFIER,
            schema=sample.schema,
            location=REJECTED_TABLE_LOCATION.resolve().as_uri(),
        )
        print(f"Created Iceberg table {REJECTED_TABLE_IDENTIFIER} at {REJECTED_TABLE_LOCATION}")
        return table


def ensure_transform_log_table(catalog: SqlCatalog):
    catalog.create_namespace_if_not_exists(TRANSFORM_LOG_NAMESPACE)
    try:
        return catalog.load_table(TRANSFORM_LOG_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            TRANSFORM_LOG_IDENTIFIER,
            schema=get_transform_log_schema(),
            location=TRANSFORM_LOG_LOCATION.resolve().as_uri(),
        )
        print(f"Created Iceberg table {TRANSFORM_LOG_IDENTIFIER} at {TRANSFORM_LOG_LOCATION}")
        return table


def bronze_source_files(catalog: SqlCatalog) -> list[str]:
    try:
        log_table = catalog.load_table(INGESTION_LOG_IDENTIFIER)
    except NoSuchTableError:
        return []
    if log_table.current_snapshot() is None:
        return []
    logs = log_table.scan(
        row_filter=f"status == 'success' and target_table == '{BRONZE_TABLE_IDENTIFIER}'",
        selected_fields=("source_file",),
    ).to_arrow()
    return sorted(set(logs["source_file"].to_pylist()))


def already_processed(log_table) -> set[str]:
    if log_table.current_snapshot() is None:
        return set()
    logs = log_table.scan(
        row_filter=f"status == 'success' and target_table == '{TABLE_IDENTIFIER_YELLOW}'",
        selected_fields=("source_file",),
    ).to_arrow()
    return set(logs["source_file"].to_pylist())


def append_transform_log(
    log_table,
    *,
    source_file: str,
    status: str,
    rows_read: int,
    rows_rejected: int,
    rows_written: int,
    snapshot_id: int | None = None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    log_data = pa.Table.from_pylist(
        [
            {
                "source_table": BRONZE_TABLE_IDENTIFIER,
                "source_file": source_file,
                "target_table": TABLE_IDENTIFIER_YELLOW,
                "status": status,
                "rows_read": rows_read,
                "rows_rejected": rows_rejected,
                "rows_written": rows_written,
                "snapshot_id": snapshot_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "error_message": error_message,
            }
        ],
        schema=get_transform_log_schema(),
    )
    log_table.append(log_data)
    log_table.refresh()


def read_bronze_batch(bronze_table, source_file: str) -> pa.Table:
    return bronze_table.scan(
        row_filter=f"{BRONZE_SOURCE_FILE_COLUMN} == '{source_file}'",
    ).to_arrow()


def main() -> int:
    catalog = get_catalog()
    try:
        bronze_table = catalog.load_table(BRONZE_TABLE_IDENTIFIER)
    except NoSuchTableError:
        print(f"Bronze table {BRONZE_TABLE_IDENTIFIER} does not exist. Run ingest_bronze.py first.", file=sys.stderr)
        return 1

    source_files = bronze_source_files(catalog)
    if not source_files:
        print("No successfully ingested bronze files found in the ingestion log.", file=sys.stderr)
        return 1

    log_table = ensure_transform_log_table(catalog)
    processed_files = already_processed(log_table)
    silver_table = None
    rejected_table = None

    processed = 0
    failed = 0
    for source_file in source_files:
        if source_file in processed_files:
            print(f"skip {source_file} (already processed successfully)")
            continue
        started_at = datetime.now(timezone.utc)
        stats = {
            "rows_read": 0,
            "rows_rejected_invalid": 0,
            "rows_rejected_duplicate": 0,
            "rows_written": 0,
            "rows_quarantined": 0,
        }

        try:
            clean_data, rejected_data, stats = clean_batch(read_bronze_batch(bronze_table, source_file), source_file)

            if clean_data.num_rows > 0:
                if silver_table is None:
                    silver_table = ensure_silver_table(catalog, clean_data)
                silver_table.append(clean_data)

            if rejected_data.num_rows > 0:
                if rejected_table is None:
                    rejected_table = ensure_rejected_table(catalog, rejected_data)
                rejected_table.append(rejected_data)
        except Exception as e:
            try:
                append_transform_log(
                    log_table,
                    source_file=source_file,
                    status="failed",
                    rows_read=stats["rows_read"],
                    rows_rejected=stats["rows_rejected_invalid"] + stats["rows_rejected_duplicate"],
                    rows_written=0,
                    snapshot_id=None,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    error_message=str(e),
                )
            except Exception as log_e:
                print(f"Could not write failure log for {source_file}: {log_e}", file=sys.stderr)
            failed += 1
            print(f"Failed to process {source_file}: {e}", file=sys.stderr)
            continue

        snapshot_id = None
        try:
            silver_table.refresh()
            snapshot = silver_table.current_snapshot()
            snapshot_id = snapshot.snapshot_id if snapshot is not None else None
        except Exception as e:
            print(f"Silver committed for {source_file}, but snapshot lookup failed: {e}", file=sys.stderr)

        try:
            append_transform_log(
                log_table,
                source_file=source_file,
                status="success",
                rows_read=stats["rows_read"],
                rows_rejected=stats["rows_rejected_invalid"] + stats["rows_rejected_duplicate"],
                rows_written=stats["rows_written"],
                snapshot_id=snapshot_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
        except Exception as e:
            failed += 1
            print(f"Silver committed for {source_file}, but the success log could not be written: {e}", file=sys.stderr)
            continue

        processed += 1
        print(
            f"process {source_file}: {stats['rows_read']:,} read, "
            f"{stats['rows_rejected_invalid']:,} invalid, "
            f"{stats['rows_rejected_duplicate']:,} duplicate, "
            f"{stats['rows_written']:,} written, "
            f"{stats['rows_quarantined']:,} quarantined, "
            f"snapshot={snapshot_id}"
        )
        breakdown = ", ".join(
            f"{rule}={count:,}" for rule, count in stats["rule_counts"].items()
        )
        print(f"  rejected by rule (rows can fail several): {breakdown}")

    if silver_table is None:
        try:
            silver_table = catalog.load_table(TABLE_IDENTIFIER_YELLOW)
        except NoSuchTableError:
            print("\nDone. Nothing to process and no silver table exists yet.")
            return 1 if failed else 0

    snapshot = silver_table.current_snapshot()
    total = int(snapshot.summary.get("total-records", 0)) if snapshot is not None else 0
    print(
        f"\nDone. {processed} file(s) processed, "
        f"{failed} file(s) failed, "
        f"{len(silver_table.metadata.snapshots)} snapshot(s), "
        f"{total:,} total rows in {TABLE_IDENTIFIER_YELLOW}."
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())