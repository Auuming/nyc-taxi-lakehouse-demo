import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_RAW_DIR = DATA_DIR / "raw"
DEFAULT_RAW_DIR_YELLOW = DEFAULT_RAW_DIR / "yellow"

DEFAULT_WAREHOUSE_DIR = DATA_DIR / "warehouse"
CATALOG_DB = DATA_DIR / "catalog.db"

NAMESPACE = "bronze"
TABLE_IDENTIFIER_YELLOW = f"{NAMESPACE}.yellow_trips"
TABLE_LOCATION_YELLOW = DEFAULT_WAREHOUSE_DIR / NAMESPACE / "yellow_trips"
SOURCE_FILE_COLUMN = "_bronze_source_file"
INGESTED_AT_COLUMN = "_bronze_ingested_at"

INGESTION_LOG_NAMESPACE = "metadata"
INGESTION_LOG_IDENTIFIER = f"{INGESTION_LOG_NAMESPACE}.ingestion_log"
INGESTION_LOG_LOCATION = DEFAULT_WAREHOUSE_DIR / INGESTION_LOG_NAMESPACE / "ingestion_log"


def get_catalog() -> SqlCatalog:
    CATALOG_DB.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    return SqlCatalog(
        "local",
        uri=f"sqlite:///{CATALOG_DB.resolve()}",
        warehouse=DEFAULT_WAREHOUSE_DIR.resolve().as_uri(),
    )


def add_lineage_columns(table: pa.Table, source_file: str) -> pa.Table:
    n = table.num_rows
    ingested_at = datetime.now(timezone.utc)
    source_file_field = pa.field(
        SOURCE_FILE_COLUMN,
        pa.string(),
        nullable=False,
    )
    ingested_at_field = pa.field(
        INGESTED_AT_COLUMN,
        pa.timestamp("us", tz="UTC"),
        nullable=False,
    )

    table = table.append_column(
        source_file_field,
        pa.repeat(pa.scalar(source_file, pa.string()), n),
    )
    table = table.append_column(
        ingested_at_field,
        pa.repeat(pa.scalar(ingested_at, pa.timestamp("us", tz="UTC")), n),
    )
    return table


def get_ingestion_log_schema() -> pa.Schema:
    return pa.schema(
        [
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
                "row_count",
                pa.int64(),
                nullable=False,
            ),
            pa.field(
                "file_size_bytes",
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


def ensure_bronze_table(catalog: SqlCatalog, sample: pa.Table):
    catalog.create_namespace_if_not_exists(NAMESPACE)
    try:
        return catalog.load_table(TABLE_IDENTIFIER_YELLOW)
    except NoSuchTableError:
        table = catalog.create_table(
            TABLE_IDENTIFIER_YELLOW,
            schema=sample.schema,
            location=TABLE_LOCATION_YELLOW.resolve().as_uri(),
        )
        print(f"Created Iceberg table {TABLE_IDENTIFIER_YELLOW} at {TABLE_LOCATION_YELLOW}")
        return table


def ensure_ingestion_log_table(catalog: SqlCatalog):
    catalog.create_namespace_if_not_exists(INGESTION_LOG_NAMESPACE)
    try:
        return catalog.load_table(INGESTION_LOG_IDENTIFIER)
    except NoSuchTableError:
        table = catalog.create_table(
            INGESTION_LOG_IDENTIFIER,
            schema=get_ingestion_log_schema(),
            location=INGESTION_LOG_LOCATION.resolve().as_uri(),
        )
        print(f"Created Iceberg table {INGESTION_LOG_IDENTIFIER} at {INGESTION_LOG_LOCATION}")
        return table


def already_ingested(log_table) -> set[str]:
    if log_table.current_snapshot() is None:
        return set()
    logs = log_table.scan(
        row_filter="status == 'success'",
        selected_fields=("source_file",),
    ).to_arrow()
    return set(logs["source_file"].to_pylist())


def append_ingestion_log(
    log_table,
    *,
    source_file: str,
    target_table: str,
    status: str,
    row_count: int,
    file_size_bytes: int,
    snapshot_id: int | None = None,
    started_at: datetime,
    finished_at: datetime,
    error_message: str | None = None,
) -> None:
    log_data = pa.Table.from_pylist(
        [
            {
                "source_file": source_file,
                "target_table": target_table,
                "status": status,
                "row_count": row_count,
                "file_size_bytes": file_size_bytes,
                "snapshot_id": snapshot_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "error_message": error_message,
            }
        ],
        schema=get_ingestion_log_schema(),
    )
    log_table.append(log_data)
    log_table.refresh()


def main() -> int:
    raw_files = sorted(DEFAULT_RAW_DIR_YELLOW.glob("*.parquet"))
    if not raw_files:
        print(f"No parquet files found in {DEFAULT_RAW_DIR_YELLOW}", file=sys.stderr)
        return 1

    catalog = get_catalog()
    sample = add_lineage_columns(pq.read_table(raw_files[0]), raw_files[0].name)
    bronze_table = ensure_bronze_table(catalog, sample)
    log_table = ensure_ingestion_log_table(catalog)
    ingested_files = already_ingested(log_table)

    appended_files = 0
    failed = 0
    for path in raw_files:
        if path.name in ingested_files:
            print(f"skip {path.name} (already ingested successfully)")
            continue
        started_at = datetime.now(timezone.utc)
        row_count = 0

        try:
            batch = add_lineage_columns(pq.read_table(path), path.name)
            row_count = batch.num_rows
            bronze_table.append(batch)
        except Exception as e:
            try:
                append_ingestion_log(
                    log_table,
                    source_file=path.name,
                    target_table=TABLE_IDENTIFIER_YELLOW,
                    status="failed",
                    row_count=row_count,
                    file_size_bytes=path.stat().st_size,
                    snapshot_id=None,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    error_message=str(e),
                )
            except Exception as log_e:
                print(f"Could not write failure log for {path.name}: {log_e}", file=sys.stderr)
            failed += 1
            print(f"Failed to ingest {path.name}: {e}", file=sys.stderr)
            continue

        snapshot_id = None
        try:
            bronze_table.refresh()
            snapshot = bronze_table.current_snapshot()
            snapshot_id = snapshot.snapshot_id if snapshot is not None else None
        except Exception as e:
            print(f"Bronze committed for {path.name}, but snapshot lookup failed: {e}", file=sys.stderr)

        try:
            append_ingestion_log(
                log_table,
                source_file=path.name,
                target_table=TABLE_IDENTIFIER_YELLOW,
                status="success",
                row_count=row_count,
                file_size_bytes=path.stat().st_size,
                snapshot_id=snapshot_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc),
            )
        except Exception as e:
            failed += 1
            print(f"Bronze committed for {path.name}, but the success log could not be written: {e}", file=sys.stderr)
            continue

        appended_files += 1
        print(f"append {path.name}: {row_count:,} rows, snapshot={snapshot_id}")

    snapshot = bronze_table.current_snapshot()
    total = int(snapshot.summary.get("total-records", 0)) if snapshot is not None else 0
    print(
        f"\nDone. {appended_files} file(s) appended, "
        f"{failed} file(s) failed, "
        f"{len(bronze_table.metadata.snapshots)} snapshot(s), "
        f"{total:,} total rows in {TABLE_IDENTIFIER_YELLOW}."
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
