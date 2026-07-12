import sys
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR_YELLOW = PROJECT_ROOT / "data" / "raw" / "yellow"

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = (
    "2026-01",
    "2026-02",
    "2026-03",
)

CHUNK_SIZE = 1 << 20  # 1 MiB


def download(url: str, target: Path) -> None:
    partial = target.with_name(target.name + ".part")
    with urllib.request.urlopen(url) as response, open(partial, "wb") as out:
        total_bytes = int(response.headers.get("Content-Length") or 0)
        copied = 0
        while chunk := response.read(CHUNK_SIZE):
            out.write(chunk)
            copied += len(chunk)
            if total_bytes:
                print(
                    f"\r  {copied / total_bytes:6.1%} of {total_bytes / 1e6:,.1f} MB",
                    end="",
                    flush=True,
                )
        print()
    partial.replace(target)


def main() -> int:
    RAW_DIR_YELLOW.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    skipped = 0
    failed = 0
    for month in MONTHS:
        filename = f"yellow_tripdata_{month}.parquet"
        target = RAW_DIR_YELLOW / filename
        if target.exists():
            skipped += 1
            print(f"skip {filename} (already downloaded)")
            continue

        url = f"{BASE_URL}/{filename}"
        print(f"download {url}")
        try:
            download(url, target)
        except Exception as e:
            failed += 1
            print(f"Failed to download {filename}: {e}", file=sys.stderr)
            continue

        downloaded += 1
        print(f"saved {target.relative_to(PROJECT_ROOT)} ({target.stat().st_size:,} bytes)")

    print(
        f"\nDone. {downloaded} file(s) downloaded, "
        f"{skipped} skipped, {failed} failed, "
        f"raw directory: {RAW_DIR_YELLOW.relative_to(PROJECT_ROOT)}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
