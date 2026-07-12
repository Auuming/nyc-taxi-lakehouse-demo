import argparse
import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_STEPS = (
    PROJECT_ROOT / "scripts" / "download_dataset.py",
    PROJECT_ROOT / "pipelines" / "ingest_bronze.py",
    PROJECT_ROOT / "pipelines" / "transform_silver_with_quarantine.py",
    PROJECT_ROOT / "pipelines" / "build_gold.py",
)
DASHBOARD_APP = PROJECT_ROOT / "dashboard" / "app.py"


def streamlit_command() -> list[str] | None:
    if importlib.util.find_spec("streamlit") is not None:
        return [sys.executable, "-m", "streamlit", "run", str(DASHBOARD_APP)]
    executable = shutil.which("streamlit")
    if executable is not None:
        return [executable, "run", str(DASHBOARD_APP)]
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the full lakehouse flow: download raw data, ingest Bronze, "
            "transform Silver, build Gold, then launch the Streamlit dashboard."
        )
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="stop after build_gold.py instead of launching Streamlit",
    )
    args = parser.parse_args()

    for step in PIPELINE_STEPS:
        print(f"\n=== {step.relative_to(PROJECT_ROOT)} ===", flush=True)
        result = subprocess.run([sys.executable, str(step)])
        if result.returncode != 0:
            print(
                f"Stopping: {step.name} exited with code {result.returncode}.",
                file=sys.stderr,
            )
            return result.returncode

    if args.no_dashboard:
        print("\nDone. All pipelines finished; dashboard skipped (--no-dashboard).")
        return 0

    command = streamlit_command()
    if command is None:
        print(
            "\nAll pipelines finished, but streamlit is not available in this "
            "environment or on PATH. Install the dashboard requirements first:\n"
            "  pip install -r dashboard/requirements-dashboard.txt\n"
            "then run: streamlit run dashboard/app.py",
            file=sys.stderr,
        )
        return 1

    print("\n=== dashboard/app.py (Ctrl+C to stop) ===", flush=True)
    return subprocess.run(command).returncode


if __name__ == "__main__":
    raise SystemExit(main())
