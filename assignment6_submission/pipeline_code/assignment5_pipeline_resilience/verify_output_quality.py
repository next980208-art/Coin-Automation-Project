"""Verify row counts, timestamp uniqueness, and required values in experiment Parquet files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume", "ma_5", "return_1m"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify assignment 5 Parquet outputs")
    parser.add_argument(
        "--experiment-report",
        type=Path,
        default=PROJECT_ROOT / "assignment5_pipeline_resilience/results/assignment5_final_report.json",
    )
    parser.add_argument(
        "--output-report",
        type=Path,
        default=PROJECT_ROOT / "assignment5_pipeline_resilience/results/assignment5_output_quality_check.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    experiment = json.loads(args.experiment_report.read_text(encoding="utf-8"))
    checks: dict[str, object] = {}
    errors: list[str] = []

    scenarios = list(experiment["scenarios"])
    recovery = experiment.get("fault_and_recovery", {}).get("recovery")
    if recovery:
        scenarios.append(
            {
                "name": recovery["name"],
                "expected_unique_count": recovery["expected_rows"],
                "flink": recovery["flink"],
            }
        )

    for scenario in scenarios:
        name = str(scenario["name"])
        expected_rows = int(scenario["expected_unique_count"])
        frames = []
        files = []
        total_bytes = 0
        for relative_path in scenario["flink"]["feature_files"]:
            path = PROJECT_ROOT / relative_path
            if not path.exists():
                errors.append(f"{name}: missing Parquet file: {relative_path}")
                continue
            files.append(str(path.relative_to(PROJECT_ROOT)))
            total_bytes += path.stat().st_size
            frames.append(pd.read_parquet(path))

        if not frames:
            checks[name] = {"healthy": False, "error": "No Parquet files found"}
            continue

        frame = pd.concat(frames, ignore_index=True)
        missing_columns = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
        missing_values = (
            int(frame[REQUIRED_COLUMNS].isna().sum().sum()) if not missing_columns else -1
        )
        duplicate_timestamps = int(frame["timestamp"].duplicated().sum())
        result = {
            "files": files,
            "parquet_bytes": total_bytes,
            "expected_rows": expected_rows,
            "parquet_rows": len(frame),
            "unique_timestamps": int(frame["timestamp"].nunique()),
            "duplicate_timestamps": duplicate_timestamps,
            "missing_required_columns": missing_columns,
            "missing_required_values": missing_values,
        }
        scenario_errors = []
        if len(frame) != expected_rows:
            scenario_errors.append(f"row count {len(frame)} != {expected_rows}")
        if duplicate_timestamps:
            scenario_errors.append(f"duplicate timestamps: {duplicate_timestamps}")
        if missing_columns:
            scenario_errors.append(f"missing columns: {missing_columns}")
        if missing_values > 0:
            scenario_errors.append(f"missing required values: {missing_values}")
        result["errors"] = scenario_errors
        result["healthy"] = not scenario_errors
        checks[name] = result
        errors.extend(f"{name}: {message}" for message in scenario_errors)

    payload = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_report": str(args.experiment_report.relative_to(PROJECT_ROOT)),
        "scenarios": checks,
        "errors": errors,
        "healthy": not errors,
    }
    args.output_report.parent.mkdir(parents=True, exist_ok=True)
    args.output_report.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if errors:
        raise RuntimeError(f"Output quality verification failed: {errors}")


if __name__ == "__main__":
    main()
