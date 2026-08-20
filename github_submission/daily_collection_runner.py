import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
AGGTRADE_RECOVERY_WINDOW = timedelta(hours=47)
EXPECTED_FLINK_PROCESSOR = "apache_flink_pyflink_batch"
EXPECTED_FLINK_SCHEMA_VERSION = "ohlcv_basic_v2_boundary4"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect one completed UTC day of BTCUSDT USDT-M research data."
    )
    parser.add_argument("--data-date", help="UTC date to collect, YYYY-MM-DD. Defaults to yesterday UTC.")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--max-aggtrade-pages", type=int, default=1000)
    parser.add_argument(
        "--feature-folder",
        default="feature_store_v2",
        help="Validated PyFlink feature output folder.",
    )
    parser.add_argument("--context-folder", default="futures_context_store_v2")
    parser.add_argument("--trade-context-folder", default="trade_context_store_v2")
    parser.add_argument("--state-folder", default="scheduler_state")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def safe_value(value):
    return str(value).replace("/", "").replace(":", "").replace(" ", "")


def parse_data_date(value):
    if value:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)


def run_id(symbol, prefix, timeframe, start_dt, end_dt):
    return f"{safe_value(symbol)}_{prefix}_{timeframe}_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}"


def marker_paths(args, start_dt, end_dt):
    symbol = safe_value(args.symbol)
    feature_run_id = run_id(args.symbol, "USDM", args.timeframe, start_dt, end_dt)
    context_run_id = f"{symbol}_USDM_CONTEXT_{safe_value(args.timeframe)}_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}"
    aggtrade_run_id = (
        f"{symbol}_USDM_AGGTRADE_1m_{start_dt:%Y%m%dT%H%M}_{end_dt:%Y%m%dT%H%M}"
    )
    return {
        "flink_features": PROJECT_ROOT
        / args.feature_folder
        / "_markers"
        / f"_SUCCESS_{feature_run_id}.json",
        "futures_context": PROJECT_ROOT
        / args.context_folder
        / "_markers"
        / f"_SUCCESS_{context_run_id}.json",
        "aggtrade": PROJECT_ROOT
        / args.trade_context_folder
        / "_markers"
        / f"_SUCCESS_{aggtrade_run_id}.json",
    }


def raw_file_path(args, start_dt, end_dt):
    run_id = run_id_for_features(args, start_dt, end_dt)
    return PROJECT_ROOT / "temp_raw_data" / f"raw_{run_id}.csv"


def run_id_for_features(args, start_dt, end_dt):
    return run_id(args.symbol, "USDM", args.timeframe, start_dt, end_dt)


def command_for(name, args, start_dt, end_dt):
    python = sys.executable
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")
    if name == "raw_download":
        return [
            python,
            "1_chunk_downloader.py",
            "--market",
            "usdm",
            "--symbol",
            args.symbol,
            "--timeframe",
            args.timeframe,
            "--start-date",
            start_date,
            "--days",
            "1",
            "--no-kafka",
            "--no-header",
        ]
    if name == "flink_features":
        return [
            python,
            "flink_batch_submitter.py",
            "--raw-file",
            str(raw_file_path(args, start_dt, end_dt)),
            "--feature-folder",
            args.feature_folder,
        ]
    if name == "futures_context":
        return [
            python,
            "9_futures_context_collector.py",
            "--symbol",
            args.symbol,
            "--timeframe",
            args.timeframe,
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--context-folder",
            args.context_folder,
        ]
    if name == "aggtrade":
        return [
            python,
            "10_aggtrade_collector.py",
            "--symbol",
            args.symbol,
            "--start-datetime",
            start_dt.isoformat(),
            "--end-datetime",
            end_dt.isoformat(),
            "--max-pages",
            str(args.max_aggtrade_pages),
            "--context-folder",
            args.trade_context_folder,
        ]
    raise ValueError(f"Unknown step: {name}")


def success_marker_is_valid(name, marker):
    if marker is None or not marker.exists():
        return False
    if name != "flink_features":
        return True
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        payload.get("processor") == EXPECTED_FLINK_PROCESSOR
        and payload.get("feature_schema_version") == EXPECTED_FLINK_SCHEMA_VERSION
    )


def execute_step(name, command, marker, dry_run):
    result = {
        "name": name,
        "command": command,
        "marker": str(marker),
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    if success_marker_is_valid(name, marker):
        result["status"] = "skipped_existing_marker"
        return result
    if dry_run:
        result["status"] = "dry_run"
        return result

    completed = subprocess.run(command, cwd=PROJECT_ROOT, text=True, capture_output=True)
    result["return_code"] = completed.returncode
    result["stdout"] = completed.stdout[-10_000:]
    result["stderr"] = completed.stderr[-10_000:]
    result["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    if completed.returncode != 0:
        result["status"] = "failed"
        return result
    if marker and not success_marker_is_valid(name, marker):
        result["status"] = "failed_missing_or_invalid_success_marker"
        return result
    result["status"] = "processed"
    return result


def write_manifest(args, start_dt, end_dt, results):
    state_dir = PROJECT_ROOT / args.state_folder / "daily_runs"
    state_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = state_dir / f"{safe_value(args.symbol)}_USDM_{args.timeframe}_{start_dt:%Y%m%d}.json"
    payload = {
        "symbol": args.symbol,
        "market": "usdm",
        "timeframe": args.timeframe,
        "feature_folder": args.feature_folder,
        "context_folder": args.context_folder,
        "trade_context_folder": args.trade_context_folder,
        "data_interval_start_utc": start_dt.isoformat(),
        "data_interval_end_utc": end_dt.isoformat(),
        "aggtrade_recovery_window_hours": AGGTRADE_RECOVERY_WINDOW.total_seconds() / 3600,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "steps": results,
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def main():
    args = parse_args()
    if args.max_aggtrade_pages <= 0:
        raise ValueError("--max-aggtrade-pages must be positive.")
    if not args.dry_run and not Path("/opt/flink/bin/flink").exists():
        raise RuntimeError(
            "실제 일일 수집은 Airflow 컨테이너 안에서 실행해야 합니다. 예: "
            "docker compose exec -T airflow python daily_collection_runner.py "
            "--data-date YYYY-MM-DD"
        )

    start_dt = parse_data_date(args.data_date)
    end_dt = start_dt + timedelta(days=1)
    now = datetime.now(timezone.utc)
    if end_dt > now:
        raise ValueError("Only completed UTC days can be collected.")

    markers = marker_paths(args, start_dt, end_dt)
    results = []
    if success_marker_is_valid("flink_features", markers["flink_features"]):
        results.append(
            execute_step(
                "flink_features",
                command_for("flink_features", args, start_dt, end_dt),
                markers["flink_features"],
                args.dry_run,
            )
        )
    else:
        raw_file = raw_file_path(args, start_dt, end_dt)
        if raw_file.exists() and not args.dry_run:
            results.append(
                {
                    "name": "raw_download",
                    "status": "reused_existing_raw_for_retry",
                    "raw_file": str(raw_file),
                }
            )
        else:
            results.append(execute_step("raw_download", command_for("raw_download", args, start_dt, end_dt), None, args.dry_run))
        if not args.dry_run and not raw_file.exists():
            results.append({"name": "flink_features", "status": "failed_missing_raw_file"})
        else:
            results.append(
                execute_step(
                    "flink_features",
                    command_for("flink_features", args, start_dt, end_dt),
                    markers["flink_features"],
                    args.dry_run,
                )
            )

    results.append(
        execute_step(
            "futures_context",
            command_for("futures_context", args, start_dt, end_dt),
            markers["futures_context"],
            args.dry_run,
        )
    )

    aggtrade_age = now - end_dt
    if aggtrade_age > AGGTRADE_RECOVERY_WINDOW:
        results.append(
            {
                "name": "aggtrade",
                "status": "unrecoverable_outside_public_api_window",
                "marker": str(markers["aggtrade"]),
                "age_hours": round(aggtrade_age.total_seconds() / 3600, 2),
                "message": "The public USDT-M aggTrade API cannot reliably recover this old interval.",
            }
        )
    else:
        results.append(
            execute_step(
                "aggtrade",
                command_for("aggtrade", args, start_dt, end_dt),
                markers["aggtrade"],
                args.dry_run,
            )
        )

    manifest_path = None if args.dry_run else write_manifest(args, start_dt, end_dt, results)
    print(f"Daily UTC interval: {start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d}")
    for result in results:
        print(f"{result['name']}: {result['status']}")
    if manifest_path:
        print(f"Manifest: {manifest_path}")

    failures = [step for step in results if step["status"].startswith("failed")]
    if failures:
        raise RuntimeError(f"Daily collection failed: {[step['name'] for step in failures]}")


if __name__ == "__main__":
    main()
