"""Run local Kafka load, PyFlink processing, fault, and recovery experiments."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ASSIGNMENT_ROOT = PROJECT_ROOT / "assignment5_pipeline_resilience"
SOURCE_FILE = PROJECT_ROOT / "assignment4_kafka_spark/data/consumed_binance_usdm_events.jsonl"
PRODUCER = PROJECT_ROOT / "assignment4_kafka_spark/kafka_market_event_producer.py"
CONSUMER = PROJECT_ROOT / "assignment4_kafka_spark/kafka_market_event_consumer.py"
PREPARE = PROJECT_ROOT / "assignment4_kafka_spark/prepare_flink_input.py"
FLINK_REPORT = PROJECT_ROOT / "assignment4_kafka_spark/write_flink_report.py"
QUALITY_CHECK = ASSIGNMENT_ROOT / "verify_output_quality.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assignment 5 local load and recovery experiment")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="assignment5.market.events.v1")
    parser.add_argument("--baseline-count", type=int, default=1_000)
    parser.add_argument("--load-count", type=int, default=10_000)
    parser.add_argument("--duplicate-count", type=int, default=500)
    parser.add_argument("--consumer-timeout", type=int, default=180)
    return parser.parse_args()


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_command(command: list[str], log_path: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        "COMMAND\n"
        + subprocess.list2cmdline(command)
        + "\n\nSTDOUT\n"
        + completed.stdout
        + "\n\nSTDERR\n"
        + completed.stderr
        + f"\n\nELAPSED_SECONDS\n{time.monotonic() - started:.6f}\n",
        encoding="utf-8",
    )
    if check and completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}); see {log_path}")
    return completed


def wait_for_kafka(bootstrap_servers: str, timeout_seconds: int = 90) -> None:
    host, port_text = bootstrap_servers.rsplit(":", 1)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port_text)), timeout=2):
                return
        except OSError:
            time.sleep(2)
    raise TimeoutError(f"Kafka did not become reachable: {bootstrap_servers}")


def run_kafka_scenario(
    name: str,
    run_id: str,
    count: int,
    duplicate_count: int,
    args: argparse.Namespace,
) -> dict[str, object]:
    data_dir = ASSIGNMENT_ROOT / "data"
    results_dir = ASSIGNMENT_ROOT / "results"
    logs_dir = ASSIGNMENT_ROOT / "logs"
    consumed_path = data_dir / f"{name}_consumed.jsonl"
    producer_report_path = results_dir / f"{name}_producer.json"
    consumer_report_path = results_dir / f"{name}_consumer.json"
    consumer_log = logs_dir / f"{name}_consumer.log"
    producer_log = logs_dir / f"{name}_producer.log"

    consumer_command = [
        sys.executable,
        str(CONSUMER),
        "--bootstrap-servers",
        args.bootstrap_servers,
        "--topic",
        args.topic,
        "--run-id",
        run_id,
        "--expected-count",
        str(count),
        "--timeout-seconds",
        str(args.consumer_timeout),
        "--output-path",
        str(consumed_path),
        "--report-path",
        str(consumer_report_path),
    ]
    started = time.monotonic()
    consumer_process = subprocess.Popen(
        consumer_command,
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    time.sleep(2)
    producer_command = [
        sys.executable,
        str(PRODUCER),
        "--bootstrap-servers",
        args.bootstrap_servers,
        "--topic",
        args.topic,
        "--source",
        "local-replay",
        "--source-file",
        str(SOURCE_FILE),
        "--count",
        str(count),
        "--duplicate-count",
        str(duplicate_count),
        "--run-id",
        run_id,
        "--report-path",
        str(producer_report_path),
    ]
    run_command(producer_command, producer_log)
    try:
        stdout, stderr = consumer_process.communicate(timeout=args.consumer_timeout + 30)
    except subprocess.TimeoutExpired:
        consumer_process.kill()
        stdout, stderr = consumer_process.communicate()
        raise RuntimeError(f"Consumer timed out for {name}")
    consumer_log.parent.mkdir(parents=True, exist_ok=True)
    consumer_log.write_text(
        "COMMAND\n"
        + subprocess.list2cmdline(consumer_command)
        + "\n\nSTDOUT\n"
        + stdout
        + "\n\nSTDERR\n"
        + stderr,
        encoding="utf-8",
    )
    if consumer_process.returncode != 0:
        raise RuntimeError(f"Consumer failed for {name}; see {consumer_log}")

    producer_report = json.loads(producer_report_path.read_text(encoding="utf-8"))
    consumer_report = json.loads(consumer_report_path.read_text(encoding="utf-8"))
    return {
        "name": name,
        "run_id": run_id,
        "expected_unique_count": count,
        "intentional_duplicate_count": duplicate_count,
        "kafka_end_to_end_seconds": round(time.monotonic() - started, 6),
        "producer": producer_report,
        "consumer": consumer_report,
        "consumed_path": str(consumed_path.relative_to(PROJECT_ROOT)),
    }


def prepare_flink_input(name: str) -> tuple[Path, dict[str, object], float]:
    input_path = ASSIGNMENT_ROOT / "data" / f"{name}_consumed.jsonl"
    csv_path = ASSIGNMENT_ROOT / "data" / f"{name}_flink_input.csv"
    report_path = ASSIGNMENT_ROOT / "results" / f"{name}_flink_input.json"
    started = time.monotonic()
    run_command(
        [
            sys.executable,
            str(PREPARE),
            "--input-path",
            str(input_path),
            "--output-path",
            str(csv_path),
            "--report-path",
            str(report_path),
        ],
        ASSIGNMENT_ROOT / "logs" / f"{name}_prepare.log",
    )
    return (
        csv_path,
        json.loads(report_path.read_text(encoding="utf-8")),
        time.monotonic() - started,
    )


def run_invalid_input_fault(baseline_input: Path) -> dict[str, object]:
    first_event = json.loads(baseline_input.read_text(encoding="utf-8").splitlines()[0])
    first_event.pop("close", None)
    invalid_path = ASSIGNMENT_ROOT / "data" / "fault_invalid_missing_close.jsonl"
    invalid_path.write_text(json.dumps(first_event, ensure_ascii=False) + "\n", encoding="utf-8")
    completed = run_command(
        [
            sys.executable,
            str(PREPARE),
            "--input-path",
            str(invalid_path),
            "--output-path",
            str(ASSIGNMENT_ROOT / "data" / "fault_should_not_exist.csv"),
            "--report-path",
            str(ASSIGNMENT_ROOT / "results" / "fault_should_not_exist.json"),
        ],
        ASSIGNMENT_ROOT / "logs" / "fault_invalid_input.log",
        check=False,
    )
    return {
        "fault": "missing_close_field",
        "input_path": str(invalid_path.relative_to(PROJECT_ROOT)),
        "expected_failure": True,
        "process_return_code": completed.returncode,
        "failure_reproduced": completed.returncode != 0,
        "error_tail": (completed.stderr or completed.stdout).strip().splitlines()[-1],
    }


def run_flink_batch(name: str, csv_path: Path) -> dict[str, object]:
    feature_folder_relative = Path("assignment5_pipeline_resilience/output") / name
    feature_folder_container = "/opt/airflow/project/" + feature_folder_relative.as_posix()
    csv_container = "/opt/airflow/project/" + csv_path.relative_to(PROJECT_ROOT).as_posix()
    started = time.monotonic()
    run_command(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-w",
            "/opt/airflow/project",
            "airflow",
            "python",
            "flink_batch_submitter.py",
            "--raw-file",
            csv_container,
            "--feature-folder",
            feature_folder_container,
            "--keep-raw",
            "--reset-stale-staging",
        ],
        ASSIGNMENT_ROOT / "logs" / f"{name}_flink.log",
    )
    flink_elapsed = time.monotonic() - started

    input_report_path = ASSIGNMENT_ROOT / "results" / f"{name}_flink_input.json"
    input_report = json.loads(input_report_path.read_text(encoding="utf-8"))
    run_id = str(input_report["run_id"])
    marker_path = PROJECT_ROOT / feature_folder_relative / "_markers" / f"_SUCCESS_{run_id}.json"
    report_path = ASSIGNMENT_ROOT / "results" / f"{name}_flink.json"
    run_command(
        [
            sys.executable,
            str(FLINK_REPORT),
            "--input-report",
            str(input_report_path),
            "--marker-path",
            str(marker_path),
            "--project-root",
            str(PROJECT_ROOT),
            "--report-path",
            str(report_path),
        ],
        ASSIGNMENT_ROOT / "logs" / f"{name}_flink_report.log",
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["flink_elapsed_seconds"] = round(flink_elapsed, 6)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def validate_results(report: dict[str, object]) -> list[str]:
    errors = []
    scenarios = report["scenarios"]
    for scenario in scenarios:
        expected = int(scenario["expected_unique_count"])
        consumer = scenario["consumer"]
        flink = scenario["flink"]
        if int(consumer["consumer_received_count"]) != expected:
            errors.append(f"{scenario['name']}: Kafka unique count mismatch")
        if int(flink["flink_output_processed_count"]) != expected:
            errors.append(f"{scenario['name']}: Flink output count mismatch")
        expected_duplicates = int(scenario["intentional_duplicate_count"])
        if int(consumer["duplicate_message_count"]) != expected_duplicates:
            errors.append(f"{scenario['name']}: duplicate count mismatch")
    if not report["fault_and_recovery"]["failure_reproduced"]:
        errors.append("Invalid-input fault was not reproduced")
    return errors


def main() -> None:
    args = parse_args()
    if args.baseline_count < 100 or args.load_count <= args.baseline_count:
        raise ValueError("Use baseline >= 100 and load > baseline.")
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Saved real market events are missing: {SOURCE_FILE}")
    for directory in ("data", "results", "logs", "output"):
        (ASSIGNMENT_ROOT / directory).mkdir(parents=True, exist_ok=True)

    wait_for_kafka(args.bootstrap_servers)
    execution_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scenarios = []
    for name, count, duplicates in (
        ("baseline_1000", args.baseline_count, 0),
        ("load_10000", args.load_count, args.duplicate_count),
    ):
        run_id = f"assignment5-{name}-{execution_id}"
        print(f"[{name}] Kafka replay: unique={count}, duplicates={duplicates}", flush=True)
        scenario = run_kafka_scenario(name, run_id, count, duplicates, args)
        csv_path, input_report, prepare_elapsed = prepare_flink_input(name)
        print(f"[{name}] PyFlink batch: rows={input_report['valid_flink_input_count']}", flush=True)
        flink_report = run_flink_batch(name, csv_path)
        scenario["prepare_elapsed_seconds"] = round(prepare_elapsed, 6)
        scenario["flink"] = flink_report
        scenario["total_pipeline_seconds"] = round(
            float(scenario["kafka_end_to_end_seconds"])
            + prepare_elapsed
            + float(flink_report["flink_elapsed_seconds"]),
            6,
        )
        scenarios.append(scenario)

    fault = run_invalid_input_fault(ASSIGNMENT_ROOT / "data" / "baseline_1000_consumed.jsonl")
    fault["recovery_action"] = "Use the validated baseline JSONL and rerun preparation plus PyFlink."
    fault["recovery_output_rows"] = scenarios[0]["flink"]["flink_output_processed_count"]
    fault["recovery_duplicate_rows"] = scenarios[0]["consumer"]["duplicate_message_count"]

    report = {
        "assignment": "5차시 데이터 파이프라인 부하·장애·복구 실험",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_source": {
            "type": "local replay derived from saved real Binance USDT-M BTCUSDT OHLCV",
            "source_file": str(SOURCE_FILE.relative_to(PROJECT_ROOT)),
            "external_load_sent": False,
        },
        "pipeline": "local JSONL -> Kafka -> consumer JSONL -> PyFlink Batch -> Parquet",
        "scenarios": scenarios,
        "fault_and_recovery": fault,
    }
    errors = validate_results(report)
    report["validation"] = {
        "expected_checks": [
            "Kafka unique count equals requested count",
            "Flink output count equals Kafka unique count",
            "intentional duplicate count is detected exactly",
            "invalid input fails safely",
        ],
        "errors": errors,
        "healthy": not errors,
    }
    final_report_path = ASSIGNMENT_ROOT / "results" / "assignment5_final_report.json"
    write_json(final_report_path, report)
    run_command(
        [
            sys.executable,
            str(QUALITY_CHECK),
            "--experiment-report",
            str(final_report_path),
            "--output-report",
            str(ASSIGNMENT_ROOT / "results" / "assignment5_output_quality_check.json"),
        ],
        ASSIGNMENT_ROOT / "logs" / "output_quality_check.log",
    )
    print(json.dumps(report["validation"], ensure_ascii=False), flush=True)
    if errors:
        raise RuntimeError(f"Assignment 5 validation failed: {errors}")


if __name__ == "__main__":
    main()
