"""Build the assignment 6 comparison, alert, and fallback evidence reports."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


SUBMISSION_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build assignment 6 evidence from actual run reports")
    parser.add_argument(
        "--experiment-report",
        type=Path,
        default=SUBMISSION_ROOT / "source_results/assignment5_final_report.json",
    )
    parser.add_argument(
        "--quality-report",
        type=Path,
        default=SUBMISSION_ROOT / "source_results/assignment5_output_quality_check.json",
    )
    parser.add_argument(
        "--output-report",
        type=Path,
        default=SUBMISSION_ROOT / "results/assignment6_pipeline_review.json",
    )
    parser.add_argument(
        "--alert-report",
        type=Path,
        default=SUBMISSION_ROOT / "results/assignment6_alert_and_fallback.json",
    )
    parser.add_argument(
        "--alert-log",
        type=Path,
        default=SUBMISSION_ROOT / "logs/assignment6_alert.log",
    )
    return parser.parse_args()


def read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def scenario_summary(scenario: dict[str, object], quality: dict[str, object]) -> dict[str, object]:
    producer = scenario["producer"]
    consumer = scenario["consumer"]
    flink = scenario["flink"]
    expected = int(scenario["expected_unique_count"])
    sent = int(producer["producer_sent_count"])
    consumed_unique = int(consumer["consumer_received_count"])
    flink_input = int(flink["flink_input_valid_count"])
    stored = int(flink["flink_output_processed_count"])
    total_seconds = float(scenario["total_pipeline_seconds"])
    duplicate_count = int(consumer["duplicate_message_count"])
    unexpected_unprocessed = max(expected - stored, 0)
    errors = []
    if consumed_unique != expected:
        errors.append(f"consumer unique {consumed_unique} != expected {expected}")
    if flink_input != expected:
        errors.append(f"Flink input {flink_input} != expected {expected}")
    if stored != expected:
        errors.append(f"stored {stored} != expected {expected}")
    if not quality.get("healthy", False):
        errors.append("Parquet quality check failed")

    return {
        "name": scenario["name"],
        "run_id": scenario["run_id"],
        "producer_sent_count": sent,
        "expected_unique_count": expected,
        "consumer_unique_count": consumed_unique,
        "intentional_duplicate_count": int(scenario["intentional_duplicate_count"]),
        "consumer_detected_duplicate_count": duplicate_count,
        "flink_input_count": flink_input,
        "final_parquet_count": stored,
        "filtered_count": sent - stored,
        "unexpected_unprocessed_count": unexpected_unprocessed,
        "kafka_seconds": float(scenario["kafka_end_to_end_seconds"]),
        "flink_seconds": float(flink["flink_elapsed_seconds"]),
        "total_pipeline_seconds": total_seconds,
        "producer_records_per_second": float(producer["send_records_per_second"]),
        "consumer_unique_records_per_second": float(consumer["unique_records_per_second"]),
        "final_rows_per_second": round(stored / total_seconds, 3),
        "parquet_duplicate_timestamps": int(quality["duplicate_timestamps"]),
        "parquet_missing_required_values": int(quality["missing_required_values"]),
        "errors": errors,
        "healthy": not errors,
    }


def main() -> None:
    args = parse_args()
    experiment = read_json(args.experiment_report)
    quality_report = read_json(args.quality_report)
    quality_by_name = quality_report["scenarios"]

    scenarios = [
        scenario_summary(scenario, quality_by_name[str(scenario["name"])])
        for scenario in experiment["scenarios"]
    ]
    baseline, load = scenarios
    fault = experiment["fault_and_recovery"]
    recovery = fault["recovery"]
    recovery_quality = quality_by_name[str(recovery["name"])]
    recovery_stored = int(recovery["flink"]["flink_output_processed_count"])
    recovery_expected = int(recovery["expected_rows"])
    alert_triggered = bool(fault["failure_reproduced"] and int(fault["process_return_code"]) != 0)
    fallback_succeeded = bool(
        alert_triggered
        and recovery_stored == recovery_expected
        and recovery_quality.get("healthy", False)
    )

    alert_payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "alert": {
            "triggered": alert_triggered,
            "severity": "ERROR",
            "code": "REQUIRED_FIELD_MISSING",
            "failed_stage": "input validation before Flink submission",
            "fault": fault["fault"],
            "process_return_code": int(fault["process_return_code"]),
            "message": fault["error_tail"],
            "delivery": "local JSON report and log",
            "external_notification_configured": False,
        },
        "fallback": {
            "triggered": alert_triggered,
            "strategy": "reuse the last validated JSONL and restart from Flink input preparation",
            "kafka_replay_required": False,
            "recovery_job_id": recovery["flink"]["flink_job_id"],
            "expected_rows": recovery_expected,
            "stored_rows": recovery_stored,
            "duplicate_timestamps": int(recovery_quality["duplicate_timestamps"]),
            "missing_required_values": int(recovery_quality["missing_required_values"]),
            "succeeded": fallback_succeeded,
        },
        "final_status": "resolved" if fallback_succeeded else "unresolved",
    }
    write_json(args.alert_report, alert_payload)

    args.alert_log.parent.mkdir(parents=True, exist_ok=True)
    args.alert_log.write_text(
        "\n".join(
            [
                f"ALERT_TRIGGERED={str(alert_triggered).lower()}",
                "ALERT_CODE=REQUIRED_FIELD_MISSING",
                "FAILED_STAGE=input validation before Flink submission",
                f"PROCESS_RETURN_CODE={fault['process_return_code']}",
                f"ERROR={fault['error_tail']}",
                "FALLBACK=validated JSONL -> Flink input preparation -> PyFlink Batch -> Parquet",
                f"RECOVERY_JOB_ID={recovery['flink']['flink_job_id']}",
                f"RECOVERY_EXPECTED_ROWS={recovery_expected}",
                f"RECOVERY_STORED_ROWS={recovery_stored}",
                f"FALLBACK_SUCCEEDED={str(fallback_succeeded).lower()}",
                f"FINAL_STATUS={alert_payload['final_status']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    errors = [error for scenario in scenarios for error in scenario["errors"]]
    if not alert_triggered:
        errors.append("Expected invalid-input alert was not triggered")
    if not fallback_succeeded:
        errors.append("Fallback recovery did not restore the expected output")

    report = {
        "assignment": "6차시 부하·복구 결과 보완 및 전체 흐름 점검",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_experiment_executed_at_utc": experiment["executed_at_utc"],
        "source_data": experiment["data_source"],
        "processing_engine": "Apache Flink PyFlink Batch (project standard; Spark is not used)",
        "pipeline": experiment["pipeline"],
        "baseline_and_load": scenarios,
        "comparison": {
            "unique_input_multiplier": round(
                int(load["expected_unique_count"]) / int(baseline["expected_unique_count"]), 3
            ),
            "elapsed_time_multiplier": round(
                float(load["total_pipeline_seconds"]) / float(baseline["total_pipeline_seconds"]), 3
            ),
            "final_throughput_multiplier": round(
                float(load["final_rows_per_second"]) / float(baseline["final_rows_per_second"]), 3
            ),
        },
        "failure_and_restart": {
            "failed_stage": "input validation before Flink submission",
            "failure_reproduced": bool(fault["failure_reproduced"]),
            "process_return_code": int(fault["process_return_code"]),
            "restart_location": "validated JSONL -> Flink input preparation",
            "recovery_job_id": recovery["flink"]["flink_job_id"],
            "recovery_expected_rows": recovery_expected,
            "recovery_stored_rows": recovery_stored,
        },
        "alert_and_fallback_report": str(args.alert_report.relative_to(SUBMISSION_ROOT)),
        "validation": {
            "errors": errors,
            "healthy": not errors,
        },
    }
    write_json(args.output_report, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if errors:
        raise RuntimeError(f"Assignment 6 review failed: {errors}")


if __name__ == "__main__":
    main()
