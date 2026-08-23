"""Send test or Binance USD-M futures OHLCV events to Kafka."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ccxt
from kafka import KafkaProducer
from kafka.serializer import Serializer


DEFAULT_TOPIC = "btc_market_events_v1"


class JsonSerializer(Serializer):
    def serialize(self, topic: str, headers: object, data: object) -> bytes:
        return json.dumps(data).encode("utf-8")


class Utf8Serializer(Serializer):
    def serialize(self, topic: str, headers: object, data: object) -> bytes:
        return str(data).encode("utf-8")


def build_test_event(sequence: int, event_time: datetime, run_id: str) -> dict[str, object]:
    """Create a valid, reproducible OHLCV-shaped event for the assignment."""
    base_price = 65000.0 + sequence * 1.8 + math.sin(sequence / 11.0) * 45.0
    open_price = round(base_price, 2)
    close_price = round(base_price + math.sin(sequence / 5.0) * 12.0, 2)
    high_price = round(max(open_price, close_price) + 8.0 + (sequence % 5), 2)
    low_price = round(min(open_price, close_price) - 8.0 - (sequence % 3), 2)

    return {
        "schema_version": "market_event_v1",
        "run_id": run_id,
        "event_id": f"{run_id}-{sequence:04d}",
        "event_sequence": sequence,
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": "BTCUSDT",
        "market": "USDT-M",
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": round(10.0 + (sequence % 31) * 0.75, 4),
        "source": "assignment4_deterministic_test",
    }


def to_utc_text(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def fetch_binance_usdm_events(count: int, timeframe: str, run_id: str) -> list[dict[str, object]]:
    """Fetch only closed BTCUSDT perpetual candles from Binance's public API."""
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    interval_ms = int(exchange.parse_timeframe(timeframe) * 1000)
    if interval_ms <= 0:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    now_ms = exchange.milliseconds()
    latest_closed_start_ms = (now_ms // interval_ms - 1) * interval_ms
    since_ms = latest_closed_start_ms - (count - 1) * interval_ms
    rows = exchange.fetch_ohlcv("BTC/USDT:USDT", timeframe, since=since_ms, limit=count)

    closed_rows = sorted(
        {int(row[0]): row for row in rows if int(row[0]) <= latest_closed_start_ms}.values(),
        key=lambda row: int(row[0]),
    )
    expected_timestamps = list(range(since_ms, latest_closed_start_ms + interval_ms, interval_ms))
    actual_timestamps = [int(row[0]) for row in closed_rows]
    if actual_timestamps != expected_timestamps:
        missing = sorted(set(expected_timestamps) - set(actual_timestamps))
        raise RuntimeError(
            "Binance USDT-M OHLCV data is incomplete: "
            f"expected={count}, actual={len(closed_rows)}, missing={len(missing)}, "
            f"first_missing={missing[:3]}"
        )

    events: list[dict[str, object]] = []
    for sequence, row in enumerate(closed_rows):
        timestamp_ms, open_price, high_price, low_price, close_price, volume = row[:6]
        events.append(
            {
                "schema_version": "market_event_v1",
                "run_id": run_id,
                "event_id": f"{run_id}-{int(timestamp_ms)}",
                "event_sequence": sequence,
                "event_time": to_utc_text(int(timestamp_ms)),
                "symbol": "BTCUSDT",
                "market": "USDT-M",
                "open": float(open_price),
                "high": float(high_price),
                "low": float(low_price),
                "close": float(close_price),
                "volume": float(volume),
                "source": "binance_usdm_public_ohlcv",
            }
        )
    return events


def write_report(path: Path, report: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka market event producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--source",
        choices=["test", "binance-usdm"],
        default="test",
        help="test creates repeatable data; binance-usdm fetches closed public BTCUSDT futures candles.",
    )
    parser.add_argument("--timeframe", default="1m", help="Binance timeframe used with --source binance-usdm.")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--run-id", default="assignment4-demo-v1")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate events, then write the report without sending to Kafka.",
    )
    parser.add_argument(
        "--report-path",
        default="assignment4_kafka_spark/results/producer_report.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.count < 100:
        raise ValueError("과제 요구사항에 따라 --count는 100 이상이어야 합니다.")

    if args.source == "binance-usdm":
        events = fetch_binance_usdm_events(args.count, args.timeframe, args.run_id)
    else:
        start_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        start_time -= timedelta(minutes=args.count - 1)
        events = [
            build_test_event(sequence, start_time + timedelta(minutes=sequence), args.run_id)
            for sequence in range(args.count)
        ]

    if len(events) != args.count:
        raise RuntimeError(f"Expected {args.count} events, received {len(events)}")

    if not args.dry_run:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=JsonSerializer(),
            key_serializer=Utf8Serializer(),
            acks="all",
            retries=3,
        )
        try:
            futures = [
                producer.send(args.topic, key=event["event_id"], value=event) for event in events
            ]
            for future in futures:
                future.get(timeout=30)
            producer.flush(timeout=30)
        finally:
            producer.close(timeout=30)

    report = {
        "status": "dry_run" if args.dry_run else "success",
        "topic": args.topic,
        "run_id": args.run_id,
        "producer_sent_count": 0 if args.dry_run else len(events),
        "fetched_event_count": len(events),
        "event_schema_version": "market_event_v1",
        "source": args.source,
        "symbol": "BTCUSDT",
        "market": "USDT-M",
        "timeframe": args.timeframe,
        "event_time_start_utc": events[0]["event_time"],
        "event_time_end_utc": events[-1]["event_time"],
    }
    write_report(Path(args.report_path), report)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
