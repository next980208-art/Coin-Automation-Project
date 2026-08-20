import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd


FUTURES_CONTEXT_SCHEMA_VERSION = "usdm_context_v2_complete_mark"


def parse_args():
    parser = argparse.ArgumentParser(description="Collect Binance USDT-M funding, open interest, and mark-price context.")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeframe", default="1m", help="Mark-price OHLCV timeframe.")
    parser.add_argument("--open-interest-timeframe", default="5m")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, inclusive UTC")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD, exclusive UTC")
    parser.add_argument("--context-folder", default="futures_context_store_v2")
    return parser.parse_args()


def utc_midnight(date_text):
    return datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def safe_value(value):
    return str(value).replace("/", "").replace(":", "").replace(" ", "")


def exchange_symbol(symbol):
    return symbol if ":" in symbol else f"{symbol}:USDT"


def fetch_paginated(fetch_page, start_ms, end_ms, rate_limit):
    rows_by_timestamp = {}
    cursor = start_ms
    while cursor < end_ms:
        batch = fetch_page(cursor)
        if not batch:
            break

        timestamps = []
        for row in batch:
            timestamp = int(row[0] if isinstance(row, (list, tuple)) else row["timestamp"])
            timestamps.append(timestamp)
            if start_ms <= timestamp < end_ms:
                rows_by_timestamp[timestamp] = row

        next_cursor = max(timestamps) + 1 if timestamps else end_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time.sleep(rate_limit / 1000)

    return [rows_by_timestamp[key] for key in sorted(rows_by_timestamp)]


def funding_frame(rows):
    records = []
    for row in rows:
        records.append(
            {
                "funding_rate_timestamp": int(row["timestamp"]),
                "funding_rate": pd.to_numeric(row.get("fundingRate"), errors="coerce"),
            }
        )
    return pd.DataFrame(records, columns=["funding_rate_timestamp", "funding_rate"])


def open_interest_frame(rows):
    records = []
    for row in rows:
        records.append(
            {
                "open_interest_timestamp": int(row["timestamp"]),
                "open_interest": pd.to_numeric(row.get("openInterestAmount"), errors="coerce"),
                "open_interest_value": pd.to_numeric(row.get("openInterestValue"), errors="coerce"),
            }
        )
    return pd.DataFrame(
        records,
        columns=["open_interest_timestamp", "open_interest", "open_interest_value"],
    )


def mark_frame(rows):
    frame = pd.DataFrame(rows, columns=["timestamp", "mark_open", "mark_high", "mark_low", "mark_price", "mark_volume"])
    return frame[["timestamp", "mark_open", "mark_high", "mark_low", "mark_price"]]


def merge_context(mark_prices, funding_rates, open_interest, symbol, timeframe, funding_status, open_interest_status):
    context = mark_prices.sort_values("timestamp").reset_index(drop=True)
    if not funding_rates.empty:
        context = pd.merge_asof(
            context,
            funding_rates.sort_values("funding_rate_timestamp"),
            left_on="timestamp",
            right_on="funding_rate_timestamp",
            direction="backward",
        )
    else:
        context["funding_rate_timestamp"] = pd.NA
        context["funding_rate"] = pd.NA

    if not open_interest.empty:
        context = pd.merge_asof(
            context,
            open_interest.sort_values("open_interest_timestamp"),
            left_on="timestamp",
            right_on="open_interest_timestamp",
            direction="backward",
        )
    else:
        context["open_interest_timestamp"] = pd.NA
        context["open_interest"] = pd.NA
        context["open_interest_value"] = pd.NA

    context["datetime_utc"] = pd.to_datetime(context["timestamp"], unit="ms", utc=True)
    context["symbol"] = symbol
    context["market"] = "usdm"
    context["timeframe"] = timeframe
    context["context_schema_version"] = FUTURES_CONTEXT_SCHEMA_VERSION
    context["funding_rate_status"] = funding_status
    context["open_interest_status"] = open_interest_status
    return context


def write_context(context, context_folder, symbol, timeframe, start_dt, end_dt):
    run_id = f"{safe_value(symbol)}_USDM_CONTEXT_{safe_value(timeframe)}_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}"
    root = Path(context_folder) / "market=usdm" / f"symbol={safe_value(symbol)}" / f"timeframe={safe_value(timeframe)}"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / f"context_{run_id}.parquet"
    context.to_parquet(output_path, index=False)

    verified = pd.read_parquet(output_path)
    if len(verified) != len(context):
        raise RuntimeError(f"선물 컨텍스트 저장 검증 실패: expected={len(context)}, actual={len(verified)}")
    if verified["mark_price"].isna().any():
        raise RuntimeError("선물 컨텍스트 저장 검증 실패: mark_price 결측치가 있습니다.")

    marker_dir = Path(context_folder) / "_markers"
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker_path = marker_dir / f"_SUCCESS_{run_id}.json"
    marker = {
        "run_id": run_id,
        "symbol": safe_value(symbol),
        "market": "usdm",
        "timeframe": timeframe,
        "rows": len(context),
        "context_schema_version": FUTURES_CONTEXT_SCHEMA_VERSION,
        "funding_rate_status": str(context["funding_rate_status"].iloc[0]),
        "open_interest_status": str(context["open_interest_status"].iloc[0]),
        "context_file": str(output_path),
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    marker_path.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path, marker_path


def fetch_optional(name, fetcher):
    try:
        return fetcher(), "available"
    except ccxt.BaseError as error:
        status = f"unavailable: {type(error).__name__}: {error}"
        print(f"경고: {name} 수집 불가. 빈 값으로 저장합니다. {status}")
        return [], status


def main():
    args = parse_args()
    start_dt = utc_midnight(args.start_date)
    end_dt = utc_midnight(args.end_date)
    if end_dt <= start_dt:
        raise ValueError("--end-date는 --start-date보다 뒤여야 합니다.")

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    market_symbol = exchange_symbol(args.symbol)
    exchange = ccxt.binanceusdm({"enableRateLimit": True})

    print(f"선물 컨텍스트 수집 시작: {market_symbol} {start_dt:%Y-%m-%d} ~ {end_dt:%Y-%m-%d}")
    mark_rows = fetch_paginated(
        lambda since: exchange.fetch_mark_ohlcv(market_symbol, args.timeframe, since=since, limit=1000),
        start_ms,
        end_ms,
        exchange.rateLimit,
    )
    funding_rows, funding_status = fetch_optional(
        "funding rate",
        lambda: fetch_paginated(
            lambda since: exchange.fetch_funding_rate_history(market_symbol, since=since, limit=1000),
            start_ms,
            end_ms,
            exchange.rateLimit,
        ),
    )
    open_interest_rows, open_interest_status = fetch_optional(
        "open interest",
        lambda: fetch_paginated(
            lambda since: exchange.fetch_open_interest_history(
                market_symbol,
                args.open_interest_timeframe,
                since=since,
                limit=500,
            ),
            start_ms,
            end_ms,
            exchange.rateLimit,
        ),
    )

    if not mark_rows:
        raise RuntimeError("mark price OHLCV 데이터를 가져오지 못했습니다.")

    step_ms = int(exchange.parse_timeframe(args.timeframe) * 1000)
    expected_timestamps = set(range(start_ms, end_ms, step_ms))
    actual_timestamps = {int(row[0]) for row in mark_rows}
    if actual_timestamps != expected_timestamps:
        missing = len(expected_timestamps - actual_timestamps)
        unexpected = len(actual_timestamps - expected_timestamps)
        raise RuntimeError(
            "mark price OHLCV 구간이 완전하지 않습니다. "
            f"expected={len(expected_timestamps):,}, actual={len(actual_timestamps):,}, "
            f"missing={missing:,}, unexpected={unexpected:,}"
        )

    context = merge_context(
        mark_frame(mark_rows),
        funding_frame(funding_rows),
        open_interest_frame(open_interest_rows),
        args.symbol,
        args.timeframe,
        funding_status,
        open_interest_status,
    )
    output_path, marker_path = write_context(context, args.context_folder, args.symbol, args.timeframe, start_dt, end_dt)

    print(f"mark price rows: {len(mark_rows):,}")
    print(f"funding rate rows: {len(funding_rows):,}")
    print(f"open interest rows: {len(open_interest_rows):,}")
    print(f"선물 컨텍스트 저장 완료: {output_path} ({len(context):,} rows)")
    print(f"성공 마커: {marker_path}")


if __name__ == "__main__":
    main()
