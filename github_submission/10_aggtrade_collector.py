import argparse
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd


TRADE_CONTEXT_SCHEMA_VERSION = "aggtrade_1m_v2_from_id"


def parse_args():
    parser = argparse.ArgumentParser(description="Collect Binance USDT-M aggregated trades and build 1-minute trade features.")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--start-datetime", help="UTC ISO time, e.g. 2024-01-01T00:00")
    parser.add_argument("--end-datetime", help="UTC ISO time, exclusive")
    parser.add_argument("--recent-minutes", type=int, help="Collect this many minutes ending at Binance server time.")
    parser.add_argument("--context-folder", default="trade_context_store_v2")
    parser.add_argument("--max-pages", type=int, default=500, help="Safety cap for 1,000-trade API pages.")
    return parser.parse_args()


def parse_utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def safe_value(value):
    return str(value).replace("/", "").replace(":", "").replace(" ", "")


def exchange_symbol(symbol):
    return symbol if ":" in symbol else f"{symbol}:USDT"


def taker_side(trade):
    side = trade.get("side")
    if side in {"buy", "sell"}:
        return side
    buyer_is_maker = trade.get("info", {}).get("m")
    if buyer_is_maker is True:
        return "sell"
    return "buy"


def add_trade(aggregates, trade):
    timestamp = int(trade["timestamp"])
    minute = timestamp - (timestamp % 60_000)
    price = float(trade["price"])
    amount = float(trade["amount"])
    if price <= 0 or amount <= 0:
        return

    record = aggregates[minute]
    notional = price * amount
    side = taker_side(trade)
    record["trade_count"] += 1
    record["trade_volume"] += amount
    record["trade_notional"] += notional
    record["first_price"] = price if record["first_price"] is None else record["first_price"]
    record["last_price"] = price
    if side == "buy":
        record["taker_buy_volume"] += amount
        record["taker_buy_notional"] += notional
    else:
        record["taker_sell_volume"] += amount
        record["taker_sell_notional"] += notional


def collect_trades(exchange, symbol, start_ms, end_ms, max_pages):
    aggregates = defaultdict(
        lambda: {
            "trade_count": 0,
            "trade_volume": 0.0,
            "trade_notional": 0.0,
            "taker_buy_volume": 0.0,
            "taker_sell_volume": 0.0,
            "taker_buy_notional": 0.0,
            "taker_sell_notional": 0.0,
            "first_price": None,
            "last_price": None,
        }
    )
    cursor = start_ms
    next_from_id = None
    seen_trade_ids = set()
    pages = 0
    total_trades = 0

    while cursor < end_ms:
        if pages >= max_pages:
            raise RuntimeError(
                f"--max-pages={max_pages}에 도달했습니다. 구간을 더 작게 나누거나 값을 높이세요. "
                "불완전한 체결 데이터는 저장하지 않습니다."
            )
        try:
            params = {"fromId": next_from_id} if next_from_id is not None else {}
            batch = exchange.fetch_trades(
                symbol,
                since=cursor if next_from_id is None else None,
                limit=1000,
                params=params,
            )
        except ccxt.ExchangeError as error:
            if "-4166" in str(error) or "recent 2 days" in str(error):
                raise RuntimeError(
                    "Binance USDT-M aggTrade 공개 API는 최근 2일만 과거 검색을 지원합니다. "
                    "5년 체결 백필에는 외부 과거 데이터 공급자 또는 지금부터의 자체 실시간 적재가 필요합니다."
                ) from error
            raise
        pages += 1
        if not batch:
            break

        latest_timestamp = cursor - 1
        numeric_trade_ids = []
        for trade in batch:
            timestamp = int(trade["timestamp"])
            latest_timestamp = max(latest_timestamp, timestamp)
            trade_id = trade.get("id")
            if trade_id is not None:
                try:
                    numeric_trade_ids.append(int(trade_id))
                except (TypeError, ValueError):
                    pass
            dedupe_key = (
                f"id:{trade_id}"
                if trade_id is not None
                else f"fallback:{timestamp}:{trade.get('price')}:{trade.get('amount')}:{trade.get('side')}"
            )
            if dedupe_key in seen_trade_ids:
                continue
            seen_trade_ids.add(dedupe_key)
            if start_ms <= timestamp < end_ms:
                add_trade(aggregates, trade)
                total_trades += 1

        if latest_timestamp >= end_ms:
            cursor = end_ms
            break
        if numeric_trade_ids:
            candidate_from_id = max(numeric_trade_ids) + 1
            if next_from_id is not None and candidate_from_id <= next_from_id:
                break
            next_from_id = candidate_from_id
            cursor = max(cursor, latest_timestamp)
        elif latest_timestamp >= cursor:
            cursor = latest_timestamp + 1
            next_from_id = None
        else:
            break
        time.sleep(exchange.rateLimit / 1000)

    if cursor < end_ms:
        raise RuntimeError(
            "요청 구간 끝까지 체결 데이터를 받지 못했습니다. "
            "이 API의 과거 접근 범위 또는 페이지 제한을 확인해야 합니다."
        )
    return aggregates, pages, total_trades


def build_context(aggregates, symbol):
    records = []
    for timestamp, item in sorted(aggregates.items()):
        volume = item["trade_volume"]
        records.append(
            {
                "timestamp": timestamp,
                "datetime_utc": pd.to_datetime(timestamp, unit="ms", utc=True),
                "symbol": symbol,
                "market": "usdm",
                "timeframe": "1m",
                "context_schema_version": TRADE_CONTEXT_SCHEMA_VERSION,
                "trade_count": item["trade_count"],
                "trade_volume": volume,
                "trade_notional": item["trade_notional"],
                "trade_vwap": item["trade_notional"] / volume if volume else None,
                "trade_first_price": item["first_price"],
                "trade_last_price": item["last_price"],
                "taker_buy_volume": item["taker_buy_volume"],
                "taker_sell_volume": item["taker_sell_volume"],
                "taker_buy_notional": item["taker_buy_notional"],
                "taker_sell_notional": item["taker_sell_notional"],
                "taker_volume_imbalance": (item["taker_buy_volume"] - item["taker_sell_volume"]) / volume if volume else 0.0,
            }
        )
    return pd.DataFrame(records)


def write_context(context, args, start_dt, end_dt, pages, total_trades):
    run_id = f"{safe_value(args.symbol)}_USDM_AGGTRADE_1m_{start_dt:%Y%m%dT%H%M}_{end_dt:%Y%m%dT%H%M}"
    output_dir = Path(args.context_folder) / "market=usdm" / f"symbol={safe_value(args.symbol)}" / "timeframe=1m"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"trade_context_{run_id}.parquet"
    context.to_parquet(output_path, index=False)
    verified = pd.read_parquet(output_path)
    if len(verified) != len(context):
        raise RuntimeError(f"체결 컨텍스트 저장 검증 실패: expected={len(context)}, actual={len(verified)}")

    marker_dir = Path(args.context_folder) / "_markers"
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker_path = marker_dir / f"_SUCCESS_{run_id}.json"
    marker = {
        "run_id": run_id,
        "symbol": safe_value(args.symbol),
        "market": "usdm",
        "timeframe": "1m",
        "rows": len(context),
        "api_pages": pages,
        "pagination": "startTime_then_aggregate_trade_fromId",
        "context_schema_version": TRADE_CONTEXT_SCHEMA_VERSION,
        "aggregated_trades": total_trades,
        "context_file": str(output_path),
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    marker_path.write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path, marker_path


def main():
    args = parse_args()
    if args.recent_minutes is not None:
        if args.start_datetime or args.end_datetime:
            raise ValueError("--recent-minutes는 --start-datetime/--end-datetime과 함께 사용할 수 없습니다.")
        if args.recent_minutes <= 0:
            raise ValueError("--recent-minutes는 1 이상이어야 합니다.")
        exchange = ccxt.binanceusdm({"enableRateLimit": True})
        end_dt = datetime.fromtimestamp(exchange.fetch_time() / 1000, tz=timezone.utc)
        start_dt = end_dt - pd.Timedelta(minutes=args.recent_minutes)
    else:
        if not args.start_datetime or not args.end_datetime:
            raise ValueError("--start-datetime/--end-datetime 또는 --recent-minutes가 필요합니다.")
        start_dt = parse_utc(args.start_datetime)
        end_dt = parse_utc(args.end_datetime)
        exchange = ccxt.binanceusdm({"enableRateLimit": True})
    if end_dt <= start_dt:
        raise ValueError("--end-datetime은 --start-datetime보다 뒤여야 합니다.")
    if args.max_pages <= 0:
        raise ValueError("--max-pages는 1 이상이어야 합니다.")

    market_symbol = exchange_symbol(args.symbol)
    print(f"aggTrade 수집 시작: {market_symbol} {start_dt.isoformat()} ~ {end_dt.isoformat()}")
    aggregates, pages, total_trades = collect_trades(
        exchange,
        market_symbol,
        int(start_dt.timestamp() * 1000),
        int(end_dt.timestamp() * 1000),
        args.max_pages,
    )
    context = build_context(aggregates, args.symbol)
    if context.empty:
        raise RuntimeError("집계된 체결 데이터가 없습니다.")
    output_path, marker_path = write_context(context, args, start_dt, end_dt, pages, total_trades)
    print(f"API pages: {pages:,}, aggregated trades: {total_trades:,}, minute rows: {len(context):,}")
    print(f"체결 컨텍스트 저장 완료: {output_path}")
    print(f"성공 마커: {marker_path}")


if __name__ == "__main__":
    main()
