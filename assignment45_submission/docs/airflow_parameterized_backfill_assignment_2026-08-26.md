# Airflow 입력값 기반 과거 데이터 백필 실행

작성일: 2026-08-26  
과제 적용 대상: Binance USDT-M 선물 시장 데이터 수집·처리 DAG

## 1. 과제 요구와 적용 내용

기존의 `btcusdt_usdm_historical_backfill` DAG를 수정해, DAG 코드를 다시 고치지 않고
실행 시 입력값만 바꿔 다른 종목과 다른 날짜 범위를 처리할 수 있게 했습니다.

| 과제 요구 | 구현 |
| --- | --- |
| 입력값 1개 이상 수신 | `symbol`, `start_date`, `end_date`, `chunk_days` 입력 지원 |
| 값을 바꿔 재실행 | `ETH/USDT`, 2026-08-20 ~ 2026-08-21로 실제 실행 |
| 기존 처리 연결 | 선물 맥락 수집 -> OHLCV 수집 -> PyFlink Batch -> Parquet -> 품질 검사 |
| 실행 로그·결과 제출 | 작은 실행 결과 JSON과 이 문서에 기록. 대용량 원본·Parquet는 제외 |

## 2. DAG 코드 위치와 입력 방식

DAG 코드: `airflow/dags/btcusdt_usdm_historical_backfill.py`

기본 자동 백필은 기존처럼 성공 마커를 기준으로 과거 방향 다음 청크를 처리합니다. 다만 Trigger
실행에서 `start_date`와 `end_date`를 **둘 다** 입력하면 `manual_range` 모드가 선택됩니다.
이 모드는 자동 백필 진행 위치를 바꾸지 않고, 입력한 정확한 날짜 범위만 한 번 처리합니다.

지원 입력값:

| 입력값 | 예시 | 의미 |
| --- | --- | --- |
| `symbol` | `ETH/USDT` | Binance USDT-M 선물 종목 |
| `start_date` | `2026-08-20` | 수동 백필 시작일 UTC, 포함 |
| `end_date` | `2026-08-21` | 수동 백필 종료일 UTC, 미포함 |
| `chunk_days` | `1` | 한 번에 처리할 날짜 청크 크기 |
| `timeframe` | `1m` | 봉 단위. 기본값은 1분 |

`start_date`와 `end_date` 중 하나만 입력하면 DAG는 명확한 오류를 내고 실행하지 않습니다. 종목은
`BTC/USDT`, `ETH/USDT`처럼 `BASE/QUOTE` 형식인지 검사하며, 이 DAG는 `usdm` 시장만 허용합니다.

## 3. 처리 순서

```text
Airflow Trigger JSON
  -> plan_next_backfill
  -> run_next_backfill
       -> 9_futures_context_collector.py
       -> backfill_runner.py
            -> 1_chunk_downloader.py
            -> flink_batch_submitter.py / PyFlink Batch
  -> verify_feature_store
  -> verify_futures_context_store
```

처리 전 OHLCV 원천 CSV 행 수와 처리 후 Feature Store 행 수를 각각 확인합니다. 저장 검증과 성공
마커 작성이 끝난 뒤에만 원천 CSV를 삭제합니다.

## 4. 입력값을 바꾼 실제 실행

실행 입력값:

```json
{
  "symbol": "ETH/USDT",
  "start_date": "2026-08-20",
  "end_date": "2026-08-21",
  "chunk_days": 1
}
```

실행 ID: `manual__2026-08-26T14:00:00+00:00`  
실행 방법: `airflow dags test`  
계획 결과: `mode=manual_range`, `source=dag_run.conf`

실행 로그 요약:

```text
선물 컨텍스트 수집: ETH/USDT:USDT, mark price 1,440행
OHLCV 원천 수집: ETH/USDT 1분봉 1,440행
PyFlink Batch 완료: job_id=c94bb7c92ad132f5237ad24bff3f0481, rows=1,440
Feature Store 품질 검사: duplicate=0, 1분 공백=0, healthy=true
Futures Context Store 품질 검사: duplicate=0, 1분 공백=0, healthy=true
```

네 Task 모두 성공했습니다.

```text
plan_next_backfill
  -> run_next_backfill
  -> verify_feature_store
  -> verify_futures_context_store
```

세부 실행 결과는 [airflow_parameterized_backfill_run_2026-08-26.json](airflow_parameterized_backfill_run_2026-08-26.json)에
작은 JSON으로 남겼습니다.

## 5. 만들어진 결과

| 결과 | 경로 | 형식 | 행 수 |
| --- | --- | --- | ---: |
| OHLCV 기본 피처 | `feature_store_v2/market=usdm/symbol=ETHUSDT/timeframe=1m/year=2026/month=08/` | Parquet | 1,440 |
| 선물 맥락 | `futures_context_store_v2/market=usdm/symbol=ETHUSDT/timeframe=1m/` | Parquet | 1,440 |
| OHLCV 성공 마커 | `feature_store_v2/_markers/_SUCCESS_ETHUSDT_USDM_1m_20260820_20260821.json` | JSON | 1개 |
| 선물 맥락 성공 마커 | `futures_context_store_v2/_markers/_SUCCESS_ETHUSDT_USDM_CONTEXT_1m_20260820_20260821.json` | JSON | 1개 |

OHLCV Feature Store의 주요 최종 컬럼은 다음과 같습니다.

```text
timestamp, datetime_utc, open, high, low, close, volume,
ma_5, return_1m, symbol, market, timeframe, feature_schema_version
```

선물 맥락 Store에는 `mark_price`, `funding_rate`, `open_interest`와 각 데이터의 수집 가능 여부를
기록하는 상태 컬럼이 추가됩니다. 공개 API가 제공하지 않는 open interest 과거 값은 0으로 만들지 않고
null과 상태 사유로 보존합니다.

## 6. 다시 실행하는 방법

### Airflow 화면

1. `http://localhost:8080` 접속
2. `btcusdt_usdm_historical_backfill` DAG 선택
3. 오른쪽 위 Trigger DAG 선택
4. Configuration JSON에 아래 값을 입력 후 실행

```json
{
  "symbol": "ETH/USDT",
  "start_date": "2026-08-20",
  "end_date": "2026-08-21",
  "chunk_days": 1
}
```

### PowerShell에서 전체 DAG 테스트

```powershell
docker compose exec -T airflow airflow dags test btcusdt_usdm_historical_backfill 2026-08-26T14:00:00+00:00 `
  --conf '{\"symbol\":\"ETH/USDT\",\"start_date\":\"2026-08-20\",\"end_date\":\"2026-08-21\",\"chunk_days\":1}'
```

PowerShell에서는 JSON 내부 큰따옴표 앞에 `\`를 붙여야 Docker 컨테이너의 Airflow CLI까지
정상 전달됩니다. 화면에서 실행할 때는 일반 JSON을 그대로 입력하면 됩니다.

## 7. GitHub 제출 범위

올릴 파일:

```text
airflow/dags/btcusdt_usdm_historical_backfill.py
backfill_runner.py
1_chunk_downloader.py
flink_batch_submitter.py
flink_jobs/batch_feature_job.py
9_futures_context_collector.py
scripts/verify_feature_store.py
scripts/verify_futures_context_store.py
docs/airflow_parameterized_backfill_assignment_2026-08-26.md
docs/airflow_parameterized_backfill_run_2026-08-26.json
```

제외할 파일:

```text
temp_raw_data_v3/의 원천 CSV
feature_store_v2/와 futures_context_store_v2/의 대용량 Parquet
runtime_reports/의 누적 실행 파일
API key, 계정 정보, 주문 권한 설정
```

이 실행은 공개 Binance 시장 데이터만 사용했으며 실제 주문 API를 호출하지 않았습니다.
