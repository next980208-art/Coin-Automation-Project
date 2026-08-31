# Kafka와 Apache Flink 실제 데이터 과제 모듈

## 1. 이 모듈이 하는 일

이 폴더는 자동매매 프로젝트의 데이터 수집·가공 구간을 작은 규모로 검증한 모듈입니다.
BTCUSDT USDT-M 1분봉 이벤트 1,000건을 Kafka로 보내고, Apache Flink(PyFlink)로
가공한 뒤 Parquet Feature Store에 저장합니다.

> **제출 증거 기준:** 이 문서의 실제 실행 결과와 `results/*_binance_live.json`은
> Binance USDT-M 공개 API에서 가져온 실제 완료 1분봉 1,000건의 실행 기록입니다.
> 아래의 테스트 모드는 네트워크 없이 같은 흐름을 다시 확인하기 위한 보조 기능입니다.

```text
Binance 공개 API 또는 테스트 이벤트
    -> Kafka Topic: btc_market_events_v1
    -> Kafka Consumer 원본 JSONL 저장
    -> 검증·중복 제거·시간 정렬
    -> Apache Flink: ma_5, return_1m 생성
    -> Parquet Feature Store 저장
```

전체 자동매매 아키텍처에서는 `시장 데이터 수집 -> Kafka -> Flink -> Feature Store`까지의
구간입니다. 라벨 생성, ML 학습, 백테스트, 실시간 주문은 이 모듈의 다음 단계입니다.

제출 범위의 아키텍처 그림 코드는 [architecture_mermaid.md](architecture_mermaid.md)에 있습니다.

## 2. 처리 엔진 선택: Apache Flink

이 프로젝트의 데이터 처리 엔진은 Spark가 아니라 **Apache Flink(PyFlink)** 입니다.
과제용으로 별도 엔진을 추가하지 않고, 실제 프로젝트에서 사용할 `Kafka -> Flink ->
Parquet Feature Store` 경로를 그대로 작은 규모로 검증했습니다.

Flink를 선택한 이유는 현재의 과거 데이터 배치 가공을 처리할 수 있고, 이후 WebSocket
실시간 데이터까지 같은 피처 계산 규칙으로 확장할 수 있기 때문입니다. 따라서 아래의
전처리·저장 결과는 Spark가 아닌 Flink 배치 작업의 실제 실행 결과입니다.

## 3. 전송 데이터 명세

Kafka Topic: `btc_market_events_v1`

| 필드 | 타입 | 의미 |
| --- | --- | --- |
| `schema_version` | string | 메시지 구조 버전. 현재 `market_event_v1` |
| `run_id` | string | 한 번의 실행을 구분하는 ID |
| `event_id` | string | 이벤트별 고유 ID. 중복 제거에 사용 |
| `event_sequence` | integer | 이벤트 순서 |
| `event_time` | string | UTC 기준 1분봉 시각 |
| `symbol` | string | 현재 `BTCUSDT` |
| `market` | string | 현재 `USDT-M` |
| `open`, `high`, `low`, `close` | double | 1분봉 OHLC 가격 |
| `volume` | double | 거래량 |
| `source` | string | `assignment4_deterministic_test` 또는 `binance_usdm_public_ohlcv` |

## 4. 가공 규칙과 최종 데이터

입력 준비 단계는 OHLCV 가격 관계와 거래량을 검사하고, 중복 `event_id`를 제거하며,
시간 순서대로 정렬합니다. 통과한 데이터만 Flink 입력 CSV가 됩니다.

Flink는 원본 OHLCV에 아래 피처를 추가합니다.

| 피처 | 계산 | 의미 |
| --- | --- | --- |
| `ma_5` | 최근 최대 5개 종가 평균 | 짧은 가격 흐름 |
| `return_1m` | `현재 종가 / 직전 종가 - 1` | 1분 가격 변화율 |

최종 Parquet 컬럼은 아래 14개입니다.

```text
timestamp, datetime_utc, symbol, market, timeframe, run_id,
open, high, low, close, volume, ma_5, return_1m,
feature_schema_version
```

## 5. 실행 전 준비

Docker Desktop을 켜고 프로젝트 루트 PowerShell에서 실행합니다.

```powershell
python -m pip install -r requirements.txt
docker compose --profile streaming up -d zookeeper kafka
docker compose up -d flink-staging-init jobmanager taskmanager airflow
```

## 6. 재현용 테스트 데이터 1,000건 실행

이 절은 네트워크 연결 없이 같은 Kafka·Flink 흐름을 다시 확인하는 용도입니다.
제출 증거로 사용하는 실제 Binance 실행은 다음 `7. 실제 Binance 1분봉 1,000건 실행`입니다.

```powershell
python assignment4_kafka_spark/kafka_market_event_producer.py --count 1000 --run-id assignment4-demo-v1
python assignment4_kafka_spark/kafka_market_event_consumer.py --expected-count 1000 --run-id assignment4-demo-v1
python assignment4_kafka_spark/prepare_flink_input.py
docker compose exec -T -u 0 airflow python /opt/airflow/project/flink_batch_submitter.py --raw-file /opt/airflow/project/assignment4_kafka_spark/data/flink_input.csv --feature-folder /opt/airflow/project/assignment4_kafka_spark/output/flink_feature_store --keep-raw
```

## 7. 실제 Binance 1분봉 1,000건 실행

`--source binance-usdm`은 API 키 없이 Binance USDT-M 공개 API에서 **완료된**
BTCUSDT 1분봉을 가져옵니다. 진행 중인 현재 1분봉은 제외하고, 시간 누락이나 중복이
있으면 중단합니다.

```powershell
$runId = "binance-usdm-" + (Get-Date -Format "yyyyMMdd-HHmmss")

python assignment4_kafka_spark/kafka_market_event_producer.py --source binance-usdm --count 1000 --run-id $runId --report-path assignment4_kafka_spark/results/producer_binance_live.json

python assignment4_kafka_spark/kafka_market_event_consumer.py --run-id $runId --expected-count 1000 --output-path assignment4_kafka_spark/data/consumed_binance_usdm_events.jsonl --report-path assignment4_kafka_spark/results/consumer_binance_live.json

python assignment4_kafka_spark/prepare_flink_input.py --input-path assignment4_kafka_spark/data/consumed_binance_usdm_events.jsonl --output-path assignment4_kafka_spark/data/flink_input_binance_usdm.csv --report-path assignment4_kafka_spark/results/flink_input_binance_live.json

docker compose exec -T -u 0 airflow python /opt/airflow/project/flink_batch_submitter.py --raw-file /opt/airflow/project/assignment4_kafka_spark/data/flink_input_binance_usdm.csv --feature-folder /opt/airflow/project/assignment4_kafka_spark/output/flink_feature_store_binance_usdm --keep-raw

python assignment4_kafka_spark/write_flink_report.py --input-report assignment4_kafka_spark/results/flink_input_binance_live.json --marker-path "assignment4_kafka_spark/output/flink_feature_store_binance_usdm/_markers/_SUCCESS_$runId.json" --project-root . --report-path assignment4_kafka_spark/results/flink_report_binance_live.json
```

실제 API 수집과 시간 연속성만 먼저 점검하려면 Producer 명령에 `--dry-run`을 추가합니다.

## 8. 제출에 사용하는 실제 Binance 실행 결과

2026-08-22에 실제 Binance USDT-M BTCUSDT 1분봉으로 실행한 결과입니다.

| 확인 항목 | 결과 |
| --- | ---: |
| Producer 전송 | 1,000건 |
| Consumer 수신 | 1,000건 |
| 입력 검증 통과 | 1,000건 |
| 잘못된 OHLCV·중복 이벤트 | 0건 |
| Flink Parquet 저장 | 1,000건 |
| Flink Job ID | `2271ebc304ea99a6c6dbbaac1d13dc57` |

증거 파일은 아래 JSON과 성공 마커입니다.

```text
results/producer_binance_live.json
results/consumer_binance_live.json
results/flink_input_binance_live.json
results/flink_report_binance_live.json
output/flink_feature_store_binance_usdm/_markers/_SUCCESS_binance-usdm-20260822-live.json
```

Parquet는 텍스트 파일이 아니라 바이너리 형식이므로 편집기에서 직접 열리지 않는 것이
정상입니다. 실제 첫 10행과 피처 값은 `flink_report_binance_live.json`의 경로를 읽어
확인할 수 있으며, 필요하면 아래 명령으로 확인합니다.

```powershell
python -c "import json; from pathlib import Path; import pandas as pd; r=json.loads(Path('assignment4_kafka_spark/results/flink_report_binance_live.json').read_text(encoding='utf-8')); print(pd.read_parquet(r['feature_files'][0]).head(10).to_string(index=False))"
```

Flink 실행 환경은 `http://localhost:8081`, Airflow는 `http://localhost:8080`에서 볼 수
있습니다. 완료된 Flink 배치 Job은 Dashboard 보존 설정에 따라 목록에서 사라질 수 있으므로,
최종 성공 여부는 위 JSON 보고서와 `_SUCCESS` 마커로 판단합니다.

## 9. 요구 항목과 증거 파일

| 제출 시 설명할 항목 | 이 프로젝트에서 한 일 | 확인 파일 또는 위치 |
| --- | --- | --- |
| 데이터·메시지 명세 | BTCUSDT USDT-M 1분봉 OHLCV 메시지 정의 | 이 문서의 `3. 전송 데이터 명세` |
| Kafka 이벤트 1,000건 | Producer 전송과 Consumer 수신을 각각 1,000건 확인 | `results/producer_binance_live.json`, `results/consumer_binance_live.json` |
| 전처리·저장 | 검증, 중복 제거, 시간 정렬 후 Flink가 `ma_5`, `return_1m` 생성 | `results/flink_input_binance_live.json`, `results/flink_report_binance_live.json` |
| 최종 저장 | 피처 1,000건을 Parquet로 저장 | `output/flink_feature_store_binance_usdm/`와 `_SUCCESS` 마커 |
| 실제 구현과 이후 계획 | 현재 범위와 미구현 실시간 경로를 분리해 기록 | 이 문서의 `10. 구현 범위와 다음 단계` |

실행 결과를 요약하면 Binance USDT-M BTCUSDT 닫힌 1분봉 1,000건을 Kafka로 전송하고
Consumer 수신 건수를 확인한 뒤, 동일한 OHLCV 데이터를 Flink 배치 작업으로 전처리해
Parquet Feature Store에 저장했습니다.

## 10. 저장 위치와 GitHub 제출

| 단계 | 위치 | 형식 |
| --- | --- | --- |
| Kafka 수신 원본 | `assignment4_kafka_spark/data/` | JSON Lines |
| Flink 입력 | `assignment4_kafka_spark/data/` | CSV |
| 피처 저장소 | `assignment4_kafka_spark/output/flink_feature_store*` | Parquet |
| 실행 증거 | `assignment4_kafka_spark/results/` | JSON |

원본 JSONL과 Parquet는 GitHub에 올리지 않습니다. 아래 명령은 코드·README·작은 JSON
보고서만 `github_submission`에 모읍니다.

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\prepare_github_submission.ps1 -Replace
```

## 11. 구현 범위와 다음 단계

현재 구현은 완료된 1분봉의 배치 수집과 Kafka·Flink 가공입니다. 실시간 WebSocket 체결·호가창,
Flink Streaming, 모델 실시간 추론, 모의 주문, 실거래 주문은 아직 구현하지 않았습니다.

다음 단계는 수정된 Feature Store v2 기준으로 라벨 생성, ML 데이터셋 생성, 시간 순서 기반
모델 검증, 비용과 거래당 계좌 손실 2% 한도를 반영한 백테스트를 다시 실행하는 것입니다.

