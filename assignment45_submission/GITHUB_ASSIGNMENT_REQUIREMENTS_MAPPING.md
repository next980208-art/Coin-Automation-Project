# 4·5차시 과제 요구사항과 프로젝트 실행 결과

## 1. 결론

두 과제 모두 현재 자동매매 데이터 프로젝트의 실제 구성에 맞춰 실행을 마쳤습니다.

실제 실행 상태를 이미지로 확인하려면
[`GITHUB_ASSIGNMENT_SCREENSHOT_EVIDENCE.md`](GITHUB_ASSIGNMENT_SCREENSHOT_EVIDENCE.md)를 먼저 봅니다.
Airflow Run ID, Flink Job ID, Kafka 처리 건수와 Parquet 품질 결과를 원본 JSON과 함께 대조할 수
있습니다.

| 과제 | 적용한 프로젝트 경로 | 상태 |
| --- | --- | --- |
| 4차시 Airflow 자동화 | Binance REST -> Airflow -> PyFlink -> Parquet | 필수 항목 완료 |
| 5차시 부하·장애·복구 | 저장 이벤트 -> Kafka -> PyFlink -> Parquet | 필수 항목 완료 |

Spark는 이 프로젝트의 처리 엔진이 아니므로 과제 안내의 대체 조건에 따라 Apache Flink PyFlink를
사용했습니다. 실거래 주문 API는 연결하지 않았습니다.

## 2. 4차시: Airflow 자동화

### 요구 1. 기존 수집·처리 코드를 Airflow DAG로 실행

충족했습니다.

```text
airflow/dags/btcusdt_usdm_historical_backfill.py
```

실제 흐름:

```text
Airflow 입력값 검사
  -> Binance USDT-M 선물 문맥 수집
  -> OHLCV 수집
  -> PyFlink 피처 계산
  -> Parquet 저장
  -> 중복·공백·필수 값 검사
```

### 요구 2. 코드를 고치지 않고 입력값 변경

`dag_run.conf` 또는 Airflow `params`로 다음 값을 받습니다.

| 입력값 | 예시 | 의미 |
| --- | --- | --- |
| `symbol` | `ETH/USDT` | 수집 종목 |
| `start_date` | `2026-08-20` | 시작일 UTC, 포함 |
| `end_date` | `2026-08-21` | 종료일 UTC, 미포함 |
| `chunk_days` | `1` | 청크 크기 |
| `timeframe` | `1m` | 봉 단위 |

실제 Trigger 입력:

```json
{
  "symbol": "ETH/USDT",
  "start_date": "2026-08-20",
  "end_date": "2026-08-21",
  "chunk_days": 1
}
```

### 요구 3. 값을 바꿔 다시 실행

기본 BTCUSDT 대신 실제 Binance ETHUSDT USDT-M 하루 데이터를 실행했습니다.

| 결과 | 값 |
| --- | ---: |
| OHLCV 수집 | 1,440행 |
| Feature Store 저장 | 1,440행 |
| Futures Context Store 저장 | 1,440행 |
| timestamp 중복 | 0건 |
| 1분 공백 | 0건 |
| 품질 상태 | `healthy=true` |
| PyFlink Job ID | `c94bb7c92ad132f5237ad24bff3f0481` |

### 요구 4. 코드·로그·결과 제출

| 증거 | 파일 |
| --- | --- |
| DAG 코드 | `airflow/dags/btcusdt_usdm_historical_backfill.py` |
| 구현 설명 | `docs/airflow_parameterized_backfill_assignment_2026-08-26.md` |
| 실제 실행 결과 | `docs/airflow_parameterized_backfill_run_2026-08-26.json` |
| GitHub 발표 문서 | `GITHUB_AIRFLOW_ASSIGNMENT_PRESENTATION.md` |

대용량 Parquet 대신 Job ID, 건수, 경로, 품질 결과가 담긴 작은 JSON을 제출합니다.

## 3. 5차시: 부하·장애·복구

### 실험 대상

```text
저장된 실제 Binance BTCUSDT 이벤트
  -> 로컬 Kafka
  -> Consumer 중복 제거
  -> PyFlink Batch
  -> Parquet Feature Store
```

외부 Binance에는 부하를 보내지 않았습니다. 실제 저장 이벤트를 로컬에서 재생했습니다.

### 요구 1. 현재 입력량과 정상 결과 기록

| 항목 | 정상 실행 결과 |
| --- | ---: |
| Producer 전송 | 1,000건 |
| Consumer 고유 수신 | 1,000건 |
| Kafka 구간 | 3.172초 |
| PyFlink 입력·출력 | 1,000행 -> 1,000행 |
| PyFlink 처리 | 8.297초 |
| 전체 파이프라인 | 11.563초 |
| 저장 중복·결측 | 0건·0건 |

### 요구 2. 더 많은 데이터 실행

| 항목 | 부하 실행 결과 |
| --- | ---: |
| Producer 총 전송 | 10,500건 |
| 고유 이벤트 | 10,000건 |
| 의도적 중복 | 500건 |
| Consumer 중복 감지 | 500건 |
| Kafka 구간 | 4.437초 |
| PyFlink 입력·출력 | 10,000행 -> 10,000행 |
| PyFlink 처리 | 8.922초 |
| 전체 파이프라인 | 13.547초 |
| 저장 중복·결측 | 0건·0건 |

10,000건은 실제 저장 이벤트를 로컬에서 반복 확장한 재생 데이터이며, 10,000건을 외부 API에서
새로 요청한 것이 아닙니다.

### 요구 3. 장애를 안전하게 재현

두 가지를 실제로 실행했습니다.

1. 같은 `event_id` 500건을 중복 전송했습니다.
2. 필수 `close` 필드를 제거한 잘못된 입력을 전달했습니다.

중복 이벤트는 Consumer에서 정확히 500건으로 감지됐습니다. 잘못된 입력은 다음 오류와 종료 코드
1로 안전하게 실패했으며 오염된 결과는 저장하지 않았습니다.

```text
RuntimeError: Flink input preparation produced no valid events.
```

추가로 이전 실시간 실험에서는 TaskManager를 재시작해 checkpoint 복구도 검증했습니다.

### 요구 4. 복구 후 누락·중복 검사

검증된 정상 입력으로 재실행한 결과 1,000행이 다시 저장됐고 중복은 0건이었습니다. 부하 결과
Parquet도 10,000행, 고유 timestamp 10,000개, 중복 0건, 필수 값 결측 0건입니다.

최종 자동 판정:

```json
{
  "errors": [],
  "healthy": true
}
```

### 5차시 제출 증거

| 증거 | 파일 |
| --- | --- |
| 실행·발표 설명 | `assignment5_pipeline_resilience/README.md` |
| 전체 자동 실행 코드 | `assignment5_pipeline_resilience/run_experiment.py` |
| 전체 실제 결과 | `assignment5_pipeline_resilience/results/assignment5_final_report.json` |
| Parquet 재검증 | `assignment5_pipeline_resilience/results/assignment5_output_quality_check.json` |
| 단계별 수치 | `assignment5_pipeline_resilience/results/*_producer.json`, `*_consumer.json`, `*_flink.json` |
| 장애 로그 | `assignment5_pipeline_resilience/logs/fault_invalid_input.log` |
| 이전 Flink 재시작 증거 | `docs/realtime_checkpoint_restart_verification_2026-08-29.md` |
| 실제 실행 증거 이미지 | `GITHUB_ASSIGNMENT_SCREENSHOT_EVIDENCE.md` |

## 4. 실행 명령

4차시 Airflow는 Docker 실행 후 Airflow UI에서 DAG를 Trigger합니다.

```powershell
docker compose up -d airflow jobmanager taskmanager
```

5차시 전체 실험:

```powershell
docker compose up -d zookeeper kafka jobmanager taskmanager airflow
python assignment5_pipeline_resilience/run_experiment.py
```

## 5. GitHub 포함·제외

포함:

```text
Airflow DAG와 관련 Python 코드
GITHUB_ASSIGNMENT_REQUIREMENTS_MAPPING.md
GITHUB_AIRFLOW_ASSIGNMENT_PRESENTATION.md
assignment5_pipeline_resilience/README.md
assignment5_pipeline_resilience/run_experiment.py
assignment5_pipeline_resilience/results/*.json
assignment5_pipeline_resilience/logs/*.log
작은 최종 Parquet과 성공 마커
```

제외:

```text
재생 중간 JSONL·CSV
전체 5년 원천 데이터
Docker·Kafka·Airflow 런타임 파일
API key, 거래소 계정 정보, 개인정보
```

## 6. 발표 핵심 문장

```text
4차시에는 기존 Binance 수집과 PyFlink 가공을 Airflow DAG로 연결하고 symbol과 날짜를 입력값으로
받게 만들었습니다. BTC 대신 ETHUSDT 하루를 실행해 1,440행을 수집·가공·저장했고 중복과 1분
공백은 0건이었습니다.

5차시에는 저장된 실제 BTCUSDT 이벤트를 외부가 아닌 로컬 Kafka에 재생했습니다. 정상 1,000건과
부하 10,000건을 비교하고 중복 500건과 필수 필드 누락 장애를 재현했습니다. 중복은 정확히 제거됐고
잘못된 입력은 저장 전에 실패했으며, 복구 후 Parquet 건수·중복·결측 검사는 모두 정상입니다.
```
