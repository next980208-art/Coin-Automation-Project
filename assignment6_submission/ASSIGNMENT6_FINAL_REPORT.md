# 6차시 최종 보고서

## 부하·복구 결과 보완 및 전체 데이터 흐름 점검

## 1. 프로젝트 개요

이 프로젝트는 Binance USDT-M 선물 시장 데이터를 수집하고, 머신러닝이 사용할 수 있는
피처로 가공한 뒤 Parquet Feature Store에 저장하는 데이터 파이프라인입니다. 최종 목표는
자동매매이지만, 현재 성능이 승인된 모델이 없으므로 실제 주문은 차단되어 있습니다.

이번 과제에서는 이미 실행한 Kafka·PyFlink 부하 및 장애 복구 실험을 바탕으로 다음 항목을
보완했습니다.

1. 기준 실행과 부하 실행의 건수·시간·처리량·저장 결과 비교
2. 실패 단계와 재실행 시작 위치 확인
3. 실제 입력 오류에 대한 Alert와 Fallback 결과 확인
4. Airflow부터 Parquet 저장까지 전체 흐름 점검
5. 최신 구성도와 데이터 모델 정리
6. 아직 실행되지 않는 단계와 남은 작업 구분

과제 문구에는 Spark가 포함되어 있지만, 이 프로젝트는 처음부터 Apache Flink를 표준 처리
엔진으로 사용했습니다. 따라서 Spark 단계는 **Apache Flink의 Python API인 PyFlink**로
처리했습니다.

## 2. 실험 대상과 데이터 출처

실험 원본은 Binance USDT-M에서 수집해 저장해 둔 실제 BTCUSDT 1분봉 1,000건입니다.

```text
pipeline_code/assignment4_kafka_spark/data/consumed_binance_usdm_events.jsonl
```

외부 Binance API에 부하를 보내지 않기 위해 1,000건을 로컬에서 재생했습니다. 부하 실행에서는
timestamp와 event_id가 겹치지 않도록 데이터를 확장해 고유 이벤트 10,000건을 만들었습니다.
중복 처리도 확인하기 위해 기존 event_id 500건을 추가로 전송했습니다.

```text
기준 실행: 실제 가격 패턴 기반 고유 이벤트 1,000건
부하 실행: 실제 가격 패턴 기반 고유 이벤트 10,000건 + 의도적 중복 500건
외부 서비스에 보낸 부하: 0건
```

## 3. 전체 데이터 흐름

```mermaid
flowchart LR
    SOURCE["저장된 실제 Binance<br/>BTCUSDT 1분봉"]
    PRODUCER["Kafka Producer<br/>event_id / run_id 부여"]
    KAFKA["Kafka Topic<br/>assignment5.market.events.v1"]
    CONSUMER["Consumer<br/>run_id 선택 / event_id 중복 제거"]
    VALIDATE{"필수 OHLCV<br/>입력 검사"}
    ALERT["로컬 Alert<br/>JSON + 로그"]
    FALLBACK["검증된 JSONL<br/>Fallback"]
    FLINK["PyFlink Batch<br/>정제 / ma_5 / return_1m"]
    PARQUET["Parquet<br/>Feature Store"]
    QUALITY["행 수 / 중복 / 결측<br/>최종 품질 검사"]

    SOURCE --> PRODUCER --> KAFKA --> CONSUMER --> VALIDATE
    VALIDATE -->|"정상"| FLINK --> PARQUET --> QUALITY
    VALIDATE -.->|"close 누락"| ALERT --> FALLBACK --> FLINK
```

### 각 도구를 사용한 이유

| 도구 | 사용 이유 |
| --- | --- |
| Airflow | 종목과 날짜를 입력받아 수집·가공·검증 작업을 정해진 순서로 실행하기 위해 사용 |
| Kafka | Producer와 Consumer의 처리 속도를 분리하고 이벤트를 다시 읽을 수 있도록 사용 |
| PyFlink | 프로젝트의 배치·실시간 피처 계산 방식을 통일하기 위해 사용 |
| Parquet | 컬럼 기반 압축 형식으로 저장해 분석과 머신러닝에서 필요한 컬럼만 빠르게 읽기 위해 사용 |
| JSON 보고서 | 단계별 처리 건수와 실행 상태를 프로그램으로 다시 검증하기 위해 사용 |
| event_id | 동일한 시장 이벤트가 반복 전송됐는지 판단하기 위해 사용 |
| run_id | 기준 실행과 부하 실행처럼 서로 다른 실행을 구분하기 위해 사용 |

## 4. 기준 실행과 부하 실행 결과

원본 실험 실행 시각은 **2026-08-31 10:47 KST**입니다.

| 측정 항목 | 기준 실행 | 부하 실행 |
| --- | ---: | ---: |
| 실행 이름 | `baseline_1000` | `load_10000` |
| 고유 입력 건수 | 1,000 | 10,000 |
| Producer 총 전송 | 1,000 | 10,500 |
| 의도적으로 추가한 중복 | 0 | 500 |
| Consumer가 확인한 전체 메시지 | 1,000 | 10,500 |
| Consumer 고유 수신 | 1,000 | 10,000 |
| Consumer가 제거한 중복 | 0 | 500 |
| PyFlink 입력 | 1,000 | 10,000 |
| PyFlink 출력 | 1,000 | 10,000 |
| 최종 Parquet 저장 | 1,000 | 10,000 |
| 예상하지 못한 미처리 | 0 | 0 |
| Kafka 구간 시간 | 3.094초 | 4.422초 |
| PyFlink 구간 시간 | 7.953초 | 8.266초 |
| 전체 파이프라인 시간 | 11.141초 | 12.875초 |
| 최종 처리량 | 89.759행/초 | 776.699행/초 |
| timestamp 중복 | 0 | 0 |
| 필수값 결측 | 0 | 0 |
| 오류 | 0 | 0 |

### 결과 해석

- 고유 입력량은 10배 증가했습니다.
- 전체 실행 시간은 1.156배 증가했습니다.
- 최종 처리량은 8.653배 증가했습니다.
- 부하 실행에서 전송과 저장의 차이인 500건은 유실이 아니라 의도한 중복 제거 결과입니다.
- 고유 이벤트 10,000건은 Consumer, PyFlink, Parquet 단계에서 모두 동일하게 유지됐습니다.

이번 결과는 현재 설정에서 10,000건까지 정상 처리했다는 의미입니다. Kafka가 실패하는 최대
한계를 찾은 실험은 아니므로 무제한 처리할 수 있다는 뜻으로 해석하지 않습니다.

## 5. 실행 화면 확인

### 5.1 Airflow 파라미터 입력

![Airflow 파라미터 입력](evidence/02_trigger_form_solusdt_json.png)

Airflow DAG 코드를 수정하지 않고 `symbol`, `start_date`, `end_date` 값을 입력할 수 있습니다.
실제 검증에서는 `SOL/USDT`와 날짜 범위를 입력해 다시 실행했습니다.

### 5.2 Airflow 작업 성공

![Airflow 성공 Graph](evidence/04_dag_run_success_graph.png)

수집, PyFlink 가공, 피처 품질 검사와 선물 문맥 품질 검사가 순서대로 성공한 화면입니다.

![Airflow 실제 실행 설정](evidence/06_dag_run_configuration_details.png)

실제 DAG Run에 전달된 종목과 날짜 JSON을 확인할 수 있습니다.

### 5.3 Flink 완료 작업

![Flink 완료 작업 목록](evidence/02_flink_completed_jobs.png)

기준, 부하, 복구 작업이 모두 `FINISHED` 상태로 표시됐습니다.

![기준 실행 1000건](evidence/03_flink_baseline_1000_job.png)

기준 Job ID는 `6a0f8949d2108b0117ebd55378600701`이고 Records Received는
1,000입니다.

![부하 실행 10000건](evidence/04_flink_load_10000_job.png)

부하 Job ID는 `08e3fad0007754619f5878d65e91b3e3`이고 Records Received는
10,000입니다.

## 6. 장애 발생과 실패 위치

### 장애 조건

이벤트에서 필수 필드인 `close`를 제거했습니다.

```text
장애 종류: missing_close_field
실패 위치: Consumer JSONL을 PyFlink 입력 CSV로 변환하기 전 입력 검사
종료 코드: 1
오류: RuntimeError: Flink input preparation produced no valid events.
```

잘못된 입력을 억지로 저장하지 않고 Flink Job 제출 전에 중단했습니다. 따라서 오염된 Parquet이
생성되지 않았고 Flink 화면에 실패 Job이 남지 않는 것이 정상입니다.

실제 오류는 다음 파일에 저장했습니다.

```text
logs/fault_invalid_input.log
```

## 7. Alert와 Fallback 결과

입력 검증 실패를 실제 실행 JSON에서 확인해 로컬 Alert 파일과 로그를 생성했습니다.

```text
Alert 코드: REQUIRED_FIELD_MISSING
Alert 발생: true
외부 Slack·이메일 전송: 사용하지 않음
Alert 저장: 로컬 JSON 및 로그
```

Fallback은 Kafka부터 전체 데이터를 다시 보내는 대신 마지막으로 검증된 기준 JSONL부터
PyFlink 입력 준비를 재실행하도록 구성했습니다.

```text
검증된 baseline JSONL
  -> PyFlink 입력 준비
  -> 복구 PyFlink Job
  -> recovery_1000 Parquet
```

| 복구 항목 | 결과 |
| --- | ---: |
| 복구 Job ID | `10a25bbb76ecde40b0b1106aabd34e4a` |
| 기대 저장 | 1,000행 |
| 실제 저장 | 1,000행 |
| timestamp 중복 | 0건 |
| 필수값 결측 | 0건 |
| Fallback 성공 | `true` |
| 최종 상태 | `resolved` |

![Alert와 Fallback 실제 JSON 화면](evidence/07_alert_fallback_actual_result.png)

위 이미지는 `results/assignment6_alert_and_fallback.json`을 로컬 서버로 열어 캡처한 실제
Chrome 화면입니다. Alert 발생, 복구 건수, 중복·결측 검사와 최종 해결 상태를 한 화면에서
확인할 수 있습니다.

![복구 Flink Job 1000건](evidence/05_flink_recovery_1000_job.png)

복구 작업도 Records Received 1,000과 `FINISHED` 상태를 확인했습니다.

## 8. 최종 저장 데이터 모델

저장 형식은 Parquet이고 다음과 같이 시장·종목·시간 단위·연·월로 나눕니다.

```text
output/<실행명>/market=usdm/symbol=BTCUSDT/timeframe=1m/year=2026/month=08/*.parquet
```

| 컬럼 | 타입 | 의미 |
| --- | --- | --- |
| `timestamp` | int64 | 1분봉 UTC millisecond 시각 |
| `datetime_utc` | timestamp | 사람이 읽을 수 있는 UTC 시각 |
| `symbol`, `market`, `timeframe` | string | 종목·시장·시간 단위 |
| `run_id` | string | 파이프라인 실행 식별자 |
| `open`, `high`, `low`, `close` | double | 1분 OHLC 가격 |
| `volume` | double | 1분 거래량 |
| `ma_5` | double | 최근 5개 종가 이동평균 |
| `return_1m` | double | 직전 1분 대비 수익률 |
| `feature_schema_version` | string | 피처 스키마 버전 |
| `event_time_ms` | int64 | 원천 이벤트 발생 시각 |
| `timestamp_unit` | string | timestamp 단위 |
| `metadata_schema_version` | string | 시장 메타데이터 계약 버전 |

전체 이벤트 모델과 상세 구성도는 `ARCHITECTURE_AND_DATA_MODEL.md`에 분리해 작성했습니다.

## 9. 단계별 결과 확인 방법

| 확인할 내용 | 파일 | 핵심 값 |
| --- | --- | --- |
| 통합 비교 | `results/assignment6_pipeline_review.json` | `validation.healthy=true` |
| Alert·Fallback | `results/assignment6_alert_and_fallback.json` | `triggered=true`, `succeeded=true` |
| Producer 건수 | `source_results/*_producer.json` | `producer_sent_count` |
| Consumer·중복 | `source_results/*_consumer.json` | `consumer_received_count`, `duplicate_message_count` |
| PyFlink 처리·저장 | `source_results/*_flink.json` | `flink_input_valid_count`, `flink_output_processed_count` |
| Parquet 품질 | `source_results/assignment5_output_quality_check.json` | 행 수·중복·결측 |
| 실제 실패 | `logs/fault_invalid_input.log` | 종료 코드와 RuntimeError |
| Alert 로그 | `logs/assignment6_alert.log` | 실패 단계와 복구 상태 |

Parquet은 바이너리 파일이므로 텍스트 편집기로 열지 않고 다음처럼 확인합니다.

```powershell
python -c "import pandas as pd; print(pd.read_parquet('파일경로.parquet').head())"
```

## 10. 현재 재검증 방법

기존 실제 결과로 6차시 통합 보고서와 Alert·Fallback 판정을 다시 만들 수 있습니다.

```powershell
python assignment6_submission/scripts/build_assignment6_report.py
```

이 명령은 외부 Binance나 Kafka에 새 부하를 보내지 않습니다. 결과가 정상이라면 다음 값이
생성됩니다.

```text
assignment6_pipeline_review.json
  validation.errors = []
  validation.healthy = true

assignment6_alert_and_fallback.json
  alert.triggered = true
  fallback.succeeded = true
  final_status = resolved
```

전체 부하 실험을 다시 실행해야 할 때만 프로젝트 루트에서 다음 명령을 사용합니다.

```powershell
docker compose up -d zookeeper kafka jobmanager taskmanager airflow
python assignment45_submission/assignment5_pipeline_resilience/run_experiment.py
```

## 11. 아직 실행되지 않는 단계

| 단계 | 현재 상태 | 남은 작업 |
| --- | --- | --- |
| 승인 모델 실시간 추론 | `no_trade` | 비용 후 양의 기대수익 모델 승인 |
| 장기 Paper Trading | 운영 전 | 실제 spread·funding·부분 체결을 반영해 수 주간 실행 |
| 모델 자동 교체 | 미연결 | Registry manifest, 사람 승인, rollback 구현 |
| 거래소 Testnet | 미실행 | reduce-only stop, 주문 재시도, 상태 reconciliation |
| 실제 자금 주문 | 차단 | Testnet과 장기 Paper 기준을 통과한 뒤 별도 승인 |
| 외부 Alert | 미연결 | 필요 시 Slack·이메일·모니터링 시스템 연결 |
| 최대 부하 한계 | 미측정 | 단계적으로 건수를 늘리고 CPU·메모리·Kafka lag 수집 |

BI, API, inference 웹 화면은 이번 과제에서 새로 추가하지 않았습니다. 현재 승인된 수익 모델도
없으므로 실제 예측이 동작하는 것처럼 보이는 예시는 제출하지 않았습니다.

## 12. 결론

이번 점검에서 정상 1,000건과 부하 10,000건의 고유 이벤트가 Kafka, Consumer, PyFlink,
Parquet 단계에서 빠짐없이 유지되는 것을 확인했습니다. 의도적 중복 500건은 정확히 제거됐고,
예상하지 못한 미처리는 0건이었습니다.

필수 필드가 누락된 데이터는 Flink 제출 전에 차단됐습니다. 이후 검증된 JSONL에서 재시작해
1,000행을 중복과 결측 없이 복구했고, Alert와 Fallback 결과는 최종 `resolved`로 기록됐습니다.

따라서 현재 데이터 파이프라인은 **정상 처리, 부하 처리, 잘못된 입력 차단, 중복 제거, 검증된
지점부터 복구, 최종 저장 품질 확인**까지 연결되어 있습니다. 다만 이 결과는 데이터 파이프라인의
안정성을 확인한 것이며, 머신러닝 수익성과 실거래 안전성을 증명한 결과는 아닙니다. 실거래 단계는
계속 차단 상태로 유지합니다.

## 13. 제출 범위

GitHub에는 `assignment6_submission` 폴더 전체를 업로드합니다. 다음 항목은 포함하지 않았습니다.

- API key와 계정 정보
- 개인정보
- 5년 대용량 원천 데이터
- 실제 거래소 주문 기능
- 외부 서비스에 부하를 보내는 코드
