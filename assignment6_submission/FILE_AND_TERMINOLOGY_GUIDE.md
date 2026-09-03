# 6차시 제출 파일 및 어려운 용어 해설서

## 1. 이 문서의 목적

이 문서는 `assignment6_submission` 폴더에 들어 있는 파일을 하나씩 이해하고, 문서와 코드에
나오는 어려운 데이터 엔지니어링 용어를 쉬운 말로 확인하기 위한 보조 해설서입니다.

최종 보고서에는 결과와 증거가 중심으로 들어 있습니다. 이 해설서에는 다음 내용을 더 자세히
담았습니다.

- 어떤 파일부터 읽어야 하는지
- 각 파일이 왜 필요한지
- 파일 안에서 무엇을 확인해야 하는지
- Kafka, Producer, Consumer, Flink, Airflow가 각각 무엇인지
- 1,000건과 10,000건 실험의 숫자가 어떻게 연결되는지
- 장애가 어디에서 발생했고 어떤 파일로 복구를 증명하는지
- 현재 구현된 것과 아직 구현되지 않은 것을 어떻게 구분하는지

이 문서는 새로운 실행 결과를 만드는 프로그램이 아닙니다. 실제 결과의 기준은 JSON, 로그,
Parquet 결과와 실행 화면이며, 이 문서는 그 증거를 읽는 방법을 설명합니다.

---

## 2. 가장 먼저 알아둘 전체 흐름

이번 제출에서 점검한 데이터 흐름은 다음과 같습니다.

```text
저장된 실제 Binance BTCUSDT 1분봉
  -> Producer가 Kafka Topic에 전송
  -> Kafka가 메시지를 보관
  -> Consumer가 메시지를 읽고 event_id 중복 제거
  -> 필수 필드와 데이터 타입 검사
  -> PyFlink가 ma_5와 return_1m 계산
  -> Parquet 파일로 저장
  -> 행 수, 중복, 결측값 검사
```

장애 복구 흐름은 다음과 같습니다.

```text
close 필드를 제거한 잘못된 입력
  -> Flink 제출 전 입력 검사에서 차단
  -> 종료 코드 1과 오류 로그 기록
  -> 로컬 Alert JSON·로그 생성
  -> 마지막으로 검증된 JSONL을 Fallback으로 선택
  -> Flink 입력 준비 단계부터 재실행
  -> Parquet 1,000행 복구
  -> 중복 0건, 필수값 결측 0건 확인
```

중요한 점은 Kafka 안에 Producer와 Consumer 프로그램이 들어 있는 것이 아니라는 것입니다.

```text
Producer 프로그램 -> Kafka -> Consumer 프로그램
```

Kafka는 메시지를 받아 Topic에 보관합니다. Producer는 Kafka에 보내는 외부 프로그램이고,
Consumer는 Kafka에서 읽는 외부 프로그램입니다. PyFlink처럼 어떤 프로그램은 앞 Topic에서는
Consumer이고, 처리 결과를 다음 Topic에 보낼 때는 Producer 역할도 할 수 있습니다.

---

## 3. 권장 읽기 순서

### 1단계: 전체 결과 이해

1. `ASSIGNMENT6_FINAL_REPORT.md`
2. `README.md`
3. `ARCHITECTURE_AND_DATA_MODEL.md`

### 2단계: 실제 숫자 확인

4. `results/assignment6_pipeline_review.json`
5. `results/assignment6_alert_and_fallback.json`
6. `source_results/`의 단계별 JSON

### 3단계: 장애와 화면 증거 확인

7. `logs/fault_invalid_input.log`
8. `logs/assignment6_alert.log`
9. `evidence/`의 PNG 파일

### 4단계: 구현 방식 확인

10. `scripts/build_assignment6_report.py`
11. `pipeline_code/assignment5_pipeline_resilience/run_experiment.py`
12. `pipeline_code/assignment4_kafka_spark/`의 Producer, Consumer, Flink 입력 코드
13. `pipeline_code/airflow/dags/btcusdt_usdm_historical_backfill.py`

처음부터 코드를 읽으면 파일이 많아 흐름을 놓치기 쉽습니다. 최종 결과를 먼저 확인하고, 그
결과가 어떤 JSON과 코드에서 나왔는지 거꾸로 내려가는 순서가 이해하기 쉽습니다.

---

## 4. 루트 문서 설명

### 4.1 `ASSIGNMENT6_FINAL_REPORT.md`

가장 중요한 최종 제출 보고서입니다.

주요 내용:

- 프로젝트와 과제의 목적
- 기준 1,000건과 부하 10,000건 비교
- 중복 500건의 의미
- 잘못된 입력 장애 재현
- Alert와 Fallback 결과
- Airflow와 Flink 실제 화면
- 단계별 결과 확인 방법
- 아직 실행하지 않은 단계

이 파일에서 먼저 확인할 숫자:

| 항목 | 값 | 의미 |
| --- | ---: | --- |
| 기준 고유 입력 | 1,000건 | 정상 상태를 측정한 작은 실행 |
| 부하 고유 입력 | 10,000건 | 입력량을 10배 늘린 실행 |
| 부하 Producer 전송 | 10,500건 | 고유 10,000건과 의도적 중복 500건 |
| Consumer 중복 제거 | 500건 | 같은 event_id를 가진 메시지를 제거한 수 |
| 최종 Parquet | 10,000행 | 고유 입력이 유실 없이 저장된 결과 |
| 예상 밖 미처리 | 0건 | 고유 데이터 기준 누락이 없다는 의미 |

보고서가 길어도 다음 네 절은 반드시 확인하는 것이 좋습니다.

```text
4절: 기준과 부하 비교
5절: 실패 단계와 재실행 위치
6절: Alert와 Fallback
10절: 아직 실행되지 않는 단계
```

### 4.2 `README.md`

제출 폴더의 사용 설명서입니다. 최종 보고서보다 짧고 실행 방법과 파일 위치를 빠르게 찾는 데
사용합니다.

주요 역할:

- 제출 내용 요약
- 먼저 확인할 파일 안내
- 결과 확인 경로
- 보고서 재생성 명령
- GitHub에 올릴 범위

다음 명령은 새로운 외부 부하 실험을 실행하지 않습니다. 기존 실제 결과 JSON을 다시 읽어 6차시
통합 보고서를 재생성합니다.

```powershell
python assignment6_submission/scripts/build_assignment6_report.py
```

### 4.3 `ARCHITECTURE_AND_DATA_MODEL.md`

프로그램이 어떤 순서로 연결되는지와 각 단계의 데이터 컬럼을 설명하는 문서입니다.

구성:

- 과거 데이터 배치 경로
- 6차시 로컬 부하·복구 경로
- 실시간 경로
- 구성 요소별 입력·처리·출력
- Kafka JSON 이벤트 모델
- Parquet 피처 데이터 모델

Mermaid 코드의 색상 의미:

| 색상 | 의미 |
| --- | --- |
| 초록색 | 코드가 있고 실제 실행으로 검증한 단계 |
| 파란색 | 데이터를 저장하는 위치 |
| 노란색 | 기능은 있으나 운영 검증이 더 필요한 단계 |
| 빨간색 | 안전을 위해 의도적으로 차단한 단계 |

구성도에서 화살표는 데이터 또는 작업 제어가 이동하는 방향입니다. 실선은 실제 연결 경로,
점선은 실패·승인·차단처럼 조건부 경로를 나타냅니다.

### 4.4 `FILE_AND_TERMINOLOGY_GUIDE.md`

현재 읽고 있는 파일입니다. 제출 결과를 추가로 만들지 않고 파일과 용어를 설명합니다.

---

## 5. `results` 폴더 설명

### 5.1 `results/assignment6_pipeline_review.json`

기준 실행, 부하 실행, 장애와 복구 결과를 한 JSON에 합친 통합 결과입니다.

주요 최상위 필드:

| 필드 | 뜻 |
| --- | --- |
| `source_data` | 실험에 사용한 데이터의 출처 |
| `processing_engine` | 실제 사용한 처리 엔진 |
| `pipeline` | 데이터가 이동한 순서 |
| `baseline_and_load` | 기준·부하 실행의 상세 수치 |
| `comparison` | 두 실행의 배수 비교 |
| `failure_and_restart` | 실패와 재실행 정보 |
| `validation` | 전체 결과가 건강한지 판단한 결과 |

`baseline_and_load`에서 확인할 필드:

| 필드 | 쉬운 뜻 |
| --- | --- |
| `producer_sent_count` | Producer가 Kafka로 보낸 전체 메시지 수 |
| `expected_unique_count` | 중복을 제외하고 남아야 할 고유 이벤트 수 |
| `consumer_unique_count` | Consumer가 실제로 확인한 고유 이벤트 수 |
| `intentional_duplicate_count` | 실험을 위해 일부러 추가한 중복 수 |
| `consumer_detected_duplicate_count` | Consumer가 실제로 찾아낸 중복 수 |
| `flink_input_count` | PyFlink가 받은 유효 입력 행 수 |
| `final_parquet_count` | 최종 Parquet에 저장된 행 수 |
| `unexpected_unprocessed_count` | 이유 없이 사라진 고유 이벤트 수 |
| `total_pipeline_seconds` | 전체 단계에 걸린 시간 |
| `final_rows_per_second` | 1초 동안 최종 저장까지 끝난 행 수 |
| `healthy` | 해당 실행이 품질 기준을 통과했는지 여부 |

부하 실행의 건수 관계는 다음과 같습니다.

```text
Producer 전체 전송 10,500
  = 고유 이벤트 10,000
  + 의도적 중복 500

Consumer 고유 수신 10,000
  = Producer 전체 10,500
  - 중복 제거 500

최종 Parquet 10,000
  = Consumer 고유 수신 10,000
  - 예상 밖 미처리 0
```

따라서 10,500건에서 10,000건으로 줄어든 것은 유실이 아닙니다. event_id가 같은 중복 500건을
의도대로 제거한 결과입니다.

### 5.2 `results/assignment6_alert_and_fallback.json`

장애를 발견한 결과와 복구 결과를 한 파일에서 확인합니다.

`alert` 주요 필드:

| 필드 | 뜻 |
| --- | --- |
| `triggered` | 오류 감지 조건이 실제로 충족됐는지 |
| `severity` | 오류 심각도 |
| `code` | 프로그램이 사용하는 오류 분류 코드 |
| `failed_stage` | 실패가 발생한 단계 |
| `process_return_code` | 실패 프로그램의 종료 코드 |
| `delivery` | Alert를 어디에 기록했는지 |
| `external_notification_configured` | Slack·이메일 같은 외부 알림 연결 여부 |

`fallback` 주요 필드:

| 필드 | 뜻 |
| --- | --- |
| `triggered` | 대체 입력을 사용한 복구가 시작됐는지 |
| `strategy` | 어떤 데이터를 사용해 어디부터 재개했는지 |
| `kafka_replay_required` | Kafka부터 다시 전송할 필요가 있었는지 |
| `expected_rows` | 복구 후 기대하는 행 수 |
| `stored_rows` | 실제 복구 저장 행 수 |
| `duplicate_timestamps` | 복구 후 timestamp 중복 수 |
| `missing_required_values` | 복구 후 필수값 결측 수 |
| `succeeded` | 복구 기준 통과 여부 |

`final_status=resolved`는 장애가 없었다는 뜻이 아닙니다. 장애가 실제 발생했고, 감지·차단·복구·
재검증까지 끝났다는 뜻입니다.

---

## 6. `source_results` 폴더 설명

`results`가 여러 실행을 합친 요약이라면 `source_results`는 각 단계에서 직접 나온 원본 결과
사본입니다. 통합 숫자가 어디에서 왔는지 추적할 때 사용합니다.

### 6.1 기준 실행 파일

| 파일 | 역할 |
| --- | --- |
| `baseline_1000_producer.json` | 기준 1,000건 Producer 전송 결과 |
| `baseline_1000_consumer.json` | 기준 Consumer 수신·중복 제거 결과 |
| `baseline_1000_flink.json` | 기준 PyFlink 입력·출력·Job 결과 |

세 파일에서 다음 관계를 확인합니다.

```text
Producer 전송 1,000
= Consumer 고유 수신 1,000
= Flink 유효 입력 1,000
= Flink 처리 출력 1,000
= Parquet 저장 1,000
```

### 6.2 부하 실행 파일

| 파일 | 역할 |
| --- | --- |
| `load_10000_producer.json` | 10,000개 고유 이벤트와 중복 500건 전송 |
| `load_10000_consumer.json` | 10,500건에서 중복 500건 제거 확인 |
| `load_10000_flink.json` | 고유 10,000행 처리·저장 결과 |

파일명은 `load_10000`이지만 Producer는 중복까지 포함해 10,500건을 보냅니다. `10000`은 최종적으로
남아야 할 고유 이벤트 수를 나타냅니다.

### 6.3 복구와 품질 파일

| 파일 | 역할 |
| --- | --- |
| `recovery_1000_flink.json` | 검증된 Fallback 입력으로 복구한 Flink Job 결과 |
| `assignment5_output_quality_check.json` | 기준·부하·복구 Parquet 품질 검사 |
| `assignment5_final_report.json` | 5차시 실험 전체 원본 요약 |

`recovery_1000_flink.json`의 Job ID는 Flink 화면의 복구 Job과 연결됩니다.

```text
10a25bbb76ecde40b0b1106aabd34e4a
```

### 6.4 `output_samples` 최종 Parquet 파일

JSON에 기록된 저장 건수를 GitHub에서도 직접 확인할 수 있도록 실제 실행에서 생성된 최종 파일
3개를 포함했습니다.

| 파일 | 실제 행 수 | 의미 |
| --- | ---: | --- |
| `output_samples/baseline_1000.parquet` | 1,000 | 기준 실행 결과 |
| `output_samples/load_10000.parquet` | 10,000 | 중복 제거 후 부하 실행 결과 |
| `output_samples/recovery_1000.parquet` | 1,000 | 검증된 입력으로 복구한 결과 |

이 파일들은 VS Code 텍스트 편집기로 열지 않고 pandas, DuckDB 또는 Parquet Viewer로 확인합니다.
기준 파일과 복구 파일이 바이트 단위로 같은 것은 동일한 검증 입력을 같은 피처 로직으로 다시
처리해 결정적으로 같은 결과를 얻었기 때문입니다.

---

## 7. `logs` 폴더 설명

### 7.1 `logs/fault_invalid_input.log`

필수 `close` 필드를 제거한 입력으로 장애를 재현했을 때의 실제 오류 로그입니다.

확인할 내용:

- 유효 입력을 만들지 못했다는 `RuntimeError`
- 실패가 Flink Job 제출 전에 발생했다는 위치
- 실제로 실행한 명령과 경과 시간

종료 코드 1은 `results/assignment6_alert_and_fallback.json`과 다음
`logs/assignment6_alert.log`에서 확인합니다. 종료 코드 0은 정상 종료, 0이 아닌 값은 오류 종료를
나타내는 것이 일반적입니다.

### 7.2 `logs/assignment6_alert.log`

원본 오류와 복구 결과를 사람이 읽기 쉬운 형태로 요약한 로컬 Alert 로그입니다.

이 로그는 외부 Slack이나 이메일이 아닙니다. 로컬 파일에 Alert 정보를 남긴 구현이며,
`results/assignment6_alert_and_fallback.json`과 같은 상태를 설명합니다.

---

## 8. `evidence` 폴더 설명

### 8.1 Airflow 관련 화면

| 파일 | 확인 내용 |
| --- | --- |
| `02_trigger_form_solusdt_json.png` | 코드를 바꾸지 않고 symbol·날짜를 입력하는 Trigger 화면 |
| `04_dag_run_success_graph.png` | DAG의 각 작업이 성공한 Graph 화면 |
| `06_dag_run_configuration_details.png` | 실제 DAG Run에 전달된 입력 JSON |

Trigger 화면은 코드를 수정하는 창이 아닙니다. 이미 작성된 DAG에 이번 실행에서 사용할 매개변수만
전달하는 화면입니다.

### 8.2 Flink 관련 화면

| 파일 | 확인 내용 |
| --- | --- |
| `01_flink_overview_after_experiment.png` | Task Slot과 완료 Job이 있는 Flink 전체 상태 |
| `02_flink_completed_jobs.png` | 완료된 Flink Job 목록 |
| `03_flink_baseline_1000_job.png` | 기준 1,000건 Job 상세 |
| `04_flink_load_10000_job.png` | 부하 10,000건 Job 상세 |
| `05_flink_recovery_1000_job.png` | 복구 1,000건 Job 상세 |

Flink 화면에서 `FINISHED`는 Job이 정상 종료됐다는 뜻입니다. 데이터 품질까지 완전하다는 의미는
아니므로 JSON의 행 수와 Parquet 품질 검사도 함께 확인해야 합니다.

### 8.3 Alert 관련 화면

| 파일 | 확인 내용 |
| --- | --- |
| `07_alert_fallback_actual_result.png` | 로컬 JSON의 Alert 발생과 Fallback 성공 결과 |

이 화면은 결과 JSON을 로컬 브라우저에서 연 모습입니다. 외부 모니터링 서비스 화면은 아닙니다.

---

## 9. `scripts` 폴더 설명

### 9.1 `scripts/build_assignment6_report.py`

기존 5차시 원본 결과를 읽어 6차시 요구사항에 맞는 통합 JSON과 Alert 로그를 생성합니다.

이 코드가 확인하는 핵심 조건:

```text
기준·부하 고유 입력 = Consumer 고유 수신 = Flink 입력 = 최종 저장
의도적 중복 = Consumer가 감지한 중복
실패 재현 = true
실패 종료 코드 != 0
복구 기대 행 수 = 복구 저장 행 수
복구 중복 = 0
복구 필수값 결측 = 0
```

조건이 맞지 않으면 결과를 무조건 건강하다고 기록하지 않고 오류 목록을 남깁니다.

### 9.2 `scripts/capture_result_window.ps1`

로컬 결과 화면을 캡처할 때 사용한 PowerShell 보조 스크립트입니다. 데이터 처리나 결과 수치를
변경하지 않습니다.

---

## 10. `pipeline_code` 폴더 설명

이 폴더는 당시 파이프라인을 재현하고 구현 내용을 확인하기 위한 코드 사본입니다. 대용량 데이터와
비밀키는 포함하지 않습니다.

### 10.1 Docker와 환경 파일

| 파일 | 역할 |
| --- | --- |
| `docker-compose.yml` | Kafka, Flink, Airflow 등 여러 컨테이너 연결 정의 |
| `Dockerfile.airflow` | Airflow 실행 이미지 구성 |
| `Dockerfile.flink` | Flink·PyFlink 실행 이미지 구성 |
| `requirements.txt` | 필요한 Python 패키지 목록 |

Docker 이미지는 프로그램 실행에 필요한 파일을 담은 설계도이고, 컨테이너는 그 이미지로 실제
실행 중인 격리된 프로세스입니다.

### 10.2 과거 배치 수집 코드

| 파일 | 역할 |
| --- | --- |
| `1_chunk_downloader.py` | 날짜 범위의 Binance OHLCV 원천 수집 |
| `backfill_runner.py` | 긴 기간을 청크로 나눠 수집·가공 반복 |
| `flink_batch_submitter.py` | PyFlink Batch Job 제출 |
| `flink_jobs/batch_feature_job.py` | 정제, ma_5, return_1m 계산과 Parquet 저장 |
| `9_futures_context_collector.py` | mark price, funding, open interest 수집 |
| `market_metadata.py` | symbol, market, timestamp 단위 표준화 |

### 10.3 Kafka·Flink 실험 코드

폴더 이름 `assignment4_kafka_spark`에는 과거 과제 이름의 `spark`가 남아 있지만 실제 처리 코드는
Flink를 사용합니다.

| 파일 | 역할 |
| --- | --- |
| `kafka_market_event_producer.py` | JSON 이벤트를 Kafka Topic에 전송 |
| `kafka_market_event_consumer.py` | Topic에서 읽고 run_id 선택·event_id 중복 제거 |
| `prepare_flink_input.py` | 필수 필드 검사 후 PyFlink 입력 생성 |
| `write_flink_report.py` | Flink Job과 저장 건수 보고서 생성 |
| `data/consumed_binance_usdm_events.jsonl` | GitHub 제출용 작은 실제 이벤트 샘플 |

### 10.4 부하·장애·복구 코드

| 파일 | 역할 |
| --- | --- |
| `assignment5_pipeline_resilience/run_experiment.py` | 기준, 부하, 중복, 장애, 복구 시나리오 실행 |
| `assignment5_pipeline_resilience/verify_output_quality.py` | Parquet 행 수·중복·결측 검사 |

### 10.5 Airflow DAG

| 파일 | 역할 |
| --- | --- |
| `airflow/dags/btcusdt_usdm_historical_backfill.py` | symbol과 날짜를 받아 과거 수집·가공·검증 순서 실행 |

DAG 코드는 실제 데이터를 직접 저장하는 모든 로직을 한 파일에 다시 작성하지 않습니다. 기존
수집기와 PyFlink 처리기를 정해진 순서로 호출하고 성공·실패 상태를 관리합니다.

---

## 11. Airflow 용어

### Airflow

여러 데이터 작업을 정해진 순서와 시간에 실행하고 상태를 기록하는 도구입니다. 데이터를 직접
가공하는 엔진이라기보다 작업 순서를 관리하는 지휘자에 가깝습니다.

### DAG

`Directed Acyclic Graph`의 약자입니다. 작업 순서에 순환이 없는 방향 그래프입니다.

```text
수집 -> PyFlink 가공 -> 피처 검사 -> 선물 문맥 검사
```

앞 작업이 성공해야 다음 작업을 실행하도록 관계를 정의합니다.

### Task

DAG 안에 있는 개별 작업입니다. 예를 들어 다운로드, Flink 제출, 품질 검사는 서로 다른 Task가
될 수 있습니다.

### DAG Run

DAG를 한 번 실행한 기록입니다. 같은 DAG라도 날짜와 symbol을 바꿔 여러 DAG Run을 만들 수
있습니다.

### Trigger

예약 시간을 기다리지 않고 DAG Run을 직접 시작하는 기능입니다. Trigger 화면에서 입력값을
변경해도 DAG 코드는 변경되지 않습니다.

### Parameter 또는 DAG conf

한 번의 실행에 전달하는 입력값입니다.

```json
{
  "symbol": "SOLUSDT",
  "start_date": "2026-08-28",
  "end_date": "2026-08-29"
}
```

### Schedule

DAG를 자동으로 실행할 시간 규칙입니다. 예를 들어 매일 UTC 00:15 실행하도록 설정할 수 있습니다.

### Catchup

Airflow가 꺼져 있던 동안 놓친 과거 예약 구간을 다시 실행하는 기능입니다. 단, 외부 API가 오래된
데이터를 제공하지 않으면 Airflow가 실행하더라도 복구할 수 없는 데이터가 있을 수 있습니다.

### Backfill

이미 지나간 과거 기간의 데이터를 다시 수집·처리해 빈 구간을 채우는 작업입니다.

### Chunk

5년처럼 긴 기간을 14일 등 작은 날짜 묶음으로 나눈 단위입니다. 한 청크가 실패해도 그 구간부터
다시 실행할 수 있습니다.

### Retry

일시적인 오류가 발생했을 때 같은 Task를 다시 시도하는 기능입니다. 잘못된 입력처럼 다시 해도
해결되지 않는 오류는 무한 재시도보다 입력을 수정하거나 Fallback을 선택해야 합니다.

---

## 12. Kafka 용어

### Kafka

여러 프로그램 사이에서 이벤트를 순서대로 보관하고 전달하는 분산 메시지 플랫폼입니다. 실시간
데이터가 처리 속도보다 빠르게 들어와도 중간에 저장해 Consumer가 이어서 읽게 합니다.

### Broker

Kafka 메시지를 실제로 저장하고 요청을 처리하는 Kafka 서버입니다. 로컬 프로젝트에서는 보통
한 Broker를 사용하지만 운영 고가용성 환경에서는 여러 Broker를 둡니다.

### Topic

메시지를 종류별로 나누는 논리적 이름입니다.

```text
assignment5.market.events.v1
```

이번 실험의 시장 이벤트가 저장된 Topic 이름입니다.

### Partition

하나의 Topic을 여러 개의 순서 있는 로그로 나눈 단위입니다. Partition이 여러 개면 여러 Consumer가
병렬로 처리할 수 있지만 전체 Topic의 완전한 순서는 보장되지 않고 Partition 내부 순서만
보장됩니다.

### Producer

Kafka Topic에 메시지를 보내는 프로그램입니다. 이번 실험에서는 저장된 OHLCV를 JSON 이벤트로
만들어 Kafka에 보냈습니다.

### Consumer

Kafka Topic에서 메시지를 읽는 프로그램입니다. 이번 Consumer는 run_id로 이번 실행 메시지만
선택하고 event_id로 중복을 제거했습니다.

### Consumer Group

같은 일을 나눠 처리하는 Consumer 묶음입니다. 한 Partition의 메시지는 같은 Consumer Group 안에서
한 Consumer에게 할당됩니다. 서로 다른 Group은 같은 Topic을 각각 독립적으로 읽을 수 있습니다.

### Offset

Partition 안에서 메시지의 위치를 나타내는 번호입니다. Consumer는 처리한 Offset을 기록해 재시작
후 어디부터 이어 읽을지 결정합니다.

### Consumer Lag

Kafka에 들어온 최신 Offset과 Consumer가 처리한 Offset의 차이입니다. Lag가 계속 커지면 들어오는
속도보다 처리 속도가 느리다는 뜻입니다.

### Retention

Kafka가 메시지를 보관하는 기간 또는 용량 규칙입니다. Consumer가 읽었다고 메시지가 즉시
삭제되는 것은 아니며 Retention 범위에서는 다시 읽을 수 있습니다.

### Replay

저장된 이벤트를 다시 전송하거나 Kafka Offset을 되돌려 다시 읽는 작업입니다. 같은 입력으로
처리 결과를 재현하거나 복구할 때 사용합니다.

### `event_id`

시장 이벤트 자체의 고유성을 판단하는 업무 ID로 사용하는 것이 목표입니다.

```text
운영 목표: binance:usdm:BTCUSDT:candle:1m:1787359320000
현재 실험: assignment5-load_10000-20260831T014703Z-00000000
```

이번 실험 코드는 `run_id + sequence`로 event_id를 만들었습니다. 그래서 같은 실행 안에서 다시 보낸
중복 500건은 찾을 수 있지만, 실행이 바뀌면 같은 1분봉에도 다른 ID가 생깁니다. 같은 1분봉을 다른
실행에서 다시 처리해도 중복으로 판단하려면 운영 버전에서 run_id와 독립적인 결정적 ID로 바꿔야
합니다.

### `run_id`

프로그램 실행을 구분하는 ID입니다. 원칙적으로 event_id는 데이터 자체를, run_id는 실행을
구분해야 합니다. 현재 실험 코드는 event_id에도 run_id가 포함되어 있으므로 실행 사이의 중복 제거는
아직 구현되지 않았습니다.

```text
event_id: 데이터가 무엇인가?
run_id: 어느 실행에서 보냈는가?
```

### 중복 제거 또는 Deduplication

같은 event_id가 여러 번 도착했을 때 한 번만 다음 단계로 보내는 처리입니다. Kafka의 전송 보장만으로
업무 이벤트 중복이 항상 자동 제거되는 것은 아니므로 별도 고유 ID와 저장 규칙이 필요합니다.

### Idempotency

같은 작업을 여러 번 실행해도 최종 결과가 한 번 실행한 것과 같게 유지되는 성질입니다. 동일
event_id를 재전송해도 Parquet에 한 행만 남는 것이 예입니다.

### At-least-once

메시지가 유실되지 않도록 최소 한 번 이상 전달하는 방식입니다. 재시도 때문에 중복될 수 있어
Consumer 또는 저장 단계의 중복 제거가 필요합니다.

### Exactly-once

논리적인 결과가 한 번만 반영되도록 만드는 처리 보장입니다. 단순히 설정 이름 하나만 켠다고 전체
외부 시스템까지 자동 보장되는 것이 아니며 Source Offset, 상태, Sink commit을 함께 설계해야 합니다.

---

## 13. Flink와 PyFlink 용어

### Apache Flink

대량의 배치 데이터와 계속 들어오는 스트리밍 데이터를 처리하는 분산 데이터 처리 엔진입니다.

### PyFlink

Apache Flink 작업을 Python으로 작성할 수 있게 해 주는 API입니다. PyFlink는 Flink와 다른 제품이
아니라 Flink를 Python으로 사용하는 방법입니다.

### Batch Processing

시작과 끝이 정해진 데이터를 한 묶음으로 처리하는 방식입니다. 이번 1,000건·10,000건 실험과
날짜 범위 과거 데이터 처리가 여기에 해당합니다.

### Stream Processing

끝없이 들어오는 이벤트를 도착하는 대로 계속 처리하는 방식입니다. Binance WebSocket 실시간
데이터 경로가 여기에 해당합니다.

### Job

Flink에 제출한 하나의 데이터 처리 프로그램 실행입니다. 기준, 부하, 복구가 각각 다른 Job입니다.

### Job ID

Flink Job을 구분하는 고유 ID입니다. JSON 보고서의 Job ID와 Flink 화면의 Job ID를 비교해 같은
실행인지 확인할 수 있습니다.

### JobManager

Flink Job 전체 계획, Task 배정, 장애 복구를 조정하는 관리자 프로세스입니다.

### TaskManager

실제 데이터 계산을 수행하는 작업 프로세스입니다. 데이터 읽기, 변환, 저장 연산이 이곳에서
실행됩니다.

### Task Slot

TaskManager가 동시에 작업을 실행할 수 있도록 나눈 자원 단위입니다. `Free Slots=1`은 현재
하나의 작업을 더 받을 수 있다는 뜻이지 과거 Job이 실행되지 않았다는 뜻이 아닙니다.

### Records Received

Flink 연산자가 받은 레코드 수입니다. 기준 Job에서 1,000, 부하 Job에서 10,000인지 화면에서
확인할 수 있습니다.

### `FINISHED`

Flink Job이 오류 없이 끝났다는 상태입니다. 행 수, 중복, 결측이 모두 정상이라는 의미까지 포함하지
않으므로 Parquet 품질 검사도 필요합니다.

### Event Time

거래소에서 실제 이벤트가 발생한 시각입니다. 네트워크 도착이 늦어도 시장 시각 기준으로 윈도우를
계산할 때 사용합니다.

### Processing Time

Flink가 이벤트를 실제 처리한 컴퓨터 시각입니다. 네트워크 지연에 따라 Event Time과 다를 수
있습니다.

### Watermark

Event Time 기준으로 어느 시각까지의 이벤트가 대부분 도착했다고 판단하는 진행 표시입니다.
늦게 도착한 이벤트를 얼마나 기다릴지 정하는 데 사용합니다.

### Late Event

Watermark가 이미 지난 뒤 도착한 오래된 이벤트입니다. 허용 지연 안에서 처리하거나 별도 저장하거나
버리는 정책을 정해야 합니다.

### Checkpoint

스트리밍 Job의 상태와 Kafka Offset을 일정 시점에 저장한 복구 지점입니다. TaskManager가 재시작돼도
마지막 성공 Checkpoint부터 이어갈 수 있습니다.

### State

Flink가 계산 중 기억하는 데이터입니다. 이동평균을 위한 최근 값, 윈도우 중간 결과, 중복 확인
정보 등이 State가 될 수 있습니다.

---

## 14. 저장 파일과 데이터 형식 용어

### JSON

중괄호와 `key: value` 구조로 된 텍스트 데이터 형식입니다. 한 실행의 통합 결과처럼 계층 구조가
있는 정보를 저장하기 좋습니다.

### JSONL

한 줄에 JSON 객체 하나를 저장하는 형식입니다. 파일 전체를 메모리에 올리지 않고 한 줄씩 읽을 수
있어 이벤트 목록에 적합합니다.

```text
{첫 번째 이벤트 JSON}
{두 번째 이벤트 JSON}
{세 번째 이벤트 JSON}
```

### CSV

행과 열을 쉼표로 구분하는 텍스트 형식입니다. 사람이 보기 쉽지만 데이터 타입과 압축·대규모 조회
효율은 Parquet보다 약합니다.

### Parquet

컬럼 단위로 압축 저장하는 바이너리 파일 형식입니다. 필요한 컬럼만 읽기 쉽고 숫자 타입을 유지해
머신러닝 데이터와 대용량 분석에 적합합니다.

Parquet은 텍스트 편집기로 열 수 없습니다. VS Code에서 바이너리 또는 지원하지 않는 인코딩이라는
메시지가 나오는 것이 정상입니다.

```powershell
python -c "import pandas as pd; print(pd.read_parquet('파일.parquet').head())"
```

### Feature Store

원천 데이터를 가공해 만든 머신러닝 입력 피처를 저장하는 논리적 공간입니다. 현재 프로젝트에서는
별도 서버 제품이 아니라 폴더와 Parquet 파일로 구현했습니다.

### Partition

저장 파일을 market, symbol, timeframe, 날짜 등으로 나눠 필요한 범위만 읽게 하는 구조입니다.

```text
market=usdm/symbol=BTCUSDT/timeframe=1m/year=2026/month=08/
```

Kafka Partition과 Parquet 저장 Partition은 이름이 같지만 역할이 다릅니다.

```text
Kafka Partition: 메시지 순서와 병렬 처리 단위
Parquet Partition: 파일 검색 범위를 줄이는 폴더 분류 단위
```

### Schema

필드 이름, 데이터 타입, 필수 여부를 정한 데이터 구조 계약입니다. Producer와 Consumer가 같은
Schema를 알아야 메시지를 안전하게 해석할 수 있습니다.

### Schema Version

데이터 구조가 바뀌었을 때 어떤 규칙으로 만들어진 데이터인지 구분하는 버전입니다.

```text
schema_version=market_event_v1
feature_schema_version=...
```

이번 Kafka 이벤트 코드에서는 필드 이름으로 `schema_version`을 사용합니다. 문서와 프로그램이 서로
다른 이름을 쓰면 안 되므로 실제 메시지 필드명을 기준으로 확인해야 합니다.

### Success Marker

Parquet 저장과 검증이 모두 성공했다는 정보를 기록한 작은 JSON 파일입니다. 파일이 존재하는지만
보지 않고 행 수, 기간, Schema Version도 확인해야 합니다.

---

## 15. 시장 데이터와 머신러닝 용어

### Binance USDT-M

증거금과 손익을 USDT로 계산하는 Binance 선물 시장입니다. 현물 Spot 시장과 데이터·수수료·펀딩·
청산 구조가 다릅니다.

### Symbol 또는 Ticker

거래 종목을 나타내는 값입니다. `BTCUSDT`는 BTC 가격을 USDT 기준으로 나타냅니다.

### OHLCV

일정 시간 동안의 가격과 거래량을 다섯 값으로 표현합니다.

| 글자 | 이름 | 뜻 |
| --- | --- | --- |
| O | Open | 시작 가격 |
| H | High | 최고 가격 |
| L | Low | 최저 가격 |
| C | Close | 마지막 가격 |
| V | Volume | 거래량 |

### Candle 또는 봉

OHLCV 한 행을 뜻합니다. 1분봉은 1분 동안의 OHLCV입니다.

### `timestamp`

컴퓨터가 시각을 표현하는 숫자입니다. 현재 프로젝트는 UTC Unix millisecond를 주로 사용합니다.

### UTC

전 세계에서 공통 기준으로 사용하는 시간대입니다. 한국 시간 KST는 UTC보다 9시간 빠릅니다.

```text
UTC 00:00 = KST 09:00
```

### Millisecond

1초의 1,000분의 1입니다. `event_time_ms`의 `ms`는 millisecond를 의미합니다.

### Feature 또는 피처

원천 데이터를 모델이 학습하기 쉬운 숫자로 가공한 입력값입니다.

```text
ma_5: 최근 5개 종가의 평균
return_1m: 직전 1분과 비교한 가격 변화율
```

### Label 또는 라벨

모델이 학습할 정답입니다. 예를 들어 다음 구간에 Long이 수익인지, Short가 수익인지 또는 거래하지
않아야 하는지를 과거 미래 가격으로 계산한 값입니다.

### `ma_5`

현재 1분봉을 포함한 최근 5개 종가의 이동평균입니다.

```text
ma_5 = 최근 종가 5개의 합 / 5
```

### `return_1m`

직전 1분 종가와 현재 종가의 변화율입니다.

```text
return_1m = 현재 종가 / 이전 종가 - 1
```

### Inference

학습된 모델에 새 피처를 넣어 Long, Short, no_trade 같은 예측을 얻는 단계입니다. 현재 승인 모델이
없으므로 실시간 추론은 안전하게 `no_trade`로 제한됩니다.

### Paper Trading

실제 돈을 보내지 않고 가상 주문과 체결을 기록하는 모의 거래입니다. 실제 체결 지연·슬리피지·부분
체결을 충분히 반영해야 의미 있는 장기 검증이 됩니다.

---

## 16. 품질·부하·복구 용어

### Baseline 또는 기준 실행

입력량을 늘리기 전 정상 상태의 시간과 처리량을 측정한 실행입니다. 이번 기준은 1,000건입니다.

### Load Test 또는 부하 실행

입력량을 늘려 처리 시간, 처리량, 오류와 저장 결과를 비교하는 실험입니다. 이번 부하는 고유
10,000건과 중복 500건입니다.

### Throughput 또는 처리량

단위 시간 동안 처리한 데이터 수입니다.

```text
최종 처리량 = 최종 저장 행 수 / 전체 실행 시간
```

기준 실행은 89.759행/초, 부하 실행은 776.699행/초입니다.

### Latency 또는 지연 시간

이벤트 하나가 입력된 뒤 결과가 나올 때까지 걸리는 시간입니다. 전체 10,000건을 처리한 시간과
개별 이벤트 Latency는 다른 지표입니다.

### Bottleneck 또는 병목

전체 파이프라인에서 처리 속도를 가장 많이 제한하는 단계입니다. Producer가 빨라도 Flink나 디스크
저장이 느리면 그 단계에 데이터가 쌓일 수 있습니다.

### Data Loss 또는 유실

고유 입력이 이유 없이 최종 결과에 도착하지 않은 상태입니다. 이번 실험의 고유 입력 기준 예상 밖
미처리는 0건입니다.

### Duplicate 또는 중복

같은 업무 이벤트가 두 번 이상 존재하는 상태입니다. timestamp만 같다고 항상 같은 이벤트인 것은
아니므로 event_id 설계가 중요합니다.

### Missing Value 또는 결측값

필수 컬럼의 값이 비어 있는 상태입니다. 가격 필드가 비면 피처와 라벨 계산이 잘못될 수 있습니다.

### Data Quality Gate 또는 품질 관문

다음 단계로 진행하기 전에 행 수, 중복, 결측, 시간 공백, Schema를 검사하는 조건입니다. 실패하면
잘못된 결과를 저장하거나 모델에 전달하지 않습니다.

### Fault Injection 또는 장애 주입

실제 발생할 수 있는 오류를 안전하게 일부러 만드는 실험입니다. 이번에는 `close` 필드를 제거해
입력 검사 실패를 재현했습니다.

### Fail Fast

잘못된 입력을 발견하면 후속 가공과 저장을 계속하지 않고 빠르게 실패시키는 방식입니다. 오염된
Parquet을 만드는 것보다 오류를 명확하게 남기고 멈추는 편이 안전합니다.

### Fail Closed

상태를 확신할 수 없을 때 위험한 동작을 허용하지 않는 방식입니다. 모델이나 데이터에 문제가 있을
때 실제 주문 대신 `no_trade`를 출력하는 것이 예입니다.

### Alert

오류 발생 여부, 원인, 단계와 심각도를 기록하거나 알리는 정보입니다. 현재 제출은 로컬 JSON·로그
Alert이며 외부 Slack·이메일은 연결하지 않았습니다.

### Fallback

주 경로를 사용할 수 없을 때 사용하는 검증된 대체 입력이나 처리 방법입니다. 이번에는 마지막으로
검증된 JSONL이 Fallback입니다.

### Recovery 또는 복구

실패 원인을 제거하거나 Fallback을 사용해 안전한 지점부터 다시 실행하고 결과를 재검증하는
과정입니다.

### Restart Location 또는 재실행 위치

전체 파이프라인을 처음부터 반복하지 않고 어느 중간 단계부터 다시 시작할지 나타냅니다.

```text
이번 재실행 위치 = 검증된 JSONL -> Flink 입력 준비
```

### Resilience 또는 복원력

부하나 장애가 생겨도 데이터를 잃거나 중복시키지 않고 정상 상태로 돌아올 수 있는 능력입니다.

### `healthy=true`

미리 정의한 품질 조건을 모두 통과했다는 뜻입니다. 시스템이 모든 입력량과 모든 장애에서 절대
문제가 없다는 뜻은 아닙니다.

---

## 17. 자주 혼동하는 내용

### Kafka가 중복을 자동으로 모두 제거하는가?

아닙니다. Kafka는 메시지 저장과 전달을 담당합니다. Producer 재시도나 Consumer 재처리로 같은
업무 이벤트가 다시 올 수 있으므로 event_id와 멱등 저장이 필요합니다.

### Kafka가 죽으면 데이터가 모두 사라지는가?

항상 그렇지는 않습니다. 디스크 볼륨과 Retention이 유지되면 프로세스를 다시 시작해도 메시지가
남을 수 있습니다. 하지만 단일 Broker와 복제 계수 1에서는 디스크 자체가 손상되면 복구할 복제본이
없습니다.

### Producer와 Consumer는 Kafka 내부 기능인가?

Kafka가 연결 API와 클라이언트 라이브러리를 제공하지만 Producer와 Consumer는 보통 별도의 외부
프로그램입니다. 현재 제출 코드에도 각각 Python 파일로 존재합니다.

### Producer가 10,500건을 보냈는데 10,000건만 저장됐으면 500건이 유실된 것인가?

아닙니다. 500건은 같은 event_id를 가진 의도적 중복입니다. 고유 이벤트 10,000건은 모두
저장됐습니다.

### Flink 화면이 비어 있으면 실행하지 않은 것인가?

항상 그렇지는 않습니다. JobManager가 재시작되거나 영구 History Server가 없으면 과거 완료 Job이
UI에서 사라질 수 있습니다. 그래서 JSON 결과, Job ID, 로그와 Parquet도 함께 보관합니다.

### `TaskManager Assigned Tasks=0`이면 처리하지 않은 것인가?

아닙니다. 화면을 확인하는 현재 시점에 실행 중인 Task가 0개라는 뜻입니다. 배치 Job은 이미
완료됐을 수 있습니다.

### Parquet을 VS Code로 열 수 없으면 파일이 깨진 것인가?

아닙니다. Parquet은 바이너리 컬럼 형식이라 일반 텍스트 편집기로 열 수 없는 것이 정상입니다.

### Job이 FINISHED면 데이터 품질도 무조건 정상인가?

아닙니다. 프로그램 오류 없이 끝났다는 뜻입니다. 저장 행 수, 중복, 결측과 시간 공백은 별도 품질
검사로 확인해야 합니다.

### 10,000건을 성공했으면 최대 한계를 찾은 것인가?

아닙니다. 현재 설정에서 10,000건을 정상 처리했다는 뜻입니다. 실패 경계와 지속 가능한 처리량을
찾으려면 입력량을 단계적으로 늘리고 CPU, 메모리, Kafka Lag와 디스크 사용량을 함께 측정해야 합니다.

### Alert 화면이 있으면 외부 알림까지 구현된 것인가?

아닙니다. 현재 Alert는 로컬 JSON과 로그입니다. 외부 알림은 별도 연결이 필요합니다.

### 모델 파일이 있으면 자동매매가 가능한가?

아닙니다. 비용 포함 Walk-Forward, 장기 Paper, 리스크 관문과 사람 승인을 통과한 모델만 추론 경로에
연결해야 합니다. 현재 승인 모델과 실제 주문은 차단되어 있습니다.

---

## 18. 파일로 결과를 확인하는 명령

### 통합 결과 JSON 읽기

```powershell
Get-Content -Raw assignment6_submission/results/assignment6_pipeline_review.json
```

### Alert와 Fallback 결과 읽기

```powershell
Get-Content -Raw assignment6_submission/results/assignment6_alert_and_fallback.json
```

### 오류 로그 읽기

```powershell
Get-Content assignment6_submission/logs/fault_invalid_input.log
```

### JSON에서 핵심 값만 확인하기

```powershell
$result = Get-Content -Raw `
  assignment6_submission/results/assignment6_pipeline_review.json | ConvertFrom-Json

$result.baseline_and_load | Format-Table `
  name, producer_sent_count, consumer_unique_count, final_parquet_count, `
  total_pipeline_seconds, final_rows_per_second, healthy
```

### 전체 코드 테스트

프로젝트 루트에서 실행합니다.

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

---

## 19. 현재 제출이 증명하는 범위

증명한 내용:

- 저장된 실제 Binance 기반 이벤트를 Kafka로 재생할 수 있음
- 기준 1,000건과 부하 10,000건이 PyFlink와 Parquet까지 도착함
- 의도적 중복 500건을 Consumer가 제거함
- 잘못된 필수 입력을 Flink 제출 전에 차단함
- 오류 종료 코드와 로그를 남김
- 검증된 JSONL에서 재개해 1,000행을 복구함
- 복구 결과의 중복과 결측이 0건임
- Airflow에서 코드를 바꾸지 않고 symbol과 날짜를 입력할 수 있음

아직 증명하지 않은 내용:

- Kafka의 절대 최대 처리 한계
- 여러 Broker를 사용한 고가용성
- Slack·이메일 외부 Alert 전송
- 승인 모델의 실시간 수익성
- 장기간 Paper Trading 성과
- 거래소 Testnet 주문 상태 관리
- 실제 자금 자동매매

`구현됨`, `실행 검증됨`, `성능 승인됨`, `실거래 가능`은 서로 다른 상태입니다. 이번 제출은 데이터
파이프라인의 부하·장애·복구와 최종 저장 정합성을 확인한 결과이며 자동매매 수익성을 증명하는
보고서가 아닙니다.

---

## 20. 한 페이지 요약

```text
Airflow
  = 작업 순서와 날짜를 관리한다.

Producer
  = 시장 이벤트를 Kafka에 보낸다.

Kafka
  = 이벤트를 Topic에 보관한다.

Consumer
  = Kafka에서 읽고 중복을 제거한다.

PyFlink
  = 검증된 데이터를 피처로 가공한다.

Parquet
  = 피처를 컬럼 기반 파일로 저장한다.

품질 검사
  = 행 수, 중복, 결측을 확인한다.

Alert
  = 장애가 발생했다는 사실과 원인을 기록한다.

Fallback
  = 검증된 대체 입력에서 다시 시작한다.

이번 결과
  = 기준 1,000건 성공
  = 부하 고유 10,000건 성공
  = 중복 500건 제거
  = 예상 밖 미처리 0건
  = 잘못된 입력 차단
  = Fallback 1,000행 복구
  = 실제 주문 기능은 차단
```
