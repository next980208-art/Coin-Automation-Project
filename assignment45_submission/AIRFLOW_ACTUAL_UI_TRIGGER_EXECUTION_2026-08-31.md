# Airflow 실제 UI 매개변수 실행 및 증거

## 1. 무엇을 증명하는 문서인가

이 문서는 Airflow DAG 코드를 수정하지 않고 브라우저의 `Trigger DAG` 화면에서 종목과
날짜를 입력해 실제로 실행한 결과를 정리합니다. 화면은 별도로 그린 예시 이미지가 아니라
실행 중인 `localhost:8080` Airflow UI에서 직접 캡처했습니다.

| 항목 | 값 |
| --- | --- |
| DAG | `btcusdt_usdm_historical_backfill` |
| Run ID | `assignment4_solusdt_20260828_20260829` |
| 종목 | `SOL/USDT` |
| 시장 | `usdm` |
| 시간 단위 | `1m` |
| 시작일 | `2026-08-28` |
| 종료일 | `2026-08-29` |
| 청크 크기 | 1일 |
| 실행 결과 | `success` |

종료일은 포함하지 않으므로 실제 처리 범위는 2026-08-28 00:00부터 23:59 UTC까지
총 1,440분입니다.

## 2. 실제 실행 순서

### 2.1 Trigger DAG 기본 화면 열기

![Airflow Trigger DAG 기본 입력 화면](assignment_evidence/airflow_ui_trigger_2026-08-31/01_trigger_form_default.png)

이 화면은 DAG에 선언된 입력 항목을 Airflow가 자동으로 만든 화면입니다. Python 코드를
고치지 않고 `symbol`, `start_date`, `end_date`, `chunk_days` 등을 바꿀 수 있습니다.

### 2.2 SOL/USDT와 날짜 입력

![SOL USDT 입력과 생성된 JSON](assignment_evidence/airflow_ui_trigger_2026-08-31/02_trigger_form_solusdt_json.png)

입력값을 바꾸면 Airflow가 아래 실행 설정 JSON을 생성합니다.

```json
{
  "symbol": "SOL/USDT",
  "market": "usdm",
  "timeframe": "1m",
  "feature_folder": "feature_store_v2",
  "context_folder": "futures_context_store_v2",
  "temp_folder": "temp_raw_data_v3",
  "target_start_date": "2021-08-25",
  "initial_end_date": "2026-08-22",
  "chunk_days": 1,
  "start_date": "2026-08-28",
  "end_date": "2026-08-29",
  "chunks_per_trigger": 1
}
```

`Trigger` 버튼을 누르면 이 JSON이 `dag_run.conf`로 전달됩니다. DAG 내부 코드는 이 값을
읽어 실제 수집·가공 명령의 인자를 만듭니다.

### 2.3 실제 DAG Run 생성

![Airflow DAG Run 생성 알림](assignment_evidence/airflow_ui_trigger_2026-08-31/03_dag_run_started.png)

화면 상단에 새 Run ID가 생성됐다는 알림이 나타났습니다. 생성 직후 다른 예약 작업이
SequentialExecutor를 사용 중이어서 이 실행이 잠시 대기했습니다. 일일 수집 DAG를 잠시
일시정지하고 Airflow 스케줄러를 재시작한 뒤 기존 예약 작업과 이 실행이 순서대로
처리됐습니다. 이 과정에서도 DAG 코드는 수정하지 않았습니다.

### 2.4 네 태스크 모두 성공

![Airflow DAG 네 태스크 성공 그래프](assignment_evidence/airflow_ui_trigger_2026-08-31/04_dag_run_success_graph.png)

1. `plan_next_backfill`: 화면 입력값을 읽고 처리 범위를 결정
2. `run_next_backfill`: Binance 데이터 수집, PyFlink 가공, Parquet 저장
3. `verify_feature_store`: 피처 데이터의 행 수, 중복, 공백, 결측 검증
4. `verify_futures_context_store`: mark price, funding rate, open interest 데이터 검증

### 2.5 실행 상세 정보 확인

![Airflow 성공 Run 상세](assignment_evidence/airflow_ui_trigger_2026-08-31/05_dag_run_success_details.png)

- 상태: `success`
- Run type: `manual`
- Externally triggered: `True`
- 시작: 2026-08-31 01:26:30 UTC
- 종료: 2026-08-31 01:26:57 UTC
- 실제 실행 시간: 27초

![Airflow 실행 설정 JSON 상세](assignment_evidence/airflow_ui_trigger_2026-08-31/06_dag_run_configuration_details.png)

Run config에는 실제 입력한 `SOL/USDT`, 시작일, 종료일, 청크 크기가 남아 있습니다. 따라서
성공한 그래프가 어떤 입력으로 실행됐는지 다시 추적할 수 있습니다.

## 3. 만들어진 결과

### 3.1 OHLCV 피처 데이터

- 저장 행 수와 고유 timestamp: 각각 1,440개
- 중복 timestamp, 1분 공백, 필수 컬럼 결측: 각각 0개
- 품질 상태: `healthy=true`
- PyFlink Job ID: `23e4817a84f50f74870da6242c684f65`

```text
feature_store_v2/market=usdm/symbol=SOLUSDT/timeframe=1m/year=2026/month=08/
features_SOLUSDT_USDM_1m_20260828_20260829_202608.parquet
```

### 3.2 선물 컨텍스트 데이터

- 저장 행 수와 고유 timestamp: 각각 1,440개
- 중복 timestamp, 1분 공백, 필수 컬럼 결측, mark price 보간: 각각 0개
- 품질 상태: `healthy=true`

```text
futures_context_store_v2/market=usdm/symbol=SOLUSDT/timeframe=1M/
context_SOLUSDT_USDM_CONTEXT_1M_20260828_20260829.parquet
```

대용량 Parquet 원본은 GitHub 제출본에서 제외하고 검증 수치는 다음 작은 JSON에 남겼습니다.

```text
assignment_evidence/airflow_ui_trigger_2026-08-31/07_run_verification.json
```

## 4. 과제 필수 항목과 연결

| 과제 요구 사항 | 이번 실행에서의 증거 |
| --- | --- |
| 기존 수집·처리 코드를 Airflow DAG로 실행 | 성공 그래프의 네 태스크 |
| 코드를 고치지 않고 입력값 변경 | Trigger DAG 화면의 SOL/USDT와 날짜 입력 |
| 값을 바꿔 한 번 더 실행 | 기존 BTC·ETH 실행과 다른 SOL/USDT 실행 |
| DAG 코드 제출 | `airflow/dags/btcusdt_usdm_historical_backfill.py` |
| 실행 화면 또는 로그 제출 | 이 문서의 실제 UI 캡처 6장 |
| 만들어진 결과 제출 | 검증 JSON과 Parquet 경로·행 수 |
| 대용량 원본과 비밀키 제외 | 화면, 코드, 작은 JSON만 제출 폴더에 포함 |

## 5. 한 문장 설명

> Airflow UI에서 코드를 수정하지 않고 SOL/USDT와 날짜 범위를 입력해 DAG를 실행했고,
> Binance 선물 데이터를 수집한 뒤 PyFlink로 가공해 두 종류의 Parquet에 각각 1,440행을
> 저장했으며 네 태스크와 품질 검사가 모두 성공했습니다.
