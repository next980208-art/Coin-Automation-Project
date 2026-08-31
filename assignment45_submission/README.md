# 4·5주차 데이터 파이프라인 과제

Binance USDT-M 선물 데이터 프로젝트에 Airflow 자동화와 부하·장애·복구 실험을 적용한
결과입니다.

## 먼저 볼 파일

1. `AIRFLOW_ACTUAL_UI_TRIGGER_EXECUTION_2026-08-31.md`: 4주차 Airflow 실행 과정과 결과
2. `ASSIGNMENT5_ACTUAL_EXECUTION_AND_UI_EVIDENCE_2026-08-31.md`: 5주차 실험 과정과 결과

코드는 다음 위치에서 확인할 수 있습니다.

- Airflow DAG: `airflow/dags/btcusdt_usdm_historical_backfill.py`
- 5주차 실험 실행: `assignment5_pipeline_resilience/run_experiment.py`
- 5주차 최종 수치: `assignment5_pipeline_resilience/results/assignment5_final_report.json`

## 4주차 결과

Airflow 화면에서 코드를 수정하지 않고 `SOL/USDT`와 2026-08-28 하루를 입력했습니다.
데이터 수집, PyFlink 가공, 피처 검사와 선물 컨텍스트 검사가 모두 성공했습니다.

- 피처 데이터: 1,440행
- 선물 컨텍스트 데이터: 1,440행
- timestamp 중복, 1분 공백, 필수값 결측: 모두 0건
- Airflow 실행 시간: 27초

## 5주차 결과

저장된 실제 BTCUSDT 데이터를 외부 서버가 아닌 로컬 Kafka에 재생했습니다.

- 정상 실행: 1,000건, 11.141초
- 부하 실행: 고유 10,000건과 중복 500건, 12.875초
- 중복 500건 제거 후 Parquet 10,000행 저장
- `close` 필드 누락 입력은 종료 코드 1로 실패
- 정상 1,000건을 다시 실행해 중복과 결측 없이 복구

현재 프로젝트에서 사용 중인 PyFlink로 처리했습니다. 대용량 원본, API key, 계정 정보와
실거래 기능은 제출 폴더에 포함하지 않았습니다.
