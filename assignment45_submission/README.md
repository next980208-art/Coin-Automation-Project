# Airflow 자동화 및 데이터 파이프라인 복구 과제 제출본

이 폴더에는 BTCUSDT 선물 데이터 프로젝트에 맞춰 수행한 두 과제가 들어 있습니다.

## 읽는 순서

1. `GITHUB_ASSIGNMENT_REQUIREMENTS_MAPPING.md`: 두 과제의 필수 항목과 실행 증거
2. `GITHUB_ASSIGNMENT_SCREENSHOT_EVIDENCE.md`: 직접 캡처한 Airflow·Flink UI와 실행 증거 해석
3. `GITHUB_AIRFLOW_ASSIGNMENT_PRESENTATION.md`: 4차시 Airflow 발표 자료
4. `assignment5_pipeline_resilience/README.md`: 5차시 부하·장애·복구 발표 자료
5. `airflow/dags/btcusdt_usdm_historical_backfill.py`: 입력값을 받는 Airflow DAG
6. `assignment5_pipeline_resilience/results/assignment5_final_report.json`: 실제 실행 수치

## 핵심 결과

- Airflow 입력값을 BTC에서 ETHUSDT로 바꿔 실제 1,440행 수집·가공·저장
- Airflow 성공 DAG 그래프와 Flink 완료 3건·실패 0건 화면을 실제 UI로 직접 캡처
- 정상 1,000건과 부하 10,000건 Kafka·PyFlink 처리 비교
- 중복 500건 정확히 감지 및 제거
- 필수 `close` 누락 입력을 안전하게 실패시키고 정상 1,000행으로 복구
- 최종 Parquet 중복 0건, 필수 값 결측 0건

Spark 대신 프로젝트 표준 엔진인 Apache Flink PyFlink를 사용했습니다. 외부 서비스에 부하를
보내지 않았고, API key와 실거래 주문 기능도 포함하지 않았습니다.
