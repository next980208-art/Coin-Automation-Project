# 4·5차시 과제 실제 실행 화면과 검증 자료

## 가장 먼저 볼 증거

4차시 Airflow 과제는 아래 문서에 실제 브라우저 실행 과정이 처음부터 끝까지 정리돼 있습니다.

- [Airflow 실제 UI 매개변수 실행 및 증거](AIRFLOW_ACTUAL_UI_TRIGGER_EXECUTION_2026-08-31.md)

핵심 화면은 다음 두 장입니다.

![SOL USDT 입력과 생성된 JSON](assignment_evidence/airflow_ui_trigger_2026-08-31/02_trigger_form_solusdt_json.png)

![Airflow DAG 네 태스크 성공](assignment_evidence/airflow_ui_trigger_2026-08-31/04_dag_run_success_graph.png)

이 화면은 예시로 그린 그림이 아니라 실행 중인 `localhost:8080` Airflow UI에서 직접
캡처했습니다. Run ID는 `assignment4_solusdt_20260828_20260829`이고 네 태스크가 모두
`success`입니다.

## Airflow 실행 검증 수치

| 항목 | 결과 |
| --- | --- |
| 종목 | `SOL/USDT` |
| 기간 | 2026-08-28~2026-08-29, 종료일 미포함 |
| OHLCV 피처 | 1,440행 |
| 선물 컨텍스트 | 1,440행 |
| timestamp 중복 | 0건 |
| 1분 공백 | 0건 |
| 필수값 결측 | 0건 |
| 품질 검사 | `healthy=true` |
| 실제 실행 시간 | 27초 |

원본 검증 JSON:

```text
assignment_evidence/airflow_ui_trigger_2026-08-31/07_run_verification.json
```

## 기존 실제 UI 증거

다음 화면들도 Airflow와 Flink UI를 직접 열어 캡처한 자료입니다.

![Airflow 기존 실행 목록](assignment_evidence/actual_ui/airflow_dag_runs_actual.png)

![Airflow 기존 성공 그래프](assignment_evidence/actual_ui/airflow_success_graph_actual.png)

![Flink 실제 Overview](assignment_evidence/actual_ui/flink_overview_actual.png)

Flink 클러스터를 재시작하면 메모리에 있던 완료 Job 상세가 사라질 수 있습니다. 그래서
Job ID, 입출력 건수, 실행 시간은 실행 결과 JSON에도 함께 저장했습니다.

## 5차시 부하·장애·복구 증거

5차시 실험은 외부 Binance 서비스에 부하를 보내지 않고 저장해 둔 실제 이벤트를 로컬 Kafka에
재생했습니다.

![Kafka PyFlink 부하 비교](assignment_evidence/assets/03_kafka_flink_load_comparison.png)

- 정상 실행: 고유 이벤트 1,000건, 최종 Parquet 1,000행
- 부하 실행: 고유 이벤트 10,000건과 중복 500건 전송
- Consumer 중복 감지: 500건
- 최종 Parquet: 고유 10,000행

![장애 복구와 품질 검증](assignment_evidence/assets/04_fault_recovery_quality.png)

- 필수 `close` 필드가 없는 입력은 종료 코드 1로 안전하게 실패
- 정상 입력으로 재실행해 1,000행 복구
- 최종 중복 0건, 필수값 결측 0건

수치 원본은 다음 파일에서 확인할 수 있습니다.

```text
assignment5_pipeline_resilience/results/assignment5_final_report.json
assignment5_pipeline_resilience/results/assignment5_output_quality_check.json
```

## 증거를 다시 만드는 방법

Airflow와 Flink를 실행한 상태에서 데이터 기반 요약 이미지는 다음 명령으로 갱신합니다.

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\capture_assignment_evidence.ps1
```

`assignment_evidence/actual_ui`와 `airflow_ui_trigger_2026-08-31` 폴더의 이미지는 실제
브라우저 UI를 직접 캡처한 파일입니다. `assignment_evidence/assets` 이미지는 실행 JSON을
읽어 보기 쉽게 요약한 보조 자료입니다.
