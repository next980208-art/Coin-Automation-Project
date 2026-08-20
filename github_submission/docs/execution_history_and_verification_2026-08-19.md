# 통합 실행 이력과 검증 기록

수정일: 2026-08-20

## 목적

이 문서는 초기의 분리된 텍스트 실행 로그를 대체합니다. 중요한 명령, 사실적인
결과, 실패 원인, 해결 방법을 보존하되, 과거의 좋은 시험 결과를 실제로 거래할 수
있는 전략의 증거로 보지 않습니다.

## 1. 최초 과거 OHLCV 시험

```powershell
python 1_chunk_downloader.py --start-date 2024-01-01 --days 7 --no-kafka
python 2_flink_processor.py
python 3_ml_training.py
```

결과:

```text
종목: BTC/USDT Spot
시간 단위: 1분
기간: 2024-01-01 ~ 2024-01-08
수집·처리 행 수: 10,080
피처 결과: Parquet + 성공 표시 파일
원천 CSV: 검증 뒤 삭제
초기 모델 시험 정확도: 약 50.15%
```

이때 이름이 `2_flink_processor.py`인 처리기는 Apache Flink Dashboard 작업이 아니라
로컬 Pandas 처리기였습니다.

## 2. 초기 실패와 해결

발생한 문제:

```text
Flink Kafka connector를 찾지 못했거나 버전이 맞지 않았습니다.
피처 처리 SQL 자체가 시작되지 않았습니다.
```

결론은 "가공은 됐는데 저장이 안 됐다"가 아닙니다. Kafka 기반 가공 단계가
시작되지 않은 상태였습니다.

초기 연구 경로의 해결 방법:

```text
파일 기반 배치 수집과 Pandas 검증을 먼저 사용
큰 API 응답 한 번에 의존하지 않고 OHLCV를 페이지 단위로 수집
원천 데이터를 삭제하기 전에 저장한 Parquet를 다시 열어 검증
```

이후 실제 PyFlink 배치 구현은 8절에 기록했습니다.

## 3. 2주 청크 시험

```powershell
python 1_chunk_downloader.py --start-date 2024-01-01 --days 14 --no-kafka
python 2_flink_processor.py
python 3_ml_training.py
```

결과:

```text
기간: 2024-01-01 ~ 2024-01-15
기대 행 수: 20,160
처리 행 수: 20,160
Parquet 검증: 통과
성공 표시 파일: 생성
검증 뒤 원천 CSV 삭제: 통과
초기 모델 시험 정확도: 약 49.16%
```

## 4. 과거 백필과 ML 연구

구현·실행한 주요 명령:

```powershell
python backfill_runner.py --symbol BTC/USDT --timeframe 1m --start-date 2024-01-01 --end-date 2024-04-01 --chunk-days 14 --no-kafka
python 4_triple_barrier_labeler.py
python 6_build_ml_dataset.py
python 7_train_direction_model.py
python 8_model_signal_backtest.py
```

확인한 기능:

- 14일 청크 반복과 성공 표시 파일 기반 재개·건너뛰기
- 파티션 Parquet Feature Store
- Triple Barrier 라벨
- ML 데이터셋 생성
- 초기 롱·숏·관망 방향 모델
- 오프라인 모델 신호 백테스트

중요한 결과:

```text
짧은 2주 결과는 좋아 보였습니다.
확장한 3개월 모델 신호 결과는 음수였습니다.
총수익률 약 -28.29%
최대 낙폭 약 -32.64%
```

따라서 현재 모델은 연구 기준선으로만 보관합니다.

## 5. USDT-M 선물 보조 데이터와 체결 데이터

확인한 결과:

```text
USDT-M OHLCV: 별도 선물 시장 경로로 수집
mark price와 funding rate: 수집·저장
과거 open interest: 공개 API가 오래된 2024년 요청을 거절
aggTrade: 최근 공개 API 범위에서만 수집·집계
호가창/open interest WebSocket 도구: 단발 캡처 확인, 연속 수집은 미구현
```

수집하지 못한 open interest 값은 0이나 가짜 값으로 바꾸지 않습니다.

## 6. 오프라인 리스크 재생

명령:

```powershell
python 12_paper_trading_risk_engine.py --market usdm --symbol BTC/USDT --account-balance 10000 --leverage 10 --liquidation-distance-pct 0.10 --min-confidence 0.65
```

기록된 결과:

```text
시작 자산: 10,000
종료 자산: 10,362.91
허용된 진입: 9
거절된 신호: 4,311
허용된 거래의 최대 실현 손실: 계좌 자산의 2%
```

한계: 이 재생은 뒤에 나온 캔들을 사용하는 Triple Barrier 결과를 썼습니다.
따라서 오프라인 리스크 엔진 시험일 뿐 실시간 페이퍼 트레이딩이 아닙니다.

## 7. Airflow 일일 배치 설정

구현한 설정:

```text
DAG ID: btcusdt_usdm_daily_collection
일정: 매일 00:15 UTC (09:15 KST)
Catchup: 사용
동시 DAG 실행 최대 수: 1
```

Dry-run 검증:

```powershell
python daily_collection_runner.py --data-date 2026-08-17 --dry-run
```

기대 결과:

```text
raw_download: dry_run
flink_features: dry_run
futures_context: dry_run
aggtrade: dry_run
```

## 8. 실제 PyFlink 배치 검증

로컬 서비스 빌드·시작:

```text
Airflow 화면: http://localhost:8080
Flink 화면: http://localhost:8081
```

고정 데이터 시험 명령:

```powershell
docker compose exec -T --workdir /opt/airflow/project airflow python flink_batch_submitter.py --raw-file /opt/airflow/project/flink_test_data/raw_BTCUSDT_USDM_1m_20240101_20240102.csv --feature-folder flink_test_output --keep-raw --timeout-seconds 300
```

성공 결과:

```text
작업 ID: 23600ba505b26342a8e97cac981141cc
Flink 상태: FINISHED
처리 행 수: 6
결과: 검증된 Parquet + 성공 표시 파일
```

원천 삭제 시험:

```text
같은 고정 데이터를 복사해 --keep-raw 없이 처리했습니다.
최종 Parquet 검증과 성공 표시 파일 생성 뒤 복사한 원천 파일이 실제로 삭제됐습니다.
```

Flink 시험 중 고친 문제:

| 문제 | 해결 |
| --- | --- |
| CSV 헤더를 데이터로 읽음 | 고정 스키마를 쓰는 헤더 없는 원천 CSV 사용 |
| TaskManager가 Windows 바인드 마운트 staging에 쓰지 못함 | Docker named volume `flink_staging` 사용 |
| Airflow와 TaskManager의 쓰기 권한이 다름 | 로컬 TaskManager 사용자와 권한을 맞추고 Flink 설정 경로를 쓰기 가능하게 수정 |
| Flink 출력에 `.csv` 확장자가 없음 | 최종 검증기가 `part-*` 파일을 읽도록 수정 |

## 9. 마지막 검증 명령

```powershell
python -m py_compile 1_chunk_downloader.py daily_collection_runner.py flink_batch_submitter.py flink_jobs\batch_feature_job.py
docker compose config --quiet
docker compose ps
```

최신 구현 당시 결과:

```text
Python 문법 검사: 통과
Docker Compose 구성 검사: 통과
Flink 개요: TaskManager 1개 / 사용 가능한 슬롯 1개
실제 고정 데이터 Flink 작업: 통과
```

## 10. 아직 실행하지 않은 것

- 새 Airflow -> 실제 Flink 경로로 완료된 실제 공개 API 날짜를 처리하는 일
- PC가 꺼져도 이어지는 재연결 호가창 수집
- Kafka 스트리밍 피처 처리
- 거래소 주문 상태를 반영하는 실시간 가격 페이퍼 트레이딩
- 실제 거래소 주문

다음 시험은 실거래 주문이 아니라, 완료된 하루를 Airflow로 실행하는 일입니다.

## 11. 2026-08-20 코드 감사와 보정

실제 저장 파일과 코드를 다시 대조해 다음 문제를 확인했습니다.

| 문제 | 확인 결과 | 수정 |
| --- | --- | --- |
| 백필·일일 처리기 불일치 | Pandas 또는 구버전 결과 마커를 현재 Flink 결과로 오인할 수 있었음 | `feature_store_v2` 분리 및 처리기·피처 스키마 버전 마커 검증 |
| 청크 경계 피처 단절 | Spot 7개, USDT-M 2개 청크 시작에서 `return_1m=0` | 직전 연속 4개 봉을 계산 컨텍스트로 사용 |
| 모델 백테스트 포지션 중복 | 구형 71거래 중 37구간 중복 | 포지션 종료 전 신규 진입 금지 |
| 비용 포함 손실 한도 | 구형 계산은 비용 포함 손실이 2%를 넘을 수 있었음 | 스탑·비용·추가 슬리피지를 합쳐 계좌 2%로 정규화 |
| 라벨·분할 시간 누수 | 불완전 미래 구간과 테스트 시작 가격이 일부 학습 라벨에 포함 가능 | 완전한 라벨 구간만 사용하고 분할 경계 240분 purge |
| 실행 시점 낙관 편향 | 신호 봉의 종가를 본 뒤 같은 종가에 즉시 진입한 것으로 계산 | 다음 봉 시가 진입으로 변경하고 신호·진입 시각 분리 |
| API 구간 완전성 미확인 | 일부 봉만 받아도 저장 단계까지 진행할 수 있었음 | OHLCV와 mark price의 예상·실제 타임스탬프가 완전히 같을 때만 저장 |
| aggTrade 페이지 경계 손실 가능성 | 타임스탬프에 1ms를 더하면 같은 밀리초의 다음 체결을 건너뛸 수 있음 | 첫 요청 뒤 집계 체결 `fromId`로 페이지 연결 및 ID 중복 제거 |
| funding 결측값 왜곡 | 숫자 변환 실패가 실제 0% funding처럼 해석될 수 있었음 | 변환 실패를 결측값으로 보존 |
| Docker 기본 서비스 과다 | 미사용 Kafka/ZooKeeper와 Airflow 웹 서버 중복 실행 | Kafka는 선택 프로필, Airflow standalone 하나로 축소 |

보정 백테스트 결과:

```text
입력: 기존 3개월 모델 예측 파일
거래 수: 79
포지션 중복: 0
승률: 44.30%
총수익률: -37.94%
최대 낙폭: -38.60%
거래 1회 최대 손실: -2.00%
```

이 보정 결과도 수정 전 경계 피처와 같은 봉 종가 진입 라벨로 학습한 모델을
사용했습니다. 다음 정식 검증은 수정된 PyFlink 피처를 새 저장소에 생성한 뒤
다음 봉 시가 라벨과 모델까지 다시 만드는 것입니다.

## 12. 2026-08-20 최종 정적 검증

| 검증 | 결과 |
| --- | --- |
| 전체 Python 파일 `py_compile` | 통과 |
| 다음 봉 시가 라벨 단위 시험 | 10행 중 완전 라벨 7행, 마지막 3행 제외, 진입가·시각 1봉 이동 확인 |
| 비중첩 모델 재생 단위 시험 | 보유 중 신호 1개 제외, 종료 뒤 재진입 확인 |
| aggTrade 동일 밀리초 페이지 경계 시험 | 1,001개 체결 모두 집계, 누락 0 |
| 구형 Feature Store 보호 | `feature_schema_version`이 없어 ML 데이터셋 생성을 의도대로 거부 |
| 구형 Flink 마커 보호 | 스키마 버전이 없는 마커를 재사용 불가로 판정 |
| 일일 실행 `--dry-run` | 2026-08-18 UTC 네 단계 구성 통과 |
| Docker Compose 구성 | 기본 4서비스, `streaming` 프로필 6서비스, 구성 검사 통과 |
| 저장 JSON | 35개 모두 파싱 통과 |
| Markdown 코드 펜스 | 홀수 개 파일 0 |
| Mermaid 코드 블록 | 독립 블록 3개, `flowchart` 3개, 펜스 균형 확인 |

Docker 컨테이너는 이전 종료 상태를 유지했습니다. 따라서 경계·스키마 버전 수정 뒤의
실제 공개 API 이틀 PyFlink 통합 실행은 아직 하지 않았습니다. 다음 실행은
`current_project_status_2026-08-19.md`의 8장 1단계입니다.
