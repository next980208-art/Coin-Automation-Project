# BTC 자동매매 연구 프로젝트: 현재 상태

최종 점검일: 2026-08-20

## 1. 결론부터 보기

현재 프로젝트는 BTCUSDT 과거 데이터를 수집하고, PyFlink 또는 레거시 Pandas로
기본 피처를 만들고, Parquet에 저장한 뒤 라벨링·모델 학습·오프라인 백테스트까지
실행할 수 있습니다.

하지만 아직 자동매매 프로그램은 아닙니다. 5년치 데이터, 연속 호가창 수집,
Flink Streaming, 실시간 추론, 실시간 페이퍼 트레이딩, 거래소 주문은 완성되지
않았습니다. 기존 모델 시험은 손실이었고, 수정 코드로 다시 학습한 유효 모델은 아직 없습니다.

2026-08-20 점검에서 데이터 경계와 백테스트 방식의 오류를 찾아 코드를 수정했습니다.
기존 저장 데이터와 모델은 수정 전 코드로 만들어졌으므로 연구 이력으로만 보관하고,
수정된 PyFlink 경로로 다시 생성해야 합니다.

## 2. 안전 정책

```text
거래 1회당 계획상 계좌 예상 손실 한도: 비용과 예상 슬리피지를 포함해 2%
목표 증거금 모드: Isolated
포지션 진입 시 스탑 필수
청산을 손절 방법으로 사용하지 않음
동시 포지션: 현재 연구 기준 1개
실거래 주문: 미구현 및 차단
```

레버리지는 포지션 크기를 바꾸는 도구이며, 계좌 손실 한도를 정하는 규칙이 아닙니다.
명목 포지션은 스탑 거리, 수수료, 예상 슬리피지, 펀딩비, 청산 거리, 계좌 위험 예산을
사용해 계산해야 합니다.

## 3. 실제 구현 상태

| 영역 | 상태 | 정확한 의미 |
| --- | --- | --- |
| Spot OHLCV 백필 | 과거 실행 완료 | 2024년 3개월 데이터가 있으나 레거시 Pandas 피처임 |
| USDT-M OHLCV 백필 | 시험 실행 완료 | 15일 데이터가 있으나 레거시 Pandas 피처임 |
| 실제 PyFlink 배치 | 고정 데이터 실행 완료 | 6행 작업이 Flink에서 `FINISHED` 됨 |
| 청크 경계 연결 | 코드 수정·단위 확인 | 이전 연속 4개 봉 로드는 확인, Docker 전체 재실행 필요 |
| Airflow 일일 DAG | 구현·등록 | 실제 공개 API 하루 전체를 수정 코드로 재검증해야 함 |
| mark price / funding | 시험 구간 저장 | 과거 open interest는 API 제한으로 비어 있음 |
| aggTrade | 최근 구간만 부분 확인 | 5년 전체를 무료 공개 API로 복구할 수 없음 |
| 호가창 / open interest | 단발 캡처 | 재연결과 장기 적재는 미구현 |
| 라벨 / ML 데이터셋 / 모델 | 코드 보정·재생성 대기 | 기존 저장 결과는 경계·누수·진입 시점 수정 전 이력 |
| 오프라인 백테스트 | 코드 보정·재실행 대기 | 구형 예측으로 중복 제거만 확인, 정식 성과 아님 |
| 실시간 페이퍼 트레이딩 | 미구현 | 저장된 미래 라벨 재생만 존재 |
| Kafka / Flink Streaming | 미구현 | Docker `streaming` 프로필만 준비 |
| 실거래 주문 | 차단 | 주문 전송 코드 없음 |

## 4. 이번 점검에서 발견하고 고친 문제

### 4-1. 5년 백필이 실제로는 Pandas를 호출하던 문제

이전 `backfill_runner.py`는 이름과 문서상 백필 처리기였지만 내부에서는
`2_flink_processor.py`를 호출했습니다. 이 파일은 Apache Flink가 아니라 Pandas입니다.

수정 내용:

- `backfill_runner.py` 기본 처리기를 `flink`로 변경
- 실제 `flink_batch_submitter.py`를 호출
- Flink 입력에는 자동으로 `--no-header --no-kafka` 적용
- 레거시 시험만 `--processor pandas`로 명시
- 성공 마커의 `processor` 값을 확인해 Pandas 결과를 Flink 결과로 잘못 건너뛰지 않음
- Airflow 일일 실행도 `feature_store_v2`를 기본으로 사용하고 Flink 처리기 마커를 검증
- 호스트에서 잘못 실행하면 API 수집 전에 Docker 컨테이너 실행 방법을 안내하고 중단

### 4-2. 청크 시작점에서 피처가 끊기던 문제

기존 청크마다 `ma_5`가 첫 가격 하나로 다시 시작하고 `return_1m`이 0으로
초기화됐습니다. 실제 저장 파일을 확인한 결과 다음과 같았습니다.

```text
Spot 3개월: 7개 청크 시작점에서 return_1m = 0
USDT-M 15일: 2개 청크 시작점에서 return_1m = 0
```

수정 내용:

- 새 청크 처리 전에 같은 시장·심볼·시간 단위의 직전 연속 4개 봉을 읽음
- 이전 봉은 rolling 계산에만 쓰고 현재 청크 결과에는 저장하지 않음
- `ma_5`, `return_1m`을 Pandas 기준값과 다시 비교
- 성공 마커에 `boundary_context_rows`와 `boundary_context_status` 기록
- 2024-01-02 시작 시험 입력에서 이전 4개 봉 로드 확인

기존 Parquet는 자동 수정하지 않았습니다. 라벨과 모델까지 연결된 연구 이력을 갑자기
덮어쓰지 않기 위해서입니다. 먼저 새 폴더에서 2일 이상 재검증한 다음 재생성합니다.

### 4-3. 백테스트에서 포지션이 겹치던 문제

구형 3개월 결과는 71개 거래 중 37개가 이전 포지션 종료 전에 다시 진입했습니다.
또한 라벨 비용이 포함된 손익에 계좌 2%를 그대로 곱해, 스탑 손실이 비용 포함 2%를
넘을 수 있었습니다.

수정 내용:

- 한 포지션 종료 전 신규 신호 제외
- `holding_minutes`로 실제 종료 시각 기록
- 왕복 비용과 추가 슬리피지를 포함해 스탑 손실이 계좌 2%가 되도록 정규화
- `--risk-per-trade-pct`가 2%를 넘으면 실행 거부
- 최대 거래 손실과 포지션 중복 제외 횟수를 결과에 기록

### 4-4. Docker가 사용하지 않는 서비스를 항상 실행하던 문제

기존 구성은 배치 처리만 할 때도 Kafka와 ZooKeeper를 항상 시작했고,
`airflow standalone`과 별도 웹 서버를 동시에 실행했습니다.

수정 내용:

- 기본 시작 서비스는 Airflow 1개, Flink JobManager, Flink TaskManager로 축소
- Airflow `standalone` 컨테이너가 8080 웹 화면도 담당
- Kafka와 ZooKeeper는 `streaming` 프로필을 지정할 때만 시작
- 첫 설치에서도 staging 초기화 이미지가 빌드되도록 Compose 수정

### 4-5. 라벨 미래 구간과 학습 분할 경계의 시간 누수

기존 라벨러는 데이터 마지막 부분처럼 최대 보유 시간만큼 미래 캔들이 없는 행도
짧아진 미래 구간으로 라벨링했습니다. 또한 시간순 80/20 분할은 했지만, 학습 구간
마지막 라벨이 테스트 시작 뒤 최대 240분 가격을 볼 수 있었습니다.

수정 내용:

- 최대 보유 구간 전체가 존재하는 행만 라벨로 저장
- 중간 시간 누락을 가로지르는 라벨 제외
- `label_horizon_complete`가 없는 구형 라벨은 ML 데이터셋 생성 거부
- 학습·테스트 경계에서 최대 보유 시간만큼 학습 행 purge
- 학습 메타데이터에 제거한 행 수와 purge 시간을 기록

이 수정 때문에 기존 라벨과 ML 데이터셋은 다시 만들어야 합니다. 이는 의도적인
보호 장치입니다.

### 4-6. 일부 API 응답도 성공으로 저장될 수 있던 문제

기존 수집기는 페이지를 반복해서 요청했지만, 거래소가 중간 데이터를 누락하거나
일찍 빈 응답을 반환해도 받은 행만 저장할 수 있었습니다. 또한 funding rate를
숫자로 바꾸지 못한 경우 `0`으로 오해할 가능성이 있었습니다.

수정 내용:

- OHLCV와 mark price는 요청 구간의 예상 타임스탬프와 실제 타임스탬프를 정확히 비교
- 빠진 봉, 중복으로 인한 불일치, 구간 밖 봉이 하나라도 있으면 성공 저장 전에 중단
- aggTrade는 첫 시간 요청 뒤 `fromId`로 이어받아 같은 밀리초의 페이지 경계 체결을 보존
- funding rate 변환 실패 값은 시장 중립을 뜻하는 `0`이 아니라 결측값으로 보존
- 기초 R 백테스트도 `label_horizon_complete`가 없는 구형 라벨 사용을 거부

기존 파일은 이 검사를 통과했다는 보증이 없으므로 새 `feature_store_v2` 재생성 결과를
정식 기준으로 사용합니다.

### 4-7. 신호가 확정된 종가에 즉시 체결됐다고 가정한 문제

기존 라벨은 한 분봉의 `close`, `high`, `low`를 피처로 사용하면서 같은 봉의 종가를
진입가로 사용했습니다. 실제로 그 봉이 끝나야 피처를 알 수 있으므로 정확히 그 종가에
이미 체결됐다고 보는 것은 낙관적인 실행 가정입니다.

수정 내용:

- 신호 봉의 모든 값이 확정된 뒤 `다음 봉 시가`를 진입가로 사용
- `entry_timestamp`, `entry_price`, `entry_rule=next_bar_open`을 라벨에 저장
- 모델 백테스트와 위험 재생은 신호 시각과 진입 시각을 분리
- 포지션 손익은 진입일이 아니라 종료일의 일간·주간 손실 한도에 반영
- 새 진입 메타데이터가 없는 구형 라벨·데이터셋·예측 파일은 실행 거부

따라서 아래 3개월 수치는 포지션 중복 제거 효과를 확인한 중간 기록일 뿐이며,
다음 봉 시가 규칙까지 반영한 새 성과 수치는 아직 없습니다.

## 5. 보정한 3개월 모델 백테스트

구형 결과:

```text
거래 수: 71
포지션 중복: 37개 거래 구간
총수익률: -28.29%
최대 낙폭: -32.64%
```

포지션 중복과 위험 계산만 수정한 뒤 같은 구형 예측 파일을 다시 재생한 결과:

```text
거래 수: 79
포지션 중복: 0
승률: 44.30%
평균 위험 정규화 R: -0.2955
총수익률: -37.94%
최대 낙폭: -38.60%
거래 1회 최대 손실: -2.00%
포지션 보유 중 제외한 신호: 5,216
```

거래 수가 늘어난 이유는 중복 제거 뒤 일일 손실 중단 시점과 진입 후보가 달라졌기
때문입니다. 결과는 더 현실적인 방향으로 나빠졌고, 현재 전략에 수익성이 없다는
결론은 더 분명해졌습니다.

이 결과도 수정 전 청크 피처, 같은 봉 종가 진입 라벨, 기존 모델 예측을 사용한
폐기 예정 연구 이력입니다. 경계 수정 피처와 다음 봉 시가 라벨로 모델을 다시 만든 뒤
최종 비교해야 합니다.

결과 파일:

```text
docs/model_signal_backtest_report_3m_corrected_2026-08-20.md
docs/model_signal_backtest_report_3m_corrected_2026-08-20.json
docs/model_signal_backtest_report_3m_corrected_2026-08-20_trades.parquet
```

## 6. 현재 데이터 저장 현황

```text
feature_store: 22개 파일
label_store: 4개 파일
ml_dataset: 4개 파일
models: 5개 파일
futures_context_store: 2개 파일
trade_context_store: 2개 파일
live_context_store: 2개 파일
flink_test_output: 2개 파일
paper_trading: 6개 파일
```

위 목록은 모두 수정 전 연구 이력입니다. 수정된 스크립트의 기본 출력은
`feature_store_v2`, `futures_context_store_v2`, `trade_context_store_v2`,
`label_store_v2`, `ml_dataset_v2`, `models_v2`, `predictions_v2`,
`live_context_store_v2`, `paper_trading_v2`로 분리했습니다. 아직 정식 v2 실행
산출물은 없습니다.

Parquet Feature Store에는 `open`, `high`, `low`, `close`, `volume` 원천 필드도
남아 있습니다. 삭제되는 것은 검증이 끝난 중복 임시 CSV입니다. 다만 체결·호가처럼
나중에 피처 정의를 바꿀 가능성이 큰 고빈도 원천은 즉시 삭제하지 말고 보존 기간과
재생 검사를 먼저 정해야 합니다.

## 7. 수정된 현재 아키텍처

정확한 그림 코드는 `mermaid_ai_architecture_code.md`에 있습니다. 세 그림을 분리했습니다.

1. 현재 실제 구현 상태
2. 5년 과거 데이터 백필 목표
3. 향후 실시간 페이퍼 트레이딩과 통제된 재학습

기존 PNG 3개는 다음 이유로 최신 기준본이 아닙니다.

- 실제로 없는 RSI·VWAP·호가 불균형 피처가 완료처럼 표시됨
- Pandas와 실제 PyFlink가 구분되지 않음
- 검증되지 않은 모델을 Model Registry의 검증 모델로 표시함
- 실시간 페이퍼 관문 없이 거래소 주문 API로 연결됨
- 새 모델을 자동 교체해 롤백과 사람 승인 단계가 없음

## 8. 다음 실행 순서

### 1단계: 수정된 PyFlink 경계를 새 폴더에서 검증

Docker를 시작한 뒤 이틀을 하루 청크로 처리합니다.

```powershell
PowerShell -ExecutionPolicy Bypass -File .\scripts\start_airflow_batch.ps1 -Build

docker compose exec -T airflow python backfill_runner.py --processor flink --market usdm --symbol BTC/USDT --timeframe 1m --start-date 2024-01-01 --end-date 2024-01-03 --chunk-days 1 --feature-folder feature_store_v2
```

둘째 날 성공 마커에서 다음 값을 확인합니다.

```text
processor = apache_flink_pyflink_batch
feature_schema_version = ohlcv_basic_v2_boundary4
boundary_context_rows = 4
boundary_context_status = applied_4_rows
```

### 2단계: 3개월 USDT-M 재생성

1단계가 통과하면 `feature_store_v2`에 3개월을 14일 청크로 다시 수집합니다.
그다음 라벨, ML 데이터셋, 방향 모델, 보정 백테스트를 모두 새 출력 폴더로 생성합니다.

```powershell
docker compose exec -T airflow python backfill_runner.py --processor flink --market usdm --symbol BTC/USDT --timeframe 1m --start-date 2024-01-01 --end-date 2024-04-01 --chunk-days 14

docker compose exec -T airflow python 9_futures_context_collector.py --symbol BTC/USDT --timeframe 1m --start-date 2024-01-01 --end-date 2024-04-01

docker compose exec -T airflow python 4_triple_barrier_labeler.py --market usdm --symbol BTCUSDT --timeframe 1m

docker compose exec -T airflow python 6_build_ml_dataset.py --market usdm --symbol BTCUSDT --timeframe 1m

docker compose exec -T airflow python 7_train_direction_model.py --market usdm --symbol BTCUSDT --timeframe 1m

docker compose exec -T airflow python 8_model_signal_backtest.py --market usdm --symbol BTCUSDT --timeframe 1m
```

각 명령이 성공한 뒤에만 다음 명령으로 진행합니다. 실패 시 `_v2` 성공 마커와 오류를
확인하고, 구형 폴더의 파일로 대신 넘어가지 않습니다.

### 3단계: 워크포워드 평가 강화

- 데이터 시간 누락 검사
- 누락 구간을 넘는 Triple Barrier 라벨 금지
- Train / Validation / Test 3구간 분리
- 여러 구간을 이동하는 워크포워드
- 수수료, 슬리피지, 펀딩비, 동시 포지션, 일일 손실 중단 반영

### 4단계: 5년 OHLCV와 선물 컨텍스트 백필

3개월 재검증을 통과한 뒤에만 5년으로 확장합니다. 공개 API로 복구할 수 없는 5년치
호가창과 전체 체결은 없는 값을 만들지 않습니다.

### 5단계: 지금부터 실시간 미시구조 데이터 축적

PC가 켜진 동안 WebSocket 재연결 수집기로 aggTrade·depth·bookTicker를 저장하고
누락 구간을 기록합니다. Kafka와 Flink Streaming은 이 수집기가 장기 안정화된 뒤
연결합니다.

### 6단계: 실시간 페이퍼 트레이딩

현재 가격과 가상 주문 상태로 최소 수 주 이상 검증합니다. 모델은 신호 후보만 만들고,
독립 리스크 엔진이 계좌 2% 한도, 청산 버퍼, 일일 중단, Kill Switch를 강제합니다.

## 9. 절대 오해하면 안 되는 부분

- 현재 프로젝트는 수익성이 증명된 시스템이 아닙니다.
- 현재 프로젝트는 실시간 자동매매 시스템이 아닙니다.
- 스탑 주문도 급변 시 정확한 가격 체결을 보장하지 않습니다.
- 2%는 목표 위험 한도이며 시장 갭과 체결 실패까지 완전히 보장하지 않습니다.
- 머신러닝이 스스로 계좌 손실 한도를 정하거나 새 모델을 실전에 자동 배포하면 안 됩니다.
- 무료 공개 API만으로 과거 5년의 완전한 호가창을 복원할 수 없습니다.
