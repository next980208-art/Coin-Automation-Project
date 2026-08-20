# BTCUSDT 선물 데이터 파이프라인 및 자동매매 연구

> 제출용 구축 현황 보고서 | 최종 정리일: 2026-08-20

이 프로젝트는 BTCUSDT 선물 시장 데이터를 수집하고, 데이터를 학습 가능한 형태로 가공한 뒤, 머신러닝 연구와 위험 관리형 자동매매로 이어가기 위한 데이터·ML 파이프라인입니다.

**현재 단계:** 과거 데이터 수집, PyFlink 피처 가공, 라벨링, 모델 학습, 연구용 백테스트의 기반을 구축하고 검증하는 단계입니다. 실제 주문을 전송하는 자동매매는 아직 구현하거나 실행하지 않았습니다.

## 최종적으로 만들 시스템

최종 시스템은 두 개의 데이터 흐름을 함께 사용합니다.

```text
[과거 데이터 구축]
Binance API -> 14일 청크 수집 -> PyFlink 가공 -> Feature Store
            -> 모델 학습 -> 워크포워드 백테스트

[실시간 운영]
거래소 WebSocket -> 실시간 피처 가공 -> 학습 완료 모델 추론
                -> 위험 관리 -> 모의 주문 -> 충분한 검증 후 실거래 검토
```

과거 데이터 흐름은 모델을 학습하고 검증하기 위한 길입니다. 실시간 흐름은 학습이 끝난 모델이 현재 시장에서 신호를 만들고, 먼저 모의투자로 안전성을 확인하기 위한 길입니다. 두 흐름의 피처 계산 방식과 저장 형식은 가능한 한 같게 유지합니다.

## 현재까지 구축한 부분

### 1. 과거 데이터 수집 구조

- BTCUSDT 1분봉을 날짜 범위와 청크 단위로 내려받는 수집기를 만들었습니다.
- 요청한 모든 1분 시각이 실제 파일에 있는지 확인합니다. 한 개라도 빠지면 불완전한 결과를 저장하지 않도록 막았습니다.
- 대용량 5년 데이터를 한 번에 받지 않고, **14일 단위 청크**로 수집·처리·검증하도록 구성했습니다.
- USDT-M 선물 OHLCV, 마크 프라이스, 펀딩비, 최근 체결(aggTrade)을 수집할 수 있는 별도 경로를 만들었습니다.

### 2. PyFlink 데이터 가공 구조

- 원천 CSV를 PyFlink 작업으로 읽어 `ma_5`, `return_1m` 같은 피처를 만들도록 구성했습니다.
- 피처 결과는 분석과 학습에 적합한 **Parquet** 형식으로 `feature_store_v2`에 저장하도록 분리했습니다.
- 14일 청크의 첫 부분도 정확하게 계산되도록, 이전 청크의 마지막 4개 봉을 계산 문맥으로 포함합니다.
- 고정된 6행 테스트 데이터로 PyFlink 실행과 피처 계산을 확인했습니다.
- 기존의 구형 결과와 수정 후 결과를 섞지 않기 위해 새 파이프라인은 모두 `_v2` 저장소를 사용합니다.

### 3. 일일 자동 수집 준비

- Airflow DAG와 일일 실행 도구를 구성했습니다.
- PC가 계속 켜져 있지 않아도, 켠 날에 이전 누락 날짜를 확인하고 수집하는 방향으로 설계했습니다.
- 현재는 `--dry-run`으로 수집·가공 단계 연결을 사전 점검할 수 있습니다.
- Docker Compose에 Airflow와 PyFlink 작업 환경을 정의했습니다.

### 4. 머신러닝 연구 파이프라인

- 피처 데이터에서 미래 가격 움직임을 기준으로 라벨을 만드는 코드를 구현했습니다.
- 진입 가격은 같은 봉 종가가 아니라 **다음 봉 시가**를 사용하도록 수정해, 미래 정보를 미리 본 것처럼 계산되는 문제를 줄였습니다.
- 미래 관찰 구간이 부족한 마지막 데이터는 학습에서 제거합니다.
- 학습 데이터와 검증 데이터는 시간 순서대로 나누고, 경계 주변 데이터는 제거하도록 구성했습니다.
- 방향성 모델 학습과 모델 신호 기반 연구용 백테스트 코드를 연결했습니다.

### 5. 위험 관리와 백테스트 보완

- 한 거래의 최대 허용 손실을 계좌 기준 **2%**로 제한하는 규칙을 반영했습니다.
- 수수료와 슬리피지를 반영할 수 있게 했습니다.
- 동시에 여러 포지션이 열려 실제보다 수익이 과대 계산되는 문제를 막았습니다.
- 일일 손익은 진입일이 아니라 실제 청산일에 반영하도록 수정했습니다.

## 현재 구현 상태

| 단계 | 현재 상태 | 의미 |
| --- | --- | --- |
| 14일 청크 수집 | 구현 완료 | 데이터 누락을 검사하며 과거 1분봉을 저장할 수 있습니다. |
| PyFlink 피처 가공 | 기본 실행 확인 | 고정 테스트에서는 통과했고, 공개 API 1~2일 범위의 v2 통합 실행이 남았습니다. |
| Feature Store v2 | 구조 완료 | 새 피처의 저장 위치와 성공 마커 검증 규칙을 분리했습니다. |
| Airflow 일일 수집 | 구조 구현 | DAG와 사전 점검은 가능하며, 실제 하루 전체 실행 확인이 남았습니다. |
| 라벨·데이터셋·모델 | 코드 보완 완료 | v2 피처가 쌓인 후 새 데이터로 재생성·재학습해야 합니다. |
| 백테스트 | 코드 보완 완료 | 새 v2 모델을 이용한 유효한 워크포워드 결과는 아직 없습니다. |
| 호가창·실시간 체결 | 향후 수집 | 과거 전체 복원은 공개 API 한계가 있어, 앞으로 실시간으로 쌓아야 합니다. |
| 실시간 추론·모의투자·실거래 | 미구현 | 데이터와 모델 검증이 끝난 뒤 순서대로 구축합니다. |

## 지금 동작하는 데이터 흐름

```text
원천 1분봉 CSV
  -> PyFlink 피처 생성
  -> Parquet Feature Store v2
  -> 라벨 생성
  -> ML 데이터셋 생성
  -> 방향성 모델 학습
  -> 연구용 백테스트
```

이 흐름의 코드는 마련되어 있지만, 현재 저장된 과거 결과 일부는 수정 전 구형 저장소에서 만든 기록입니다. 따라서 새 `feature_store_v2` 데이터를 처음부터 생성한 뒤, 라벨·모델·백테스트를 다시 실행해야 현재 구조의 성능을 판단할 수 있습니다.

## 확인한 결과와 남은 검증

다음 항목은 코드 또는 단위 수준에서 확인했습니다.

- 프로젝트 Python 파일 문법 검사 통과
- 미래 봉이 부족한 라벨 데이터 제거 확인
- 다음 봉 시가 기준 진입으로 변경된 것 확인
- 겹치는 포지션을 제외하는 백테스트 동작 확인
- 동일 밀리초 체결 1,001건의 페이지 경계 누락 방지 확인
- Airflow 일일 흐름 `--dry-run` 4단계 통과
- Docker Compose 구성 문법 검사 통과

아직 Docker 엔진을 실행하지 않은 상태이므로, 컨테이너 안에서 PyFlink와 Binance 공개 API를 연결한 **v2 전체 통합 실행**은 다음 검증 작업입니다.

## 최종 완성까지의 진행 계획

### 단계 1. v2 배치 파이프라인 실제 실행

Docker 환경에서 1~2일치 데이터를 수집하고 PyFlink로 가공합니다. 성공 마커, 행 수, 시간 누락 여부, 피처 값까지 확인합니다.

### 단계 2. 5년치 학습용 데이터 축적

USDT-M 1분봉을 14일 청크로 반복 수집합니다. 각 청크가 성공한 뒤에만 원천 데이터 정리 여부를 결정하며, 학습에 필요한 피처 Parquet와 품질 기록은 보존합니다.

### 단계 3. 모델 재학습과 엄격한 평가

v2 피처로 라벨과 학습 데이터셋을 새로 만들고, 시간 순서 기반 워크포워드 백테스트를 실행합니다. 누적 수익률만 보지 않고 최대 낙폭, 거래 비용, 연속 손실, 손절 주문의 현실성도 함께 평가합니다.

### 단계 4. 실시간 데이터 축적

WebSocket으로 호가창, 체결, 선물 문맥 데이터를 실시간 저장합니다. 과거 공개 API로 복원할 수 없는 호가창 정보는 이 단계부터 장기적으로 쌓습니다.

### 단계 5. 실시간 모의투자

학습 완료 모델의 신호를 실시간 피처에 적용하고, 거래소 주문 없이 가상 체결과 위험 관리만 수행합니다. 이 단계에서 데이터 지연, 주문 가능 가격, 손절 작동, 일일 손실 제한을 검증합니다.

### 단계 6. 실거래 검토

장기간 모의투자 결과가 기준을 충족할 때만 실거래 연결을 별도 작업으로 검토합니다. 실거래에서도 거래당 계좌 기준 최대 손실 2%, 손절 주문, 일일 손실 한도, 비상 중지 기능을 강제합니다.

## 과거 백테스트 결과에 대한 주의

이전에 만든 3개월 백테스트는 구형 피처와 구형 라벨을 사용했습니다. 중복 포지션을 보정한 과거 기록은 79회 거래, 누적 -37.94%, 최대 낙폭 -38.60%였으며, 이는 현재 구조의 성능이 아닙니다.

즉, 지금 단계에서는 수익이 검증된 모델이 아니라 **수익을 과장하지 않도록 데이터와 평가 방식을 고친 연구 기반**이 구축된 상태입니다. 새 v2 파이프라인 전체 실행과 재학습·워크포워드 결과가 나온 뒤에만 성능을 판단합니다.

## 구현 파일

| 경로 | 역할 |
| --- | --- |
| `1_chunk_downloader.py` | OHLCV 청크 수집과 시간 누락 검증 |
| `backfill_runner.py` | 14일 단위 백필 실행 |
| `flink_batch_submitter.py`, `flink_jobs/` | PyFlink 피처 가공과 검증 |
| `dags/`, `daily_collection_runner.py` | Airflow 기반 일일 수집 준비 |
| `4_triple_barrier_labeler.py` | 다음 봉 시가 기준 라벨 생성 |
| `6_build_ml_dataset.py`, `7_train_direction_model.py` | 학습 데이터와 방향성 모델 생성 |
| `8_model_signal_backtest.py` | 비용·중복 포지션·위험 규칙을 반영한 백테스트 |

## 파일 경로

[README.md](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\README.md)
전체 목표, 현재 구축 상태, 최종 계획을 먼저 봅니다.

[mermaid_ai_architecture_code.md](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\docs\\mermaid_ai_architecture_code.md)
데이터가 수집부터 학습·모의투자까지 어떻게 흐르는지 그림으로 이해합니다.

[1_chunk_downloader.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\1_chunk_downloader.py) → [backfill_runner.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\backfill_runner.py)
과거 1분봉을 14일 청크로 수집하고 누락을 검사하는 부분입니다.

[flink_batch_submitter.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\flink_batch_submitter.py) → [batch_feature_job.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\flink_jobs\\batch_feature_job.py)
원천 데이터를 PyFlink로 가공해 피처 Parquet로 저장하는 핵심 부분입니다.

[daily_collection_runner.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\daily_collection_runner.py) → [btcusdt_daily_collection.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\airflow\\dags\\btcusdt_daily_collection.py)
매일 또는 PC를 켠 날 누락 데이터를 수집·가공하는 자동화 구조입니다.

[4_triple_barrier_labeler.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\4_triple_barrier_labeler.py) → [6_build_ml_dataset.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\6_build_ml_dataset.py) → [7_train_direction_model.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\7_train_direction_model.py)
피처 데이터에 정답을 붙이고, 학습용 데이터셋을 만들고, 모델을 학습하는 흐름입니다.

[8_model_signal_backtest.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\8_model_signal_backtest.py) → [12_paper_trading_risk_engine.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\12_paper_trading_risk_engine.py)
모델 신호를 백테스트하고, 거래당 최대 손실 2% 위험 규칙을 적용하는 부분입니다.

[9_futures_context_collector.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\9_futures_context_collector.py) → [10_aggtrade_collector.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\10_aggtrade_collector.py) → [11_realtime_market_capture.py](C:\\Users\\next9\\OneDrive\\바탕 화면\\coin_project\\github_submission\\11_realtime_market_capture.py)
펀딩비·마크프라이스·체결·실시간 데이터로 확장하는 다음 단계입니다.
