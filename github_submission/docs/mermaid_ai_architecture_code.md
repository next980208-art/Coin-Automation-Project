# 아키텍처

수정일: 2026-08-20

각 코드 블록은 서로 독립적입니다. 원하는 그림의 코드 블록 하나만 복사해
Mermaid AI의 `Code` 패널에 붙여넣으면 됩니다.

기존 PNG 3개는 2026-08-17 당시의 목표 그림입니다. 현재 구현과 미구현 기능이
섞여 있으므로 최신 기준본으로 사용하지 않습니다. 아래 코드로 다시 출력한 그림을
최신 아키텍처로 사용합니다.

상태 색상은 다음 뜻입니다.

- 초록색: 구현하고 실행한 영역
- 노란색: 일부 구현했거나 재검증이 필요한 영역
- 회색: 앞으로 구현할 영역
- 빨간색: 현재 차단된 실거래 영역

## 1. 현재 실제 구현 상태

```mermaid
flowchart TB
    classDef done fill:#d9ead3,stroke:#38761d,stroke-width:2px,color:#111;
    classDef partial fill:#fff2cc,stroke:#bf9000,stroke-width:2px,color:#111;
    classDef planned fill:#eeeeee,stroke:#777,stroke-width:2px,color:#111;
    classDef blocked fill:#f4cccc,stroke:#990000,stroke-width:2px,color:#111;
    classDef store fill:#cfe2f3,stroke:#0b5394,stroke-width:2px,color:#111;
    classDef legacy fill:#ead1dc,stroke:#741b47,stroke-width:2px,color:#111;

    subgraph BATCH["현재 배치 연구 경로"]
        direction TB
        AIR["Airflow 일일 DAG<br/>구현 및 등록"]:::partial
        REST["Binance 공개 REST API<br/>USDT-M 1분봉"]:::done
        DOWN["청크 수집기<br/>헤더 없는 임시 CSV"]:::done
        CTX["이전 연속 4개 봉<br/>청크 경계 계산 컨텍스트"]:::partial
        FLINK["실제 PyFlink 배치<br/>ma_5 / return_1m"]:::done
        CHECK["품질 검사<br/>행 수 / 시간 / 피처 값"]:::done
        STORE["Parquet Feature Store v2<br/>OHLCV + 기본 피처"]:::store
        MARK["성공 마커 생성<br/>처리기 / 스키마 / 경계 상태"]:::done
        DEL["중복 임시 CSV 삭제<br/>OHLCV 값은 Parquet에 유지"]:::done

        AIR --> REST --> DOWN
        DOWN --> CTX --> FLINK --> CHECK
        CHECK -->|"통과"| STORE --> MARK --> DEL
        CHECK -.->|"실패"| KEEP["원천 CSV 보존<br/>작업 중단"]:::partial
    end

    subgraph RESEARCH["현재 오프라인 연구 경로"]
        direction TB
        LABEL["다음 봉 시가 Triple Barrier<br/>코드 수정 / 재생성 필요"]:::partial
        DATASET["ML 데이터셋<br/>구형 결과 재생성 필요"]:::partial
        MODEL["XGBoost 방향 모델<br/>구형 결과 재학습 필요"]:::partial
        TEST["비중첩 오프라인 백테스트<br/>코드 수정 / 재실행 필요"]:::partial
        RESULT["3개월 보정 결과<br/>수익성 없음"]:::partial

        STORE --> LABEL --> DATASET --> MODEL --> TEST --> RESULT
    end

    subgraph CONTEXT["선물 보조 데이터와 미시구조"]
        direction TB
        MF["mark price / funding<br/>시험 구간 저장"]:::partial
        AGG["aggTrade 1분 집계<br/>최근 공개 범위만"]:::partial
        LIVE["depth / open interest<br/>단발 캡처"]:::partial
    end

    subgraph NOTYET["아직 구현하지 않은 영역"]
        direction TB
        FIVE["5년 전체 PyFlink 백필"]:::planned
        STREAM["재연결 수집기<br/>Kafka / Flink Streaming"]:::planned
        PAPER["실시간 가격 기반<br/>페이퍼 트레이딩"]:::planned
        ORDER["거래소 실주문"]:::blocked
        FIVE --> STREAM --> PAPER --> ORDER
    end

    LEGACY["2_flink_processor.py<br/>이름과 달리 레거시 Pandas"]:::legacy
```

이 그림에서 `Airflow`는 구현·등록과 고정 데이터 검증까지 끝났지만, 수정된 경계
컨텍스트 코드를 포함한 실제 공개 API 하루 전체 실행은 다시 확인해야 하므로 노란색입니다.

## 2. 5년 과거 데이터 백필 목표 아키텍처

```mermaid
flowchart TB
    classDef source fill:#d9ead3,stroke:#38761d,stroke-width:2px,color:#111;
    classDef process fill:#ffffff,stroke:#333,stroke-width:2px,color:#111;
    classDef store fill:#cfe2f3,stroke:#0b5394,stroke-width:2px,color:#111;
    classDef gate fill:#fff2cc,stroke:#bf9000,stroke-width:2px,color:#111;
    classDef limit fill:#f4cccc,stroke:#990000,stroke-width:2px,color:#111;
    classDef note fill:#eeeeee,stroke:#777,stroke-width:1px,color:#111;

    subgraph AVAILABLE["공개 API로 과거 복구 가능한 데이터"]
        direction TB
        OHLCV["USDT-M OHLCV"]:::source
        MARK["mark price / funding rate"]:::source
    end

    LIMIT["무료 공개 API만으로 5년치<br/>호가창과 전체 체결 복구 불가"]:::limit

    SCHED["Airflow 또는 backfill_runner<br/>14일 청크 순차 실행"]:::process
    RAW["임시 원천 구역<br/>청크별 CSV"]:::store
    BOUNDARY["직전 연속 데이터 포함<br/>rolling 피처 경계 연결"]:::process
    FLINK["PyFlink Batch<br/>정제 + 버전이 있는 피처"]:::process
    QUALITY{"품질 관문 통과?"}:::gate
    CANON["정식 Parquet 저장소<br/>OHLCV + 피처 + 스키마 버전"]:::store
    MARKER["성공 마커 / manifest<br/>행 수 + 시간 범위 + 코드 버전"]:::store
    DELETE["검증된 중복 CSV만 삭제"]:::note
    QUAR["원천 보존 / 격리<br/>오류 원인 확인"]:::note

    OHLCV --> SCHED
    MARK --> SCHED
    SCHED --> RAW --> BOUNDARY --> FLINK --> QUALITY
    QUALITY -->|"통과"| CANON --> MARKER --> DELETE
    QUALITY -.->|"실패"| QUAR
    LIMIT -.-> FUTURE["지금부터 WebSocket으로 수집<br/>누락 구간 기록"]:::note

    subgraph ML["누수 방지 학습과 검증"]
        direction TB
        GAP["시간 누락 검사<br/>갭을 넘는 라벨 금지"]:::process
        LABEL["다음 봉 시가 Triple Barrier<br/>수수료 / 슬리피지 / 펀딩"]:::process
        PIT["시점 일치 데이터셋<br/>그 시점에 알 수 있던 값만"]:::process
        SPLIT["시간순 Train / Validation / Test<br/>워크포워드"]:::process
        TRAIN["후보 모델 학습"]:::process
        BACK["비중첩 백테스트<br/>동시 포지션 / MDD / 비용"]:::process
        PAPER["실시간 페이퍼 검증"]:::gate
        CAND["후보 모델 보관<br/>자동 실전 배포 금지"]:::store

        CANON --> GAP --> LABEL --> PIT --> SPLIT --> TRAIN --> BACK --> PAPER --> CAND
    end
```

핵심은 원천을 무조건 지우는 것이 아닙니다. OHLCV 값은 정식 Parquet에 그대로
남기고 중복 CSV만 지웁니다. 체결·호가 원천은 피처 정의와 재생 검사가 안정되기
전까지 짧은 보존 기간을 두는 편이 안전합니다.

## 3. 향후 실시간 페이퍼 트레이딩 아키텍처

```mermaid
flowchart TB
    classDef source fill:#d9ead3,stroke:#38761d,stroke-width:2px,color:#111;
    classDef stream fill:#fce5cd,stroke:#b45f06,stroke-width:2px,color:#111;
    classDef process fill:#ffffff,stroke:#333,stroke-width:2px,color:#111;
    classDef store fill:#cfe2f3,stroke:#0b5394,stroke-width:2px,color:#111;
    classDef gate fill:#fff2cc,stroke:#bf9000,stroke-width:2px,color:#111;
    classDef blocked fill:#f4cccc,stroke:#990000,stroke-width:2px,color:#111;

    WS["거래소 WebSocket<br/>aggTrade / depth / bookTicker"]:::source
    COL["재연결 수집기<br/>sequence 검사 / 누락 기록"]:::process
    KRAW["Kafka 원천 토픽<br/>내구성 / 재생 가능"]:::stream
    FSTR["Flink Streaming<br/>Event Time / Watermark / Checkpoint"]:::stream
    ONLINE["실시간 피처 스트림<br/>오프라인과 같은 정의"]:::process
    INFER["버전 고정 추론 서비스"]:::process
    RISK["독립 하드 리스크 관문<br/>계획 손실 2% / 증거금 / 청산 버퍼<br/>일일 중단 / Kill Switch"]:::gate
    PAPER["가상 주문 및 체결 모델<br/>실시간 페이퍼 트레이딩"]:::gate
    AUDIT["신호 / 거절 / 체결 / 지연 로그"]:::store

    WS --> COL --> KRAW --> FSTR --> ONLINE --> INFER --> RISK --> PAPER --> AUDIT
    ONLINE --> FEATURELOG["시점별 피처 로그"]:::store

    subgraph RETRAIN["통제된 재학습"]
        direction TB
        OFFTRAIN["예약 재학습<br/>실시간 즉시 학습 금지"]:::process
        WALK["워크포워드 + 비용 백테스트"]:::process
        SHADOW["Shadow / Paper 장기 비교"]:::gate
        APPROVE{"사람이 교체 승인?"}:::gate
        REG["버전 모델 저장소<br/>롤백 가능"]:::store

        FEATURELOG --> OFFTRAIN
        AUDIT --> OFFTRAIN
        OFFTRAIN --> WALK --> SHADOW --> APPROVE
        APPROVE -->|"승인"| REG --> INFER
        APPROVE -.->|"거절"| OFFTRAIN
    end

    LIVEGATE{"실거래 전 별도 승인과<br/>장기 페이퍼 기준 통과?"}:::gate
    ORDER["주문 상태 관리<br/>reduce-only stop / 재조정"]:::blocked
    EXCHANGE["거래소 주문 API"]:::blocked

    PAPER -.-> LIVEGATE
    LIVEGATE -.->|"현재 차단"| ORDER -.-> EXCHANGE
```

이 구조에서는 머신러닝이 계좌 손실 한도를 바꿀 수 없습니다. 모델은 신호 후보를
만들고, 독립된 리스크 관문이 포지션 크기와 거래 허용 여부를 결정합니다. 새 모델도
백테스트 결과 하나만으로 자동 교체하지 않습니다.

## 4. 기존 PNG에서 잘못 보였던 부분

| 기존 표현 | 문제 | 수정 방향 |
| --- | --- | --- |
| REST API에서 5년치 체결·호가 수집 | 무료 공개 API로 전체 과거 호가창을 복구할 수 없음 | 복구 가능한 배치 데이터와 지금부터 쌓는 WebSocket 데이터를 분리 |
| Feature Processor가 RSI·VWAP·Imbalance 생성 | 현재 실제 PyFlink는 `ma_5`, `return_1m`만 생성 | 현재 피처와 목표 피처를 별도 표시 |
| Model Registry에 검증된 모델 존재 | 현재는 `models/` 파일뿐이며 수익성이 검증되지 않음 | 후보 모델 보관으로 표시 |
| 새 모델이 기존 모델보다 좋으면 즉시 교체 | 백테스트 과적합과 운영 장애 위험 | 워크포워드, Shadow/Paper, 사람 승인, 롤백 추가 |
| 주문 실행 봇에서 거래소 API로 직결 | 리스크·주문 상태·Kill Switch 관문 부족 | 독립 하드 리스크와 페이퍼 단계를 먼저 배치 |
| 원천 데이터 전체 삭제 | 재가공 불가능 위험 | 정식 Parquet에 원천 필드를 유지하고 중복 임시 파일만 삭제 |
