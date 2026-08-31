# Flink Checkpoint 재시작·Exactly-Once 검증 기록

실행일: 2026-08-29 KST  
대상: `BTCUSDT` 실시간 1분 피처  
안전 상태: 페이퍼 전용, 실거래 주문 API 없음

## 1. 작업 목적

이전 단계에서 event time, watermark, 허용 지연 처리를 구현했다. 그러나 Job이 정상 실행되는
것만으로 장애 복구가 검증되는 것은 아니다. TaskManager가 종료됐다가 다시 시작될 때 다음을
확인해야 한다.

1. Flink가 마지막 성공 checkpoint에서 Kafka offset과 1분 집계 상태를 복원하는가?
2. 장애 직전 피처가 재전송돼 같은 `feature_id`가 중복되지 않는가?
3. 장애 중 수집된 원천 이벤트를 다시 읽어 1분 피처 공백을 채우는가?
4. 아직 commit되지 않은 Kafka transaction을 downstream이 읽지 않는가?

## 2. 기존 설정에서 발견한 위험

Flink checkpoint 자체는 다음과 같이 정상 작동하고 있었다.

```text
completed: 4
failed: 0
checkpoint interval: 60초
```

하지만 `KafkaSink`의 기본 delivery guarantee는 `NONE`이었다. 이 상태에서는 Flink state와
Kafka source offset을 복구하더라도 출력 메시지가 checkpoint transaction에 포함되지 않아
재시작 경계의 중복을 방지한다고 말할 수 없다.

## 3. 적용한 변경

### Kafka sink를 exactly-once로 변경

`flink_jobs/realtime_kafka_feature_job.py`에 다음 설정을 적용했다.

```python
.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
.set_transactional_id_prefix("realtime-features-v1")
.set_property("transaction.timeout.ms", "300000")
```

- Flink가 피처를 Kafka transaction으로 기록한다.
- checkpoint가 완료될 때 해당 transaction도 commit된다.
- 장애 중 미완료 transaction은 복구 과정에서 commit되지 않거나 abort된다.
- transactional ID prefix는 재시작해도 동일하게 유지한다.

### 추론 consumer를 read-committed로 변경

`realtime/inference_service.py`에 다음 옵션을 추가했다.

```python
isolation_level="read_committed"
```

이제 추론 서비스는 Flink checkpoint와 함께 commit된 피처만 읽는다. 진행 중이거나 abort된
transaction의 피처는 읽지 않는다.

### 연속성 검증 도구 추가

`scripts/verify_realtime_feature_continuity.py`는 Kafka 피처 Topic을 `read_committed`로 읽고
다음을 검사한다.

- `feature_id` 누락
- 같은 `feature_id` 중복
- 1분 event time 공백
- 대상 심볼과 시작 시각 필터

검증 결과는 JSON으로 저장하고 하나라도 실패하면 종료 코드 1을 반환한다.

## 4. 자동 테스트

추가한 테스트:

1. 연속된 고유 피처는 정상으로 판정한다.
2. 같은 `feature_id` 두 건은 실패로 판정한다.
3. 1분이 빈 구간은 실패로 판정한다.
4. 지정 시작 시각 이전 피처는 검사에서 제외한다.

전체 실행:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

결과:

```text
Ran 19 tests
OK
```

## 5. Exactly-Once Job 실행

기존 Job을 취소하고 새 설정으로 제출했다.

```text
Job ID: bb08fbdd257a9e4e755ad862ece0a01b
상태: RUNNING
```

TaskManager 로그에서 다음을 확인했다.

```text
transactional.id = realtime-features-v1-0-3
transaction.timeout.ms = 300000
Instantiated a transactional producer
```

재시작 전 결과:

```text
completed checkpoints: 5
failed checkpoints: 0
committed feature -> inference: 1건
```

## 6. TaskManager 강제 재시작

다음 명령으로 JobManager, Kafka, WebSocket 수집기는 유지하고 계산 worker인 TaskManager만
재시작했다.

```powershell
docker compose restart taskmanager
```

재시작 중 TaskManager slot이 잠깐 0개가 되어 Job이 `RESTARTING` 상태로 전환됐다. 로그에서
다음을 확인했다.

```text
Restoring job bb08fbdd257a9e4e755ad862ece0a01b from Checkpoint 6
Recovering subtask 0 to checkpoint 6 for source Kafka raw market source
```

TaskManager가 다시 등록된 뒤 같은 Job ID가 `RUNNING`으로 복구됐고 checkpoint 7이 완료됐다.

```text
restored checkpoint: 6
post-restart completed checkpoint: 7
checkpoint 7 size: 6994 bytes
checkpoint 7 duration: 206 ms
```

재시작 순간 task가 실행 중이 아니어서 checkpoint trigger 1회가 실패했다. 이는 장애를
의도적으로 만든 시각에 발생한 예상된 실패다. 이후 checkpoint 7이 성공했으므로 복구 실패로
판정하지 않는다.

## 7. 중복·공백 최종 검증

실행 명령:

```powershell
python scripts\verify_realtime_feature_continuity.py `
  --start-time-ms 1787931150470 `
  --poll-seconds 20 `
  --output runtime_reports\realtime_checkpoint_restart_2026-08-29.json
```

결과:

| 항목 | 결과 |
| --- | ---: |
| committed 피처 | 5건 |
| 고유 `feature_id` | 5건 |
| 중복 `feature_id` | 0건 |
| 잘못된 `feature_id` | 0건 |
| 1분 공백 | 0건 |
| 최종 상태 | `healthy: true` |

검증한 event time 범위:

```text
1787931180000 ~ 1787931420000
```

이 범위는 TaskManager 재시작 전후를 포함한다.

## 8. 이번 결과의 의미와 한계

이번 검증으로 다음을 확인했다.

- Flink source offset과 집계 state가 checkpoint에서 복원된다.
- Kafka 피처 출력이 exactly-once transaction으로 기록된다.
- 추론 consumer는 committed 피처만 읽는다.
- 실제 한 번의 TaskManager 장애에서 피처 중복과 1분 공백이 발생하지 않았다.

다만 이것이 전체 자동매매 시스템의 end-to-end exactly-once를 의미하지는 않는다. 추론 서비스와
리스크 서비스는 Python consumer/producer이며, 출력 전송과 input offset commit이 하나의 Kafka
transaction으로 묶여 있지 않다. 해당 서비스가 정확히 전송 직후 죽으면 같은 deterministic
`signal_id`가 다시 만들어질 수 있다. 실거래 전에는 downstream idempotency 저장소 또는 Kafka
transactional consume-transform-produce 구조가 추가로 필요하다.

## 9. 다음 작업

데이터 경로의 다음 우선순위는 **오프라인 배치 피처와 실시간 피처 parity 자동 테스트**다.
동일한 고정 입력을 두 계산 경로에 넣고 OHLCV, `ma_5`, `return_1m`이 같은지 컬럼별 허용 오차로
검증한다. 이 작업이 끝나야 과거 학습 때 본 숫자와 실시간 추론 때 본 숫자가 같다고 증명할 수 있다.
