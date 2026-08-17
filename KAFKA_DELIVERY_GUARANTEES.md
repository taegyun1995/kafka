# Kafka 전송 보장과 분산 정합성

이 문서는 Producer의 기본 전송 경로를 이해한 뒤 보는 심화 문서입니다. Producer 자체의 Record 구성·Partition 선택·`acks`는 [Kafka Producer](KAFKA_PRODUCER.md)를 먼저 봅니다.

전체 읽기 순서는 [Kafka 학습 가이드](KAFKA_STUDY_GUIDE.md)를 참고하세요.

여기서는 서로 다른 문제를 하나의 “한 번만 처리”라는 말로 섞지 않습니다.

```text
Kafka 저장 내구성             → acks · ISR
자동 Retry의 중복 저장 방지    → Idempotent Producer
Kafka 안의 여러 쓰기 원자성    → Kafka Transaction · EOS
DB 변경과 발행 의도 누락 방지  → Transactional Outbox
Consumer 업무 중복 처리 방지   → Inbox / eventId 멱등성
여러 서비스 업무 정합성        → Saga · 보상 Transaction
```

---

## 1. Delivery Semantics는 Producer 옵션 하나가 아니다

At-most-once, At-least-once, Exactly-once는 Producer의 재시도와 Consumer의 Offset Commit 순서를 함께 본 **처리 흐름 전체의 결과**입니다.

| 보장 | 실패를 다루는 방식 | 결과 | 대표 사용처 |
|---|---|---|---|
| At-most-once | 전송 실패를 재시도하지 않거나 처리 전에 Offset Commit | 중복은 적지만 유실 가능 | 일부 Telemetry |
| At-least-once | Retry하고 처리 성공 뒤 Offset Commit | 유실 가능성은 낮지만 재처리 가능 | 가장 흔한 기본 전략 |
| Exactly-once (Kafka 내부 EOS) | Output Record와 다음 Offset을 Kafka Transaction으로 함께 Commit | Kafka Input → Output → Offset을 한 결과로 확정 | Kafka-to-Kafka 변환 |

```text
At-least-once
= 같은 Input이 다시 올 수 있음을 받아들이고
  Consumer의 DB 처리·외부 호출을 멱등하게 만드는 전략
```

## 2. acks와 Idempotence는 해결하는 문제가 다르다

| 구성 | 해결하는 문제 | 해결하지 않는 문제 |
|---|---|---|
| `acks=all` | 현재 ISR의 모든 Replica acknowledge | Retry 중복, 업무 중복 |
| Idempotent Producer | 같은 Producer Session의 자동 Retry Batch 중복 저장 | 애플리케이션이 새로 두 번 발행한 Event |
| Kafka Transaction | 여러 Kafka Record와 Consumer Offset의 Commit·Abort 원자성 | 외부 DB·HTTP 호출의 자동 원자성 |

### Idempotence: 자동 Retry가 Record를 두 번 남기지 않는다

Producer는 PID·Epoch와 Partition별 Sequence를 붙여 Batch를 전송합니다.

```text
PID 77 · Sequence 12 전송
→ Broker Log 저장
→ ProduceResponse 유실
→ 같은 PID 77 · Sequence 12로 자동 Retry
→ Broker가 중복 Batch로 판정
→ Log에는 한 번만 남김
```

하지만 애플리케이션이 같은 Event에 대해 새 `send()`를 두 번 호출하면 각각 다른 Sequence가 붙습니다. 둘 다 별도 Record로 저장될 수 있습니다.

```text
Idempotence
≠ eventId 기준의 업무 중복 제거
```

`min.insync.replicas`는 `acks=all`에서 필요한 acknowledge 개수를 직접 지정하는 설정이 아닙니다. 쓰기를 허용하기 위한 ISR의 최소 수를 정하며, 실제 `acks=all` 응답은 해당 시점의 ISR 전체가 acknowledge한 뒤 반환됩니다.

멱등성을 반드시 유지하려면 호환되는 `acks`, Retry, In-flight 설정을 사용해야 합니다. 최신 Client의 기본 조합을 임의로 약화하는 경우에는 `enable.idempotence=true`를 명시해 충돌을 설정 오류로 드러내는 편이 안전합니다.

## 3. Kafka Transaction은 Kafka 내부만 묶는다

`transactional.id`를 가진 Producer는 `beginTransaction()`과 `commitTransaction()` 사이에 여러 Topic·Partition의 Record를 담을 수 있습니다. Commit Marker가 기록되면 `read_committed` Consumer에 함께 보입니다.

```java
producer.initTransactions();

try {
    producer.beginTransaction();
    producer.send(new ProducerRecord<>("order-events", orderId, orderEvent));
    producer.send(new ProducerRecord<>("audit-events", orderId, auditEvent));
    producer.commitTransaction();
} catch (KafkaException e) {
    producer.abortTransaction();
    throw e;
}
```

| 항목 | 의미 |
|---|---|
| `transactional.id` | 재시작에도 이어지는 논리 Producer ID. 동시에 실행되는 서로 다른 Instance에는 서로 다른 값 필요 |
| `transaction.timeout.ms` | 열린 Transaction의 최대 시간 |
| `isolation.level=read_committed` | Commit된 Transaction만 Consumer에게 반환 |
| `sendOffsetsToTransaction()` | Output Record와 Input Consumer의 다음 Offset을 함께 확정 |

### Kafka-to-Kafka EOS

Consume → Transform → Produce에서는 출력만 Transaction으로 보내면 부족합니다. 입력을 어디까지 처리했는지인 **다음 Offset**도 같은 Transaction에 넣어야 합니다.

```text
Input Consumer Record 읽기
→ beginTransaction()
→ Output Topic에 전송
→ sendOffsetsToTransaction(nextOffsets, groupMetadata)
→ commitTransaction()

Commit  → Output과 Offset이 함께 확정
Abort   → Output과 Offset 모두 확정되지 않음
```

이 보장은 Kafka에서 읽은 Input, Kafka에 쓴 Output, Kafka에 Commit한 Offset의 경계입니다. DB Update·HTTP 호출·이메일 발송을 자동으로 되돌리지 않습니다.

### Transactional Fencing

새 Instance가 동일한 `transactional.id`로 초기화하면 새로운 Epoch를 얻고, 이전 Instance는 `ProducerFencedException`을 받아 더 이상 Transactional Producer로 사용하면 안 됩니다.

```text
Instance A: transactional.id = order-1
Instance B: 같은 transactional.id로 initTransactions()
→ B가 새 Epoch 확보
→ A는 Fenced, 종료
```

이는 같은 논리 Producer의 중복 실행을 막는 장치입니다.

---

## 4. Transactional Outbox: DB 변경과 발행 의도를 함께 확정한다

주문 DB 저장과 Kafka 발행은 서로 다른 시스템입니다. DB Commit 뒤 Kafka 발행 전에 장애가 나면 주문은 생겼지만 Event는 빠질 수 있습니다.

Outbox는 Kafka를 DB Transaction에 직접 참여시키지 않습니다. 대신 업무 데이터와 “나중에 발행해야 할 Event”를 같은 **로컬 DB Transaction**으로 저장합니다.

```text
1. DB Transaction
   orders 저장
   + outbox_events(eventId, aggregateType, aggregateId, eventType, payload) 저장
   → 둘 다 Commit 또는 둘 다 Rollback

2. Relay / CDC
   Commit된 Outbox row를 Kafka에 발행
   aggregateType → Topic
   aggregateId (예: orderId) → Kafka Key
```

Outbox는 Producer 옵션이 아닙니다. Relay 또는 CDC Connector가 실제 Kafka Producer 역할을 합니다.

### Outbox와 Kafka Transaction은 다르다

```text
Outbox Transaction
= DB의 Order 변경 + Outbox row를 원자적으로 확정

Kafka Transaction
= Kafka Output Record + Consumer Offset을 원자적으로 확정
```

Outbox Relay가 Kafka 발행에는 성공했지만 `published_at` 표시 전에 죽으면 같은 Outbox Event가 다시 발행될 수 있습니다. 새 `send()`이므로 Producer Idempotence 범위 밖입니다. Event의 `eventId`를 Header 또는 Value에 포함하고 Consumer가 중복 업무 처리를 막아야 합니다.

## 5. Inbox: Consumer 쪽의 업무 중복 방지

Inbox는 Consumer가 받은 `eventId`와 실제 업무 변경을 같은 DB Transaction으로 확정하는 패턴입니다.

```text
Kafka Record 수신
→ DB Transaction
   inbox(eventId) Insert
   + payment 상태 변경
→ Commit

같은 eventId 재수신
→ Inbox의 Unique 제약으로 이미 처리됨을 판단
→ 업무 변경을 다시 하지 않음
```

전용 Inbox 테이블 대신 대상 테이블의 Unique 제약이나 멱등한 Upsert로 같은 효과를 만들 수도 있습니다. 핵심은 **Consumer Offset을 Commit하기 전에 업무 처리 중복을 판별할 영속 기준**이 있다는 점입니다.

## 6. Saga: 여러 서비스의 Commit을 Rollback하지 않는다

Saga는 Order, Payment, Inventory처럼 여러 서비스의 DB Transaction을 하나의 ACID Transaction으로 묶지 않습니다. 각 서비스는 자신의 로컬 Transaction을 Commit하고, 실패하면 과거 Commit을 DB Rollback하는 대신 새 Transaction으로 보상합니다.

```text
Order: 주문 생성 + Outbox Event Commit
→ Payment: 결제 승인 + Outbox Event Commit
→ Inventory: 재고 예약

재고 부족
→ Payment: 결제 취소 Transaction
→ Order: 주문 취소 Transaction
```

Saga는 Kafka Transaction이 아닙니다. Event가 At-least-once로 재전달될 수 있으므로, 각 단계와 보상 동작 모두 `eventId` 기준으로 반복 실행에 안전해야 합니다.

---

## 핵심 경계 요약

```text
Producer Idempotence
→ Kafka Protocol Retry 중복 방지

Kafka Transaction
→ Kafka Record + Consumer Offset 원자성

Outbox
→ DB 변경 + 발행 의도 누락 방지

Inbox
→ Consumer 업무 중복 처리 방지

Saga
→ 여러 서비스의 장기 업무를 보상으로 조율
```

## 공식 문서

- [KafkaProducer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [KafkaConsumer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Apache Kafka Producer Configs](https://kafka.apache.org/43/configuration/producer-configs/)
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html)
- [Saga Pattern](https://microservices.io/patterns/data/saga.html)
