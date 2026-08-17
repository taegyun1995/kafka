# Apache Kafka Producer

Producer는 애플리케이션 프로세스 안에서 Event를 Kafka Record로 만들고, **선택한 Topic-Partition의 Leader Broker**에 전송하는 Kafka Client입니다.

전체 읽기 순서는 [Kafka 학습 가이드](KAFKA_STUDY_GUIDE.md)를 참고하세요.

```text
Application
→ KafkaProducer Client
→ Partition Leader Broker
→ Follower Replica 복제
→ acks 조건 충족 후 결과 반환
```

이 문서는 Producer의 **핵심 전송 경로**만 다룹니다. Protocol·세부 옵션·장애 분석은 [Producer 심화](KAFKA_PRODUCER_DEEP_DIVE.md), EOS·Outbox·Inbox·Saga는 [전송 보장과 분산 정합성](KAFKA_DELIVERY_GUARANTEES.md)으로 분리했습니다.

---

## 추천 학습 순서

| 단계 | 먼저 답할 질문 | 이 문서의 범위 |
|---|---|---|
| 1. 경계 | Producer는 어디에서 실행되고 누구에게 보내는가? | 전체 구조 |
| 2. Record | 무엇을 보내며 어떤 값이 저장되는가? | Topic · Key · Value · Header |
| 3. Routing | 어느 Partition과 Broker로 가는가? | Key Hash · Sticky · Leader |
| 4. 성공 | 언제 Kafka 전송 성공인가? | Future · `acks` · ISR |
| 5. 기본 설정 | 처음에 무엇만 정하면 되는가? | Serializer · Key · `acks` · Idempotence |
| 6. 운영 경계 | 종료·장애·분산 정합성은 어디까지인가? | Close · 심화 문서 링크 |

---

## 1. Producer는 Broker가 아니라 애플리케이션 안에서 동작한다

```text
애플리케이션 프로세스                         Kafka Cluster
┌───────────────────────────┐      ┌─────────────────────────────┐
│ Service                   │      │ Broker 1 · Broker 2 · Broker 3 │
│   ↓                       │      │   └─ Partition Leader/Follower │
│ KafkaTemplate (Spring 시) │      └─────────────────────────────┘
│   ↓                       │                    ▲
│ KafkaProducer Client      │ ── ProduceRequest ─┘
└───────────────────────────┘
```

- **KafkaProducer**: 직렬화, Partition 선택, Batch, Retry, Network 전송을 수행하는 실제 Client
- **KafkaTemplate**: Spring Kafka에서 제공하는 편의 `send()` API. 실제 전송은 내부 Producer에 위임
- **Broker**: Client 요청을 받고 Partition Replica Log를 저장하는 Kafka 서버
- **Kafka Cluster**: 여러 Broker가 같은 Metadata를 공유하며 함께 동작하는 운영 단위

Producer는 ZooKeeper에 Event를 보내지 않습니다. ZooKeeper Mode에서도 Record는 항상 Kafka Broker의 Partition Leader로 전송됐습니다.

### Spring에서는 보통 KafkaTemplate을 호출한다

```java
@Component
public class OrderEventProducer {
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderEventProducer(
        KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, OrderCreatedEvent>> publish(
        OrderCreatedEvent event
    ) {
        return kafkaTemplate.send("order-events", event.orderId(), event);
    }
}
```

직접 Apache Kafka Client를 사용한다면 애플리케이션이 `KafkaProducer`를 한 번 만들고 재사용합니다. 어느 방식이든 요청마다 Producer를 새로 만들지 않습니다.

---

## 2. ProducerRecord: 전송 입력과 Broker 저장 형식을 구분한다

Producer가 받는 입력은 `ProducerRecord`입니다.

```java
new ProducerRecord<>(
    "order-events", // Topic: 보낼 목적지
    "order-123",    // Key: Partition 선택 기준이 될 수 있는 업무 식별자
    event            // Value: Event Payload
);
```

| API 항목 | 역할 | 개별 Log Record에 남는가 |
|---|---|---|
| `topic` | 보낼 논리 목적지 | 아니오. TopicPartition의 저장 위치 문맥 |
| `partition` | 선택적으로 직접 지정하는 Partition 번호 | 아니오. 저장 위치 문맥 |
| `key` | Partition 선택 기준이 될 수 있는 식별자 | 예 |
| `value` | Event Payload | 예 |
| `headers` | `eventId`, `traceId`, `eventType` 같은 부가 정보 | 예 |
| `timestamp` | 시간 정보 | 예 |

### 같은 Event를 세 층에서 보면 다르게 보인다

| 층 | `OrderCreated(order-123)`가 보이는 모습 |
|---|---|
| Producer API | `topic=order-events`, `partition=미지정`, `key=order-123`, `value={...}` |
| Broker Log | `order-events / P2 / offset 42`에 Key·Value·Header bytes 저장 |
| Consumer API | `topic()`, `partition()`, `offset()`, `key()`, `value()`, `headers()` |

```text
Key가 Partition을 선택한다 (명시 Partition이 없을 때)
Broker가 Offset을 부여한다
Topic + Partition + Offset이 정확히 한 Kafka Record를 가리킨다
```

Topic과 Partition은 Header가 아닙니다. 하나의 Record Batch가 이미 하나의 TopicPartition에 속하므로, Batch 안의 각 Record에 Topic 문자열과 Partition 번호를 반복 저장하지 않습니다.

### 업무 Event 정보의 위치

| 정보 | 권장 위치 | 이유 |
|---|---|---|
| 주문별 순서 기준 `orderId` | Key | 같은 Key를 같은 Partition으로 보내기 쉽다 |
| 상품·금액·상태 | Value | Consumer가 처리할 업무 Payload |
| `eventId`, `traceId`, Event Type | Header 또는 Value Envelope | 중복 판별·추적·계약에 사용 |
| 어떤 Event 흐름인지 | Topic | Record의 목적지 |

`orderId`를 Key로 쓰는 것은 PK가 절대 규칙이라서가 아닙니다. **함께 순서를 보장할 업무 대상(Aggregate)이 무엇인가**가 기준입니다. 결제 상태는 `paymentId`, 사용자 상태는 `userId`가 Key가 될 수 있습니다.

### Key와 Value는 각각 직렬화된다

```text
Key 객체   → Key Serializer   → byte[]
Value 객체 → Value Serializer → byte[]
Header 값                       byte[]
```

`ProducerRecord` 전체를 하나의 덩어리로 직렬화하지 않습니다. Topic·Partition은 전송 위치 정보이고 Key·Value에는 각각 Serializer가 적용됩니다.

---

## 3. Partition은 Producer Client가 선택하고, Broker가 Offset을 준다

Producer Client는 Topic Metadata에서 Partition 목록과 각 Partition Leader의 위치를 알고 있습니다. 먼저 Partition 하나를 고른 다음, 그 Partition Leader Broker로 전송합니다.

```text
Topic: order-events (P0, P1, P2)
→ Partitioner가 P1 선택
→ P1 Leader가 Broker 2임을 Metadata에서 확인
→ KafkaProducer → Broker 2로 ProduceRequest 전송
```

### Partition 선택 우선순위

| ProducerRecord 입력 | 선택 결과 | 일반적인 사용처 |
|---|---|---|
| `partition=N` | 명시한 Partition N | 특별한 Routing 요구가 있을 때만 |
| `partition=null`, Key 있음 | Key Hash | 주문·결제처럼 대상별 순서가 중요할 때 |
| `partition=null`, Key 없음 | 기본 Sticky | 로그·Metric처럼 특정 대상 순서가 불필요할 때 |
| RoundRobinPartitioner 명시 | Record마다 다음 Partition | 특별한 균등 분배 요구가 있을 때 |

### Key와 Partition 번호를 함께 줄 수 있다

```java
new ProducerRecord<>("order-events", 3, "order-123", event);
```

이때 **명시 Partition 3이 우선**입니다. `order-123`은 Broker에 저장될 Key이지만, 기본 Key Hash Partitioner는 Partition 선택에 쓰이지 않습니다.

`partition=0`을 계속 지정하면 P0만 사용하므로 Hot Partition이 됩니다. 일반적인 도메인 Event는 Partition 번호를 하드코딩하지 않고 Key만 주는 편이 안전합니다.

### Sticky와 Round Robin을 언제 쓰는가

```text
Key 있음  → 기본 Key Hash
Key 없음  → 기본 Sticky
Round Robin → 기본값이 아닌 명시 설정
```

Sticky는 잠시 한 Partition의 Batch를 채운 뒤 대상을 바꿔 Batch·압축 효율을 높입니다. Round Robin은 Record마다 Partition을 바꾸므로, 같은 Key의 순서를 유지해야 하는 Event에는 맞지 않습니다.

특정 Key 하나가 압도적으로 많은 Event를 만들면, Key Hash를 써도 그 Key가 향하는 Partition은 Hot Partition이 됩니다. 이는 Sticky 옵션으로 해결할 문제가 아니라 **순서 보장 범위와 Key 단위**를 재설계할 문제입니다.

Partition 수를 늘리면 미래 Event의 Key Hash 결과가 바뀔 수 있습니다. “같은 Key는 영원히 같은 Partition”이라고 가정하면 안 됩니다.

---

## 4. 실제 Broker 전송: Metadata → Batch → Leader

`bootstrap.servers`는 Producer가 처음 Cluster에 들어가 Metadata를 얻는 Broker 주소 목록입니다. 최종 Record 목적지 IP가 아닙니다.

```text
Bootstrap Broker에 Metadata 요청
→ Topic의 Partition · Leader 정보 Cache
→ Partitioner가 대상 Partition 선택
→ 해당 Partition Leader Broker로 전송
```

`send()`는 일반적으로 비동기입니다.

```text
send(record)
→ Key·Value Serializer
→ Partitioner
→ Partition별 Record Accumulator Batch
→ Sender가 Leader Broker로 ProduceRequest 전송
→ Future / Callback 완료
```

같은 Topic·Partition의 Record는 Batch로 묶여 전송됩니다. Producer가 TCP 연결을 어떻게 재사용하고, Batch·Buffer가 가득 찼을 때 어떤 일이 일어나는지는 [Producer 심화](KAFKA_PRODUCER_DEEP_DIVE.md)를 봅니다.

---

## 5. `send()` 성공은 Consumer 업무 성공이 아니다

Spring Kafka는 `CompletableFuture<SendResult<K,V>>`를 반환합니다.

```java
kafkaTemplate.send(record).whenComplete((result, exception) -> {
    if (exception != null) {
        log.error("Kafka 전송 실패", exception);
        return;
    }

    RecordMetadata metadata = result.getRecordMetadata();
    log.info("topic={}, partition={}, offset={}",
        metadata.topic(), metadata.partition(), metadata.offset());
});
```

성공 결과에는 Topic, Partition, Offset, Timestamp 등이 담깁니다.

```text
Producer 전송 성공
≠ Payment·Inventory 같은 Consumer 업무 처리 성공
```

Record마다 `future.get()`으로 기다리면 비동기 전송과 Batch의 장점을 줄일 수 있습니다. 호출 흐름에서 반드시 확인해야 하는 실패만 Callback/Future로 처리합니다.

---

## 6. acks와 ISR: 언제 저장 성공으로 볼 것인가

`acks`는 Producer가 ProduceRequest 성공 응답을 기다리는 범위입니다. ISR은 Leader를 충분히 따라가고 있는 Replica 집합이며, Producer가 직접 관리하는 값은 아닙니다.

| 설정 | 성공 응답 기준 | 특징 |
|---|---|---|
| `acks=0` | Broker 응답을 기다리지 않음 | 빠르지만 실패 확인이 어려움 |
| `acks=1` | Leader Replica의 Local Log 저장 | 복제 전 Leader 장애 시 유실 가능 |
| `acks=all` | 현재 ISR의 모든 Replica가 acknowledge | ISR 수가 `min.insync.replicas`보다 작으면 쓰기 거부 |

```text
Replication Factor = 3
min.insync.replicas = 2
acks = all

ISR 3 → 3개 Replica의 acknowledge가 필요하고 쓰기 가능
ISR 2 → 2개 Replica의 acknowledge가 필요하고 쓰기 가능
ISR 1 → 최소 ISR 조건 위반으로 Broker가 쓰기 거부
```

`min.insync.replicas`는 Producer 옵션이 아니라 Topic/Broker 설정입니다. Replica·ISR·Leader 선출의 상세 구조는 [Kafka Broker와 Cluster](KAFKA_BROKER_AND_CLUSTER.md)를 봅니다.

---

## 7. 처음에는 이 설정만 결정한다

옵션 전체를 외우기보다 아래 질문에 먼저 답합니다.

| 질문 | 기본 결정 |
|---|---|
| 어디에 접속할까? | `bootstrap.servers`를 여러 Broker로 설정 |
| 어떤 형식으로 보낼까? | Key·Value Serializer와 Event Schema 결정 |
| 대상별 순서가 필요한가? | 업무 Aggregate ID를 Key로 사용 |
| 저장 성공 기준은? | 일반 업무 Event는 `acks=all` 기준 검토 |
| Retry 중 Log 중복을 막을까? | Idempotence 사용 |
| 처리량·압축이 필요한가? | `linger.ms`, `batch.size`, `compression.type`을 측정하며 조정 |

아래는 Kafka 4.3 기준으로 의도를 명시한 Producer 설정 예시입니다. 처리량·지연·압축률은 실제 부하에서 측정해 조정해야 합니다.

```properties
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
client.id=order-service
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=com.example.kafka.OrderEventSerializer

acks=all
enable.idempotence=true

linger.ms=5
compression.type=zstd
```

Kafka 4.3에서는 충돌하는 설정이 없을 때 `acks=all`, Idempotence, `linger.ms=5`가 기본값이기도 합니다. 설정을 명시하는 목적은 모든 환경에서 반드시 값을 바꾸기 위해서가 아니라, 애플리케이션의 의도를 드러내고 설정 충돌을 빠르게 발견하기 위해서입니다.

Idempotence는 **같은 Producer Session에서 자동 Retry된 Batch가 Kafka Log에 중복 저장되는 것**을 막습니다. 애플리케이션이 같은 Event를 두 번 `send()`한 경우나 Consumer의 업무 중복까지 막지는 않습니다. 그 경계는 [전송 보장과 분산 정합성](KAFKA_DELIVERY_GUARANTEES.md)에서 다룹니다.

---

## 8. 종료: Producer를 매번 만들고 닫지 않는다

```text
새 Event 수신 중단
→ 진행 중인 send 완료 대기
→ 필요한 경계에서 flush()
→ close()
```

`close()`는 대기 중인 전송을 마치려 시도한 뒤 I/O Thread와 Network Resource를 정리합니다. Spring 환경에서는 보통 Container가 Lifecycle 종료 시 이를 관리합니다.

강제 종료에서는 종료 코드가 실행되지 않을 수 있습니다. 종료 처리만으로 유실·중복 보장이 완성되지 않으므로 `acks`, Retry, Idempotence, Outbox 같은 각 경계를 함께 설계해야 합니다.

---

## 핵심 정리

```text
Application이 Topic · Key · Value · Headers를 구성
→ KafkaProducer가 직렬화하고 Partition을 선택
→ 그 Partition의 Leader Broker에 Batch 전송
→ Broker가 acks 조건에 맞춰 응답
→ Future / Callback으로 성공·실패 확인
```

| 혼동하기 쉬운 표현 | 정확한 의미 |
|---|---|
| Producer가 Cluster로 보낸다 | 실제로는 선택된 Partition Leader Broker로 전송 |
| Key가 Partition이다 | Key는 명시 Partition이 없을 때 Partition 선택 입력값 |
| `partition`은 Record Header다 | TopicPartition의 전송·저장 위치 문맥 |
| Offset을 Producer가 정한다 | Broker가 Partition Log에 저장하며 부여 |
| `send()` 성공 = 업무 성공 | Kafka `acks` 성공일 뿐 Consumer 업무 성공은 아님 |
| Idempotence = Exactly-once 업무 처리 | 자동 Retry Batch의 중복 저장 방지 범위 |

## 다음 문서

- [Producer 심화: Protocol · 옵션 · 장애](KAFKA_PRODUCER_DEEP_DIVE.md)
- [Kafka 전송 보장과 분산 정합성](KAFKA_DELIVERY_GUARANTEES.md)
- [Kafka Broker와 Cluster 구조](KAFKA_BROKER_AND_CLUSTER.md)

## 공식 문서

- [KafkaProducer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
- [ProducerRecord API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html)
- [Apache Kafka Producer Configs](https://kafka.apache.org/43/configuration/producer-configs/)
