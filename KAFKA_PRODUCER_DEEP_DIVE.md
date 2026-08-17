# Kafka Producer 심화: Protocol · 옵션 · 장애

이 문서는 [Kafka Producer](KAFKA_PRODUCER.md)의 핵심 경로를 읽은 뒤, 성능·장애·통신 원리를 더 파고들 때 보는 참고 문서입니다.

전체 읽기 순서는 [Kafka 학습 가이드](KAFKA_STUDY_GUIDE.md)를 참고하세요.

```text
먼저 볼 것
→ Record · Key · Partition · Leader Broker · acks

그다음 볼 것
→ TCP Protocol · 모든 옵션 · Retry/Timeout · Buffer · In-flight
```

---

## 1. Framework와 Kafka Client의 경계

Spring Boot에서는 보통 애플리케이션이 `KafkaTemplate`을 호출합니다. 실제 직렬화, Partition 선택, Batch, Network 전송은 Apache Kafka Client의 `KafkaProducer`가 수행합니다.

```text
Spring
Service → KafkaTemplate → KafkaProducer → Broker

Kafka Client 직접 사용
Service ─────────────────→ KafkaProducer → Broker
```

`ProducerFactory`는 KafkaProducer의 생성·공급·재사용을 관리하는 Spring 구성요소이며, Record가 Broker로 가는 전송선 그 자체는 아닙니다.

### 재사용 원칙

Producer를 요청마다 만들고 닫지 않습니다. Producer 내부에는 Metadata Cache, TCP 연결, Buffer, Sender I/O Thread가 있으므로 애플리케이션 수명 동안 재사용합니다.

---

## 2. KafkaProducer는 TCP 위에서 Kafka Protocol을 말한다

```text
KafkaProducer
→ Kafka Binary Protocol
→ TLS / SASL (선택)
→ TCP Byte Stream
→ Broker Socket Server
```

| 계층 | 책임 |
|---|---|
| TCP | 연결, 순서 보장, 흐름 제어, Byte Stream |
| Kafka Protocol | Request/Response 경계, API Version, Correlation ID, Topic·Partition·Record Batch |
| KafkaProducer | 직렬화, Partition, Batch, Retry, 실제 Network I/O |
| KafkaTemplate | Spring 편의 API |

TCP가 연결되었다고 Kafka 저장이 성공한 것은 아닙니다. Broker가 ProduceRequest를 처리하고 `acks` 조건에 맞는 ProduceResponse를 반환해야 `send()`의 결과가 성공합니다.

Producer는 Record마다 연결을 새로 만들지 않습니다.

```text
Bootstrap Broker에 연결
→ TLS/SASL 인증 (필요 시)
→ Metadata 조회
→ 대상 Partition Leader Broker와 연결
→ 같은 연결로 ProduceRequest 반복
→ Leader 변경·연결 오류 시 Metadata 갱신 및 재연결
```

## 3. 옵션은 이름이 아니라 상황으로 고른다

### 최소 안전 시작점

```properties
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
client.id=order-service
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=com.example.kafka.OrderEventSerializer

acks=all
enable.idempotence=true
delivery.timeout.ms=120000

linger.ms=5
compression.type=zstd
```

| 상황 | 먼저 보는 항목 |
|---|---|
| 연결할 Cluster를 찾지 못함 | `bootstrap.servers`, Security, Metadata |
| Event 형식이 맞지 않음 | Key/Value Serializer, Schema |
| 처리량·압축을 높이고 싶음 | `batch.size`, `linger.ms`, `compression.type` |
| 지연·Timeout·Retry를 분석 | `delivery.timeout.ms`, `request.timeout.ms`, Retry Backoff |
| Buffer가 찬다 | `buffer.memory`, `max.block.ms`, Broker 병목 |
| 기본 Routing이 맞지 않음 | Key 설계, `partitioner.class` |

### 자주 혼동하는 옵션 관계

```text
delivery.timeout.ms
= Batch 대기 + Request 응답 대기 + Retry를 합친 전체 기한

request.timeout.ms
= 개별 Kafka Request의 응답 대기 기한

batch.size + linger.ms + 유입량
= 실제 Batch 크기와 압축률·지연을 함께 결정
```

`buffer.memory`만 키워서 지속적인 Broker 지연을 해결할 수는 없습니다. 유입량이 전송량보다 계속 높으면 Buffer는 결국 다시 찹니다.

## 4. Retry · Timeout · In-flight

### Retry의 최종 경계

```text
send()
→ 일시적 오류 또는 응답 Timeout
→ Retry · 필요 시 Metadata 갱신
→ delivery.timeout.ms 안에 성공하면 Future 성공
→ 기한 초과 또는 복구 불가 오류면 Future 실패
```

Future가 최종 실패한 후 애플리케이션이 같은 Event를 새로 발행할지는 Kafka Client가 결정할 수 없습니다. 이는 업무 Event의 `eventId`, Outbox, Consumer 멱등성 정책으로 판단합니다. 자세한 보장 경계는 [Kafka 전송 보장과 분산 정합성](KAFKA_DELIVERY_GUARANTEES.md)을 봅니다.

### Partition 순서와 In-flight

Kafka의 순서는 Topic 전체가 아니라 Partition 단위입니다.

| 조합 | 결과 |
|---|---|
| Retry + Idempotence OFF + In-flight 요청 여러 개 | 앞 Batch의 재시도 중 뒤 Batch가 먼저 저장되어 순서가 바뀔 수 있음 |
| Idempotence ON + 호환되는 In-flight 설정 | Retry 중복과 순서 역전을 방지하도록 Producer Protocol이 동작 |

Partition 수를 늘리면 이후 Key Hash Mapping이 바뀔 수 있으므로, 특정 업무 Key의 장기 순서는 Topic 증설 정책까지 포함해 설계합니다.

## 5. Buffer와 Backpressure

`send()`는 보통 Record를 Producer의 Buffer에 넣고 Future를 빠르게 반환합니다. 다만 Metadata가 없거나 Buffer가 가득 찼다면 `max.block.ms`까지 호출 Thread가 대기할 수 있습니다.

```text
Serializer
→ Partition별 Accumulator Batch에 적재
→ Sender가 batch.size 또는 linger.ms 기준으로 전송
→ 유입량 > 전송량이 계속됨
→ buffer.memory 소진
→ max.block.ms 이후 Timeout 가능
```

Backpressure를 만났을 때는 아래 순서로 봅니다.

1. Broker·Network·Quota 때문에 전송량이 떨어졌는가?
2. 특정 Key나 명시 Partition 때문에 한 Partition만 뜨거운가?
3. Producer 유입량 자체가 지속적으로 더 큰가?
4. 그 뒤에만 `batch.size`, `linger.ms`, `buffer.memory` 조정을 검토한다.

## 6. 종료

```text
새 Event 수신 중단
→ 진행 중인 send 완료 대기
→ 필요한 경계에서 flush()
→ close()
```

`close()`는 대기 중인 전송을 처리하려 시도한 뒤 I/O Thread와 Network Resource를 정리합니다. 강제 종료에서는 이 코드가 실행되지 않을 수 있으므로, 종료 처리만으로 유실·중복 보장을 완성할 수는 없습니다.

## 공식 문서

- [Apache Kafka Producer Configs](https://kafka.apache.org/43/configuration/producer-configs/)
- [Apache Kafka Protocol Guide](https://kafka.apache.org/43/design/protocol/)
- [KafkaProducer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
