# PopupPass backend

첫 번째 구현은 Kafka나 별도 Worker 없이, 하나의 Spring Boot 서버에서 예약 업무를 끝까지 처리하는 단일 서버 MVP다.

```text
HTTP 요청
  → ReservationService
  → PostgreSQL
```

예약 규칙과 데이터 변경은 `ReservationService`에 모으고, HTTP Controller는 요청·응답 변환만 담당한다. 지금은 **가장 단순한 방법**으로 처리하되, 같은 업무 요청의 재시도는 `Idempotency-Key`로 한 번만 반영한다. 정원 차감과 1인 1매를 위한 동시성 장치는 아직 없다(**4. 지금 없는 것**). 나중에 처리 시간이 길어지거나 버퍼링·독립 확장이 필요해지면 이 업무 경계를 별도 Worker와 메시지 브로커로 옮긴다.

## 1. PostgreSQL 실행

프로젝트 루트에서:

```bash
docker compose up -d postgres
```

PostgreSQL은 호스트 `localhost:15432`로 노출된다.

## 2. Spring Boot 실행

Gradle이 설치되어 있다면 `backend` 디렉터리에서 실행한다.

```bash
gradle bootRun
```

또는 애플리케이션 컨테이너로 실행한다.

```bash
docker compose --profile app up --build
```

애플리케이션은 `http://localhost:18090`에서 동작한다.

## 3. 실제 흐름 확인

예약 회차를 만든다.

```bash
curl -X POST http://localhost:18090/reservation-rounds \
  -H 'Content-Type: application/json' \
  -d '{"id":"round-1","name":"PopupPass 1회차","capacity":2}'
```

회차를 조회하면 남은 자리가 보인다.

```bash
curl http://localhost:18090/reservation-rounds/round-1
```

```json
{"id":"round-1","name":"PopupPass 1회차","capacity":2,"remainingCount":2,"status":"OPEN"}
```

예약을 요청하면 같은 HTTP 요청 안에서 업무 처리가 끝나고 결과가 즉시 반환된다.

```bash
curl -X POST http://localhost:18090/reservation-rounds/round-1/reservations \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: round-1-user-1-attempt-1' \
  -d '{"userId":"user-1"}'
```

응답 예시는 다음과 같다.

```json
{
  "requestId": "0198...",
  "roundId": "round-1",
  "userId": "user-1",
  "status": "CONFIRMED",
  "createdAt": "2026-08-21T12:00:00Z"
}
```

응답의 `requestId`로 결과를 다시 조회할 수도 있다.

```bash
curl http://localhost:18090/reservations/{requestId}
```

예약 요청의 `Idempotency-Key`는 클라이언트가 한 번의 업무 시도에 붙이는 식별자다. 같은 키로 같은 회차·사용자를 다시 보내면 새로 판단하지 않고 처음 저장한 `requestId`, 상태, 시각을 그대로 돌려준다. 같은 키를 다른 회차나 사용자에 재사용하면 `409 Conflict`다. 키를 바꾸면 새로운 시도이므로, 매진으로 거절된 사용자는 자리가 생긴 뒤 새 키로 다시 예약할 수 있다.

이미 **확정된** 예약이 있는 `userId`가 새로운 키로 다시 보내면 `REJECTED_DUPLICATE`, 정원을 초과하면 `REJECTED_SOLD_OUT`이 저장된다. 중복 판정은 확정된 예약이 있는지 **애플리케이션이 조회 한 줄로** 확인한다. DB에는 아직 이를 막는 제약이 없다.

회차를 마감하면 더 이상 예약을 받지 않는다.

```bash
curl -X POST http://localhost:18090/reservation-rounds/round-1/close
```

마감은 **설정 연산**이라 여러 번 호출해도 결과가 같다. 마감 뒤의 요청은 자리가 남아 있어도 거절되며, 이때는 매진과 구분해 `REJECTED_ROUND_CLOSED`로 남는다.

| 예약 결과 | 언제 |
| --- | --- |
| `CONFIRMED` | 회차가 열려 있고 자리가 남음 |
| `REJECTED_DUPLICATE` | 같은 사용자의 확정 예약이 이미 있음 |
| `REJECTED_SOLD_OUT` | 회차는 열려 있으나 자리가 없음 |
| `REJECTED_ROUND_CLOSED` | 회차가 마감됨 |
| `REJECTED_ROUND_NOT_FOUND` | 회차가 없음 |

> 상태를 추가할 때는 `schema.sql`의 `reservations_status_check` 목록도 함께 고친다. Hibernate는 테이블을 처음 만들 때만 이 제약을 만들고 `ddl-auto: update`는 기존 제약을 고치지 않으므로, 목록이 어긋나면 이미 만들어진 DB에서만 저장이 막힌다.

## 4. 지금 없는 것

이 구현에는 정원 차감과 1인 1매를 지키는 **동시성 장치가 아직 없다.** 다만 같은 `Idempotency-Key`를 가진 재시도는 이미 처리한 결과를 돌려준다. 서로 다른 키의 동시 요청은 여전히 한 번에 하나씩 온다고 가정한다.

```java
// ReservationService.reserve() — 키를 확인한 뒤 읽고, 판단하고, 쓴다
var round = roundRepository.findById(roundId).orElse(null);
...
round.takeOne();
roundRepository.save(round);
```

읽은 시점과 쓰는 시점 사이가 비어 있으므로, 요청이 겹치면 이런 일이 일어난다.

```bash
# 정원 2인 회차에 서로 다른 다섯 사람이 동시에
for u in 1 2 3 4 5; do
  curl -s -X POST http://localhost:18090/reservation-rounds/round-1/reservations \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: round-1-user-$u-attempt-1" \
    -d "{\"userId\":\"user-$u\"}" &
done; wait
```

```text
다섯 건 모두 CONFIRMED · remainingCount = 1
```

정원 2를 3건 넘겨 팔았고 장부와 실제도 어긋난다. 서로 다른 키로 재시도하면 여전히 새 요청이지만, 같은 키로 재시도하면 저장된 결과를 다시 받는다.

무엇을 더하면 무엇이 막히는지는 **6. 보호 장치를 하나씩 켜 보기**에서 단계별로 확인한다. 필요를 겪은 뒤에 이 구현으로 하나씩 되가져온다.

## 5. 테스트

Testcontainers가 PostgreSQL 컨테이너를 직접 띄우므로 Docker만 실행 중이면 된다.

```bash
cd backend
gradle test
```

선착순에서 실제로 밟는 경로를 검증한다.

요청이 한 번에 하나씩 올 때의 업무 규칙과 같은 키의 재시도를 확인한다. 서로 다른 요청의 동시성은 여기서 다루지 않고 `com.popup.pass.study`의 단계별 테스트에서 본다.

- 정원이 남으면 확정, 넘으면 매진 거절
- 확정된 사용자가 다시 요청하면 중복 거절이고 정원은 깎지 않음
- 매진으로 거절된 사용자가 자리가 생기면 다시 예약 가능
- 없는 회차에 요청하면 회차 없음으로 거절
- 회차 조회로 남은 자리가 보임
- 마감한 회차는 자리가 남아 있어도 예약을 받지 않음
- 마감을 여러 번 호출해도 결과가 같음
- 결과를 `requestId`로 다시 조회 가능
- 같은 `Idempotency-Key`로 재시도해도 같은 결과와 한 건의 기록만 남음
- 같은 키를 다른 요청 내용에 사용하면 충돌하고 자리와 기록이 변하지 않음

## 6. 보호 장치를 하나씩 켜 보기

지금 코드에는 선착순을 지키는 장치가 세 겹 들어 있다. 다 켜 놓고 보면 **무엇 덕분에 막혔는지** 알 수 없으므로, 아무 보호도 없는 0단계부터 하나씩 더하며 그때마다 무엇이 깨지는지 본다.

| 단계 | 더한 것 | 막히는 것 | 여전히 남는 것 |
| --- | --- | --- | --- |
| 0 | 없음 | — | 초과 판매 · 이중 예약 · 재시도 중복 |
| 1 | 원자적 차감 (`update ... where remaining_count > 0`) | 초과 판매 | 이중 예약 · 재시도 중복 |
| 2 | 확정 예약 부분 유니크 인덱스 | 이중 예약 | 재시도가 원래 결과를 잃음 |
| 3 | 멱등성 키 (`Idempotency-Key` → `idempotency_key` UNIQUE) | 재시도 중복 | — |

단계마다 파일이 하나씩이다. 무엇이 어디서 깨지는지 보이도록 JPA 없이 SQL을 그대로 쓰고, 전용 테이블(`study_*`)만 건드리므로 실제 예약 테이블과 섞이지 않는다.

```text
src/main/java/com/popup/pass/study/
├── steps/StudyStep.java                 단계가 공유하는 계약
├── steps/Step0NoGuard.java              읽고 → 판단하고 → 쓴다
├── steps/Step1AtomicDecrement.java      + 조건부 UPDATE 한 문장
├── steps/Step2UniqueIndex.java          + 부분 유니크 인덱스
├── steps/Step3IdempotencyKey.java       + 멱등성 키 (학습용은 request_id PK로 단순화)
├── steps/StepWithoutDuplicateRule.java  곁가지 · 1인 2매를 허용하는 세계
└── StudyRunner.java                     한 단계에 한 상황을 들이대고 판정까지 만든다
```

단계 구현에는 Spring 애너테이션을 붙이지 않았다. 표본으로 읽어야 하는 코드라 보호 장치 외의 것이 눈에 들어오지 않게 두었고, 조립은 `StudyConfig`에서만 한다.

### 브라우저에서 눌러 보기

학습용 창구는 `study` 프로필에서만 열린다. 평소 실행에는 없는 코드다.

```bash
gradle bootRun --args='--spring.profiles.active=study'
```

그 다음 두 화면을 연다 — `site/popup-pass-demo.html`은 **현재 동기 구현**을 그대로 부르고, `site/popup-pass-guards.html`은 보호 장치를 켠 학습용 단계들을 부른다. 단계 × 상황 매트릭스가 나오고, 칸을 누르면 **실제 서버에서 진짜로 동시 요청이 일어난다.** 경합이 실제로 벌어지므로 누를 때마다 숫자가 조금씩 다를 수 있다.

```bash
# 0단계에 "정원 2에 5명이 동시에"를 들이댄다
curl -X POST http://localhost:18090/study/steps/step0/scenarios/oversell
```

```json
{
  "verdict": "BROKEN",
  "headline": "정원 2인데 5건이 확정됐다 — 3건을 넘겨 팔았다",
  "confirmed": 5, "remainingCount": 1, "savedRows": 5,
  "notes": ["다섯이 같은 값을 읽고 같은 값을 덮어썼다. ...", "장부는 1자리 남았다는데 확정은 5건이다. ..."]
}
```

판정(`OK` · `PARTIAL` · `BROKEN`)과 설명은 미리 적어 둔 것이 아니라 **방금 나온 숫자에서 만든다.** 창구 목록은 `GET /study`에 있다.

### 상황

| 상황 | 무엇을 한다 |
| --- | --- |
| `oversell` | 정원 2에 서로 다른 다섯 사람이 동시에 |
| `double-booking` | 같은 사람의 요청 두 건이 동시에 |
| `retry` | 확정 뒤 응답이 유실되어 같은 키로 다시 보낸다 |
| `concurrent-retry` | 재시도가 원래 요청과 겹친다 |

### 테스트로 고정해 두기

```bash
gradle test --tests '*StudyStepsTest*'
```

5단계 × 4상황 = 20칸의 판정을 그대로 단언한다. `BROKEN` 칸이 있는 것은 실패가 아니라 **그 단계가 아직 막지 못하는 것**을 고정해 둔 것이다. 화면이 보여 줄 결과와 같은 값이다.

### 요점은 2단계다

2단계가 안전해 보인 것은 예약이라는 업무의 성질 때문이 아니라 **1인 1매 규칙이 멱등성을 대신 제공하고 있었기 때문**이다. 곁가지 단계(`no-duplicate-rule`)로 그 규칙을 걷어내면 재시도가 그대로 두 번째 자리를 먹는다. Kafka를 붙이면 at-least-once 재전달이 더해지므로 이 자리가 다시 문제가 된다.

## 7. 부하를 걸어 보기

6절까지가 물은 것은 **"답이 맞는가"** 였다. 초과 예약과 중복 예약은 DB 제약 한 줄로 고쳐진다.
이 절이 묻는 것은 다른 질문이다 — **"제때 대답하는가"**. 아무리 정확해도 커넥션 하나는 한 번에 한 건씩만 처리한다.

브라우저는 한 출처에 커넥션을 여섯 개까지만 열기 때문에 동시 300건은 화면에서 만들 수 없다.
서버 안에서 스레드를 세워 `ReservationService.reserve()`를 직접 동시에 부른다.
정원은 요청 수의 두 배로 잡고 사용자도 전원 다르게 두어 매진·중복 거절이 섞이지 않게 했다. 남는 변수는 시간뿐이다.
워밍업은 정원이 따로인 회차에서 해서 본 회차의 자리를 먹지 않는다.

```bash
# 동시 300건, 1초 안에 답을 못 받으면 "떠났을 요청"으로 센다
curl -s -X POST http://localhost:18090/load/runs \
  -H 'Content-Type: application/json' \
  -d '{"concurrency":300,"clientPatienceMillis":1000}'

# 이 서버의 커넥션 풀 크기와 프리셋
curl -s http://localhost:18090/load
```

브라우저에서는 `site/popup-pass-load.html`을 열고 **프리셋 전부 훑기**를 누른다.
동시 요청 수별로 줄이 쌓여야 천장이 보인다.

### 관측한 것

커넥션 풀 10개(Hikari 기본값), 로컬 PostgreSQL 기준이다.
첫 요청은 클래스 로딩과 JIT 때문에 유독 느려서 본 측정 전에 10건을 미리 보내 데운다(결과의 `warmupRequests`).
그래도 JIT는 계속 데워지므로 **절대값은 실행마다 흔들린다.** 봐야 할 것은 값이 아니라 두 열이 벌어지는 모양이다.

| 동시 요청 | 처리량 | p50 | p95 | 풀 앞 대기 최대 | 판정 |
| --- | --- | --- | --- | --- | --- |
| 1 | 250건/s | 4.1ms | 4.1ms | 0 | CALM |
| 10 | 1,111건/s | 7.5ms | 9.5ms | 0 | CALM |
| 30 | 1,667건/s | 11.4ms | 17.6ms | 20 | QUEUEING |
| 100 | 1,887건/s | 25.1ms | 47.9ms | 88 | QUEUEING |
| 300 | 2,273건/s | 76.6ms | 125.6ms | 289 | QUEUEING |
| 600 | 2,449건/s | 115.4ms | 223.9ms | 582 | QUEUEING |

동시 요청을 10건에서 600건으로 **60배** 올리는 동안 처리량은 **2.2배** 늘어나는 데 그쳤고,
p95 지연은 9.5ms에서 223.9ms로 **24배**가 됐다. 늘어난 요청은 처리량이 아니라 대기 줄로 갔다.

풀이 10개이므로 동시 600건 중 590건은 언제나 남의 트랜잭션이 끝나기를 기다린다.
`reserve()`는 한 건의 업무 처리를 트랜잭션 안에서 끝내므로 커넥션을 그동안 붙잡고, 요청 하나의 커넥션 점유 시간은 약 4ms다.
600건 ÷ 10커넥션 × 4ms ≈ 245ms — 실제로 잰 전체 소요와 맞는다.
**커넥션 점유 시간은 부하가 커져도 변하지 않는다.** 변하는 것은 자기 차례를 기다리는 시간뿐이다.

기다려 줄 시간을 100ms로 낮추면 판정이 바뀐다.

```
동시 300건 · 인내심 100ms → FAILING
  기다리다 떠났을 요청 38건 / 300건
  서버는 300건을 전부 처리했다. 실패는 한 건도 없다.
```

**서버는 다 처리했다. 문제는 그 사이 사람이 기다리고 있었다는 것이다.**
동기 처리에서 먼저 무너지는 것은 정확성이 아니라 응답이다.

### 지연만 깨지는 게 아니다

정원을 요청 수의 두 배로 잡은 것은 **매진 거절이 섞이지 않게** 하려는 것이지, 정합성이 지켜진다는 뜻이 아니다.
코드는 순정 그대로라 자리 차감은 여기서도 유실된다.

```
동시 300건 → 확정 300건, 정원에서 빠진 자리 33칸 (차감 267칸 증발)
```

정원이 넉넉해 매진으로 드러나지 않았을 뿐, 6절의 0단계에서 본 Lost Update가 그대로 일어나고 있다.
실행마다 몇 칸이 증발했는지는 결과의 `ledger`에 남는다.

이 어긋남은 **조건부 UPDATE 한 줄로 고쳐진다.** 위의 지연은 그 뒤에도 남는다 — 그래서 처방이 다르다.
다음 절에서 실제로 다 켜고 재 본다.

### 다 고치면 빨라지는가

앞의 어긋남은 6절의 보호 장치로 막힌다. 그러면 **다 켜면 응답도 빨라지는가.**
같은 부하를 0단계(보호 없음)와 3단계(다 켬)에 한 번씩 건다.
두 실행은 같은 DB·같은 커넥션 풀·같은 재는 방법을 쓰고, 다른 것은 보호 장치뿐이다.
경합을 눈에 보이게 벌리는 학습용 연출(`widenRaceWindow`)은 **양쪽 모두 꺼 둔다** —
켠 채로 재면 재는 것이 코드가 아니라 그 `sleep`이 된다.

```bash
curl -s -X POST http://localhost:18090/load/comparisons \
  -H 'Content-Type: application/json' -d '{"concurrency":300}'
```

| 동시 300건 | 0단계 · 순정 | 3단계 · 멱등성 키 | |
| --- | --- | --- | --- |
| 사라진 차감 | 285칸 | **0칸** | 고쳐졌다 |
| p95 응답 지연 | 82.7ms | **115.8ms** | 그대로다 (오히려 40% 늘었다) |
| 처리량 | 3,093건/s | 2,400건/s | 그대로다 |
| 풀 앞 대기 최대 | 261건 | 290건 | 그대로다 |

**정합성은 DB 제약 한 줄로 끝났다. 응답은 그 뒤에도 남았다.**
보호 장치는 결과를 지키는 물건이지 빨리 대답하게 하는 물건이 아니다 —
검사를 하나 더 하므로 오히려 조금 더 걸린다.

이 표가 이 프로젝트가 Kafka로 넘어가는 근거 전부다.
초과 예약과 이중 예약을 다 막고도 남는 것이 있고, 그것은 조건을 더 건다고 사라지지 않는다.

### 이것이 고쳐지지 않는 이유

풀을 키우면 될 것 같지만 커넥션은 PostgreSQL 쪽 자원이기도 해서 무한정 늘릴 수 없고,
늘려도 DB가 같은 일을 더 많이 동시에 할 뿐 한 건당 비용은 그대로다.
천장의 위치만 옮겨질 뿐 천장 자체는 남는다.

남은 길은 **응답과 처리를 떼어 놓는 것**이다.
접수만 하고 즉시 응답한 뒤 처리는 뒤에서 하면, 이 대기는 응답 시간에서 사라진다.
큐가 필요한 자리가 여기다.

## 8. 종료

```bash
docker compose --profile app down
```

PostgreSQL 데이터까지 초기화할 때만 다음을 사용한다.

```bash
docker compose down -v
```

## 9. 다음 분리 기준

다음 문제가 실제로 나타날 때 처리 업무를 별도 프로세스로 분리한다.

- 예약 처리 시간이 길어 HTTP 응답이 느려짐 — **7절에서 확인했다**
- 순간적인 요청 폭증을 흡수할 버퍼가 필요함 — **7절에서 확인했다**
- 실패한 업무를 나중에 재시도해야 함
- API와 처리 업무를 독립적으로 확장해야 함
- API 재시작과 무관하게 처리 대상을 보존해야 함
