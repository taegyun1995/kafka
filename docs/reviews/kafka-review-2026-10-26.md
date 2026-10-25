# Kafka HTML · YAML Builder 검토 기록

검토일: 2026-10-26. 기준 커밋: `566c8ff`.

주제별 사전 검토 10차를 진행하고 확인한 문제를 수정한 뒤, 완성본에 대해 추가 10차 검토를 진행했다. 같은 검사를 20회 실행한 기록이 아니라, 아래와 같이 서로 다른 범위와 반례를 나눠 검토한 기록이다. 추가 검토 중 발견한 문제는 수정하고 관련 검사를 다시 실행했다.

## 사전 검토 10차

| 차수 | 검토 범위 | 확인 결과 |
| --- | --- | --- |
| 1 | 문서 구조·개념 목록·Cluster/Control Plane 관계 | 개념 섹션 61개 확인. 전체 Cluster와 내부 Control Plane을 동급 탭으로 표시한 구조 수정 필요 |
| 2 | Event/Command·Application Event·Outbox/Inbox/Saga | DB Commit/발행 간극, 중복 기록과 업무 효과의 원자성 확인. 외부 PG 효과 경계 설명 보강 필요 |
| 3 | Producer·Key·Schema·acks·ISR·Retry·TCP | 재시도가 항상 delivery timeout까지 이어진다는 단정 수정 필요. acks=0 예외를 TCP 설명에도 명시 |
| 4 | Consumer·Group·Offset·Rebalance·Thread·재전달 실험 | position/commit, 다음 Offset, 동일 Group 재전달 절차 확인. Lag 측정 기준을 도구별로 구분할 필요 |
| 5 | 전달 보장·EOS·Kafka Transaction·외부 DB | Kafka 내부 EOS와 MySQL 효과 구분 확인. Transaction 예제의 Timeout 후 자원 정리 보강 필요 |
| 6 | Broker 저장·Retention·Compaction·Tombstone·성능·운영 | Compaction 빈 Offset과 범위를 벗어난 Offset 혼동 수정. Tombstone 전체 복원 스캔 시간 조건 보강 |
| 7 | KRaft·ZooKeeper·Controller·Quorum·버전 차이 | Kafka 4.3 공식 설정·운영 문서와 대조. Controller 다수결과 Replica ISR을 구분하는 설명 유지 |
| 8 | Spring Producer·Listener·Transaction·JSON·Builder | 숫자 1.5 입력에도 max-poll-records: 100을 출력하고 복사가 가능한 문제를 브라우저에서 재현 |
| 9 | ID·로컬 링크·팝업 잔여 참조·모바일 기본 화면 | 중복 ID·깨진 로컬 링크·팝업 참조 없음. 기존 스타일 유지 조건 확인 |
| 10 | 앞선 지적의 반례·공식 구현 재검증 | CLI Offset 조회 기본 isolation을 소스로 확인. 중복 consumer YAML이라는 후보는 출력 범위 중첩에 따른 오탐으로 기각 |

## 수정한 내용과 근거

| 항목 | 변경 | 근거 |
| --- | --- | --- |
| 탐색 계층 | 상단 Kafka Cluster 아래에서 저장·복제 / Control Plane 선택. Control Plane을 읽어도 상위 Cluster 활성 표시 유지 | Cluster 전체와 Broker/Controller 역할 구분. [KRaft 운영](https://kafka.apache.org/43/operations/kraft/) |
| 재시도 기한 | retries 소진 등으로 delivery.timeout.ms보다 먼저 실패할 수 있음을 명시 | [Producer Configs](https://kafka.apache.org/43/configuration/producer-configs/) |
| Lag | Group CLI의 HW − Committed Offset과 Consumer API의 isolation별 끝/position 기준 구분 | [KafkaConsumer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html), [OffsetsUtils](https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/OffsetsUtils.java), [ListOffsetsOptions](https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/admin/ListOffsetsOptions.java) |
| Transaction 예제 | init 실패를 포함한 finally close 추가. commit Timeout/Interrupt 때 abort로 바꾸지 않는 설명 | [KafkaProducer API](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html) |
| 외부 PG | PG 성공 뒤 DB 기록 전 중단 구간, 안정적인 업무/멱등 키와 결과 조회·대사 필요성 명시 | [Kafka 전달 보장의 외부 시스템 경계](https://kafka.apache.org/43/design/design/), [Saga](https://microservices.io/patterns/data/saga.html) |
| Compaction | 보존 범위 안의 빈 Offset은 다음 남은 Record로 진행함을 명시. Retention의 범위 초과와 구분 | [Log Compaction](https://kafka.apache.org/43/design/design/#log-compaction) |
| Tombstone | 최대 중단 시간에 더해 따라잡기·전체 상태 복원 스캔 시간 고려 | [Topic Configs · delete.retention.ms](https://kafka.apache.org/43/configuration/topic-configs/) |
| Builder 입력 | 빈 값·소수·음수·범위 초과와 설정 간 제약을 표시하고 잘못된 출력의 복사·저장 차단 | 브라우저 재현 및 Producer 설정 조건 대조 |
| Builder 메시지 계약 | String/JSON DTO/Byte Array/직접 지정. JSON은 대상 타입·신뢰 패키지·Type Header 정책 함께 생성 | [Spring Kafka 3.3 Serdes](https://docs.spring.io/spring-kafka/reference/3.3/kafka/serdes.html) |
| Boot 설정 위치 | 기존 max-poll-interval 대신 consumer.properties의 [max.poll.interval.ms]로 출력 | [Spring Boot 3.4 설정 목록](https://docs.spring.io/spring-boot/3.4/appendix/application-properties/index.html)에서 별도 typed 속성이 아님을 확인 |
| 반응형 | 761~980px의 Record/Produce/Session 도식 세로 배치. 320/375px 긴 설정 키의 Grid 최소 폭·줄바꿈 수정 | 실제 브라우저 측정 및 화면 확인 |
| 조건부 입력 | CSS의 display:grid가 hidden을 덮던 DTO 입력을 확실히 숨김 | 기본 String 모드와 JSON 모드에서 실제 표시 확인 |

## 수정 후 추가 검토 10차

| 차수 | 검토 범위 | 결과 |
| --- | --- | --- |
| 11 | 구조 보존·로컬 링크·스크립트·배포 경로 | 기존 61개 개념 섹션 ID와 순서 유지. 중복 ID·깨진 로컬 링크·팝업/API 참조 없음. JS 구문 검사와 git diff --check 통과 |
| 12 | 수정된 개념·예제·설정의 공식 근거 대조 | Lag·Retention·Timeout·PG 경계 재검토. Boot의 max.poll.interval.ms 출력 위치 추가 수정 후 재확인 |
| 13 | 기본 구성과 5개 프리셋 | Producer/Consumer/Listener 범위, TX Prefix, read_committed 출력 비교. 5개 출력 모두 Ruby Psych YAML 파서로 파싱·계층 검증 |
| 14 | 입력 오류·경계값·연동 조건 | 빈 값·1.5·-1·2147483648·멱등성 retries=0/in-flight=6·잘못된 Broker URL·빈 Group ID 차단. delivery=30004 오류, 30005 허용 확인 |
| 15 | 범위 의존성과 제외 설정 | Listener 선택 시 Consumer 포함 및 auto commit false 고정. 범위 전체 해제 차단. 제외된 Producer의 잘못된 숫자는 Consumer 출력에 영향 없음 |
| 16 | 직렬화·JSON·사용자 문자열 | Byte Array 양쪽 클래스 확인. 직접 지정의 빈 Serializer 차단. JSON DTO 필수 입력·대상 클래스·패키지·헤더 설정 확인. IPv6/복수 Broker와 따옴표·콜론·# 포함 Group ID 보존 |
| 17 | 복사·파일 저장·독립 파싱 | 복사한 값과 출력 문자열 일치. 실제 Downloads/application.yml 계열 파일과 생성 결과 바이트 비교. 최종 출력과 JSON 계약을 YAML 파서로 다시 검증 |
| 18 | PC 및 태블릿 레이아웃 | 768/1024/1440px에서 본문 61개 섹션의 영역 넘침·주요 본문 잘림 검사. 768px 도식 3곳 수정 후 모두 0. Cluster 내부 탐색 화면 확인 |
| 19 | 모바일·줄바꿈·키보드 | 320/375/390px 본문과 Builder의 페이지 가로 넘침·주요 설명/라벨 잘림 0. 모바일 비교 표 카드 5개, 일반 표/코드의 내부 스크롤 확인. 모바일 목차 Enter 이동 후 닫힘, Cluster/Control Plane Enter 탐색 확인 |
| 20 | 최종 회귀·예외·누락·정적 배포 설정 | 수정된 숫자 검증·JSON·프리셋·복사·저장을 재확인. 두 페이지 콘솔 오류 0. Vercel은 site/를 정적으로 제공하며 API 서버 의존 없음 |

레이아웃 검사는 요청한 뷰포트와 브라우저의 실제 innerWidth가 일치하는 경우만 결과로 채택했다. 일반 문장과 설정 키는 영역 안에서 줄바꿈하며, YAML/코드 원문의 들여쓰기와 긴 줄은 내부 가로 스크롤로 보존한다. 비교 표는 모바일에서 행별 카드로 표시한다.

추가 검토 중 자동화의 다운로드 이벤트 대기가 멈춰 세션을 복구했다. 해당 시도는 성공으로 계산하지 않았다. 이후 실제 다운로드 파일을 찾아 생성 문자열과 바이트 비교하고 YAML 파서로 확인했다. 고정 사이드바 링크에 자동화 click이 불필요한 페이지 스크롤을 일으킨 경우에는 키보드 Enter 경로로 실제 해시·도착 위치·활성 목차를 확인했다.

## 확인 범위의 한계

실제 Kafka·MySQL·PG를 띄운 장애 실험이나 부하 테스트를 실행한 것은 아니다. 이번 결과는 정적 문서의 개념·공식 자료 대조, 브라우저의 설정 생성·입력 검증·파일 내보내기, 반응형 화면 검사다. YAML 문법과 설정 위치 검증을 통과한 것이 업무 DTO 클래스의 존재, Broker 연결, Spring 애플리케이션 전체 실행을 증명하지는 않는다.

실제 iOS Safari·Android 기기에서의 터치 검사는 수행하지 않았다. 반응형 검사는 Chrome의 명시적 뷰포트 크기로 수행했다. 오류가 영원히 없다는 보장이 아니라, 위 범위에서 확인한 문제를 수정하고 재검증했다는 기록이다.

## 후속 사용성 수정 · acks 잠금 안내

사용자 피드백: 멱등성 체크가 acks보다 아래에 있어 선택기가 잠긴 이유를 찾기 어려웠다. 멱등성과 Transaction 체크를 Producer 옵션의 맨 위로 이동하고, acks 옆에 현재 상태(멱등성으로 고정 / Transaction으로 고정 / 직접 선택)와 바로 아래에 해제 방법을 표시했다. Transaction으로 잠긴 멱등성에도 별도 이유를 표시한다. 기존의 자세한 보장 범위 설명은 같은 영역의 펼치기에 보존했다.

기본 잠금, 멱등성 해제 후 acks=1/0, Transaction 활성화·해제, 초기화와 키보드 펼치기를 확인했다. 5개 프리셋의 생성 YAML은 변경 전 결과와 전부 동일했다. 320/390/768/1440px에서 페이지 가로 넘침과 새 안내 영역의 잘림이 없음을 확인했다. Builder 내부 해시 이동이 전용 페이지 스타일을 해제하던 문제도 수정해 다시 확인했다. 콘솔 오류는 없었다.
