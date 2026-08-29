# Kafka Study

Kafka 학습 문서는 정적 HTML 사이트로 관리합니다.

## 문서 원본

- 메인 문서: `site/index.html`
- YAML 설정 빌더: `site/yaml-builder.html`
- 스타일 및 동작: `site/assets/`
- 다이어그램 원본: `site/assets/diagrams-src/`

Kafka 내용은 Markdown 문서와 중복 관리하지 않습니다. 내용 수정은 `site/` 아래의 HTML과 관련 에셋에 반영합니다.

## 실습 프로젝트

- [PopupPass 학습 기획](site/popup-pass-plan.html) — 무엇을 왜 만드는가, 어디까지가 완료인가. **프로젝트 입구**
- [PopupPass 기초 아키텍처](site/popup-pass-architecture.html) — 지금 코드의 구성 요소 · 요청 흐름 · 테이블
- [PopupPass 보호 장치 실행](site/popup-pass-guards.html) — 선착순을 무엇이 지키는가. 실제 서버에 동시 요청을 일으킨다
- [PopupPass 부하 관측](site/popup-pass-load.html) — 동기 처리가 어디서 느려지는가. 처리량의 천장, 자라는 응답 지연, 그리고 보호 장치를 다 켜도 남는 것
- [PopupPass 흐름 시연](site/popup-pass-demo.html) — 동기 구현을 실제로 호출한다. 한 건씩 보낼 때와 겹쳐 보낼 때의 차이
- [PopupPass 단일 서버 MVP 실행환경](backend/README.md) — 실행 명령과 API 사용법

Kafka 이론과 실습 프로젝트 설계는 HTML로 관리합니다.

## 실제 실행환경

실습 프로젝트는 먼저 PostgreSQL과 하나의 Spring Boot 서버로 예약 업무를 완성합니다. 처리 지연·재시도·버퍼링·독립 확장 같은 분리 이유가 확인되면 Kafka와 별도 Worker를 다음 단계로 추가합니다. 처리 지연과 버퍼링은 [부하 관측](site/popup-pass-load.html)에서 숫자로 확인했습니다 — 동시 요청을 60배 올리는 동안 처리량은 2.2배 느는 데 그치고 p95 지연은 24배가 됩니다. 자세한 명령은 [backend/README.md](backend/README.md)를 확인합니다.

## 로컬 실행

프로젝트 루트에서 다음 명령을 실행합니다.

```bash
python3 -m http.server 8765 --directory site
```

브라우저에서 다음 주소로 접속합니다.

- 메인 문서: <http://localhost:8765/>
- YAML 설정 빌더: <http://localhost:8765/yaml-builder.html>
- PopupPass 실습: <http://localhost:8765/popup-pass-plan.html>

보호 장치 실행·부하 관측·흐름 시연 화면은 실제 서버가 있어야 동작합니다. 별도 터미널에서 다음을 실행합니다.

```bash
docker compose up -d postgres
cd backend && gradle bootRun --args='--spring.profiles.active=study'
```

애플리케이션은 `http://localhost:18090`, PostgreSQL은 `localhost:15432`를 씁니다.
