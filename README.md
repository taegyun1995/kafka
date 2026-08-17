# Kafka Study

Kafka 학습 문서는 정적 HTML 사이트로 관리합니다.

## 문서 원본

- 메인 문서: `site/index.html`
- YAML 설정 빌더: `site/yaml-builder.html`
- 스타일 및 동작: `site/assets/`
- 다이어그램 원본: `site/assets/diagrams-src/`

Kafka 내용은 Markdown 문서와 중복 관리하지 않습니다. 내용 수정은 `site/` 아래의 HTML과 관련 에셋에 반영합니다.

## 로컬 실행

프로젝트 루트에서 다음 명령을 실행합니다.

```bash
python3 -m http.server 8765 --directory site
```

브라우저에서 다음 주소로 접속합니다.

- 메인 문서: <http://localhost:8765/>
- YAML 설정 빌더: <http://localhost:8765/yaml-builder.html>
