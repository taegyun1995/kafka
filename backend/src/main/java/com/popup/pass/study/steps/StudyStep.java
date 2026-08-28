package com.popup.pass.study.steps;

/**
 * 선착순 예약을 지키는 보호 장치를 0단계부터 하나씩 더해 간다.
 *
 * <p>단계마다 막히는 것과 남는 것:
 * <pre>
 *   0단계  보호 없음            → 초과 판매 · 이중 예약 · 재시도 중복
 *   1단계  + 원자적 차감        → 초과 판매만 막힌다
 *   2단계  + 부분 Unique Index  → 이중 예약도 막힌다. 재시도는 원래 결과를 잃는다
 *   3단계  + 멱등성 키          → 재시도가 처음 결과를 그대로 돌려받는다
 * </pre>
 *
 * <p>구현은 프로덕션의 {@code ReservationService}와 같은 업무를 하지만, 무엇이 어디서 깨지는지
 * 눈에 보이도록 JPA 없이 SQL을 그대로 쓴다. 전용 테이블 {@code study_*}만 건드린다.
 */
public interface StudyStep {

    String CONFIRMED = "CONFIRMED";
    String REJECTED_SOLD_OUT = "REJECTED_SOLD_OUT";
    String REJECTED_DUPLICATE = "REJECTED_DUPLICATE";

    /** 화면과 API가 쓰는 식별자. */
    String id();

    /** 버튼을 늘어놓는 순서. 곁가지는 본줄 뒤에 온다. */
    int order();

    String name();

    /** 이 단계에서 새로 더한 보호 장치. */
    String addedGuard();

    /** 이 단계가 무엇을 하는지 한 줄로. */
    String summary();

    /** 확정 예약 부분 Unique Index를 걸고 시작할 것인가. */
    boolean needsConfirmedUniqueIndex();

    /**
     * 예약을 시도한다.
     *
     * @param idempotencyKey 클라이언트가 보낸 키. <strong>이 값을 읽는 단계는 3단계뿐이다.</strong>
     *                       나머지 단계는 헤더를 아예 모르므로 서버가 새 식별자를 만든다.
     */
    Attempt reserve(String idempotencyKey, String roundId, String userId);

    /** 시도 한 건의 결과. requestId까지 돌려주어야 "원래 결과를 되찾을 수 있는가"가 보인다. */
    record Attempt(String requestId, String status) {
    }
}
