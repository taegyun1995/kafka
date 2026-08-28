package com.popup.pass.study.steps;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 곁가지 · 1인 2매를 허용하는 세계. 원자적 차감만 남고 중복 규칙이 없다.
 *
 * <p>{@code (round_id, user_id)} 유니크가 성립하지 않으므로 부분 Index도, 확정 예약 조회도 쓸 수 없다.
 * 2단계가 안전해 보였던 것이 예약이라는 업무의 성질 때문이 아니라
 * <strong>1인 1매 규칙이 멱등성을 대신 제공하고 있었기 때문</strong>임을 보여 주는 자리다.
 */
public class StepWithoutDuplicateRule extends JdbcStudyStep {

    public StepWithoutDuplicateRule(JdbcTemplate jdbc) {
        super(jdbc);
    }

    @Override
    public String id() {
        return "no-duplicate-rule";
    }

    @Override
    public int order() {
        return 4;
    }

    @Override
    public String name() {
        return "곁가지 · 1인 2매 허용";
    }

    @Override
    public String addedGuard() {
        return "원자적 차감만 (중복 규칙 없음)";
    }

    @Override
    public String summary() {
        return "기획이 1인 2매로 바뀌면 중복 규칙이 사라진다. 그 순간 무엇이 드러나는지 본다.";
    }

    @Override
    public Attempt reserve(String idempotencyKey, String roundId, String userId) {
        var requestId = newRequestId();
        return decrementIfAvailable(roundId)
                ? record(requestId, roundId, userId, CONFIRMED)
                : record(requestId, roundId, userId, REJECTED_SOLD_OUT);
    }
}
