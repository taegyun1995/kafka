package com.popup.pass.study.steps;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 1단계 · 원자적 차감을 더한다. 정원은 이제 DB가 한 문장으로 지킨다.
 * 중복은 애플리케이션 조회로만 막으므로 조회와 삽입 사이가 여전히 비어 있다.
 */
public class Step1AtomicDecrement extends JdbcStudyStep {

    public Step1AtomicDecrement(JdbcTemplate jdbc) {
        super(jdbc);
    }

    public Step1AtomicDecrement(JdbcTemplate jdbc, long raceWindowMillis) {
        super(jdbc, raceWindowMillis);
    }

    @Override
    public String id() {
        return "step1";
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public String name() {
        return "1단계 · 원자적 차감";
    }

    @Override
    public String addedGuard() {
        return "update ... where remaining_count > 0";
    }

    @Override
    public String summary() {
        return "정원은 DB가 한 문장으로 지킨다. 중복은 여전히 애플리케이션 조회가 지킨다.";
    }

    @Override
    public Attempt reserve(String idempotencyKey, String roundId, String userId) {
        var requestId = newRequestId();

        boolean alreadyConfirmed = confirmedExists(roundId, userId);
        widenRaceWindow();
        if (alreadyConfirmed) {
            return record(requestId, roundId, userId, REJECTED_DUPLICATE);
        }
        if (!decrementIfAvailable(roundId)) {
            return record(requestId, roundId, userId, REJECTED_SOLD_OUT);
        }
        return record(requestId, roundId, userId, CONFIRMED);
    }
}
