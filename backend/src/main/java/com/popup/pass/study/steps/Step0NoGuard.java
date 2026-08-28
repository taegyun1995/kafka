package com.popup.pass.study.steps;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 0단계 · 순정. 읽고 → 판단하고 → 쓴다.
 * 중복도 재시도도 보지 않으며, 정원 차감이 읽기와 쓰기 두 문장으로 갈라져 있다.
 */
public class Step0NoGuard extends JdbcStudyStep {

    public Step0NoGuard(JdbcTemplate jdbc) {
        super(jdbc);
    }

    public Step0NoGuard(JdbcTemplate jdbc, long raceWindowMillis) {
        super(jdbc, raceWindowMillis);
    }

    @Override
    public String id() {
        return "step0";
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public String name() {
        return "0단계 · 순정";
    }

    @Override
    public String addedGuard() {
        return "없음";
    }

    @Override
    public String summary() {
        return "잔여 수량을 읽고, 판단하고, 읽어 둔 값으로 덮어쓴다.";
    }

    @Override
    public Attempt reserve(String idempotencyKey, String roundId, String userId) {
        // 이 단계는 Idempotency-Key라는 것을 아예 모른다. 매번 새 식별자를 만든다.
        var requestId = newRequestId();

        Integer remaining = jdbc.queryForObject(
                "select remaining_count from study_rounds where id = ?", Integer.class, roundId);
        widenRaceWindow();
        if (remaining == null || remaining <= 0) {
            return record(requestId, roundId, userId, REJECTED_SOLD_OUT);
        }
        // 읽어 둔 값으로 덮어쓴다. 그 사이 남이 깎은 것은 이 문장이 지운다.
        jdbc.update("update study_rounds set remaining_count = ? where id = ?", remaining - 1, roundId);
        return record(requestId, roundId, userId, CONFIRMED);
    }
}
