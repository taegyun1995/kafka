package com.popup.pass.study.steps;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

/** 단계들이 함께 쓰는 SQL 도구. 여기에는 보호 장치를 넣지 않는다. */
public abstract class JdbcStudyStep implements StudyStep {

    /** 화면에서 경합을 눈에 보이게 만들기 위한 연출값. */
    public static final long DEFAULT_RACE_WINDOW_MILLIS = 50;

    protected final JdbcTemplate jdbc;

    private final long raceWindowMillis;

    protected JdbcStudyStep(JdbcTemplate jdbc) {
        this(jdbc, DEFAULT_RACE_WINDOW_MILLIS);
    }

    protected JdbcStudyStep(JdbcTemplate jdbc, long raceWindowMillis) {
        this.jdbc = jdbc;
        this.raceWindowMillis = raceWindowMillis;
    }

    @Override
    public boolean needsConfirmedUniqueIndex() {
        return false;
    }

    /**
     * 읽기와 쓰기 사이의 틈을 눈에 보이게 벌린다.
     * 이 지연이 없어도 같은 일이 일어난다. 확률이 낮아져 눈에 덜 띌 뿐이다.
     *
     * <p>다만 이것은 <strong>연출</strong>이므로 응답 시간을 재는 자리에서는 0으로 둔다.
     * 켠 채로 지연을 재면 재는 것이 코드가 아니라 이 {@code sleep}이 된다.
     */
    protected void widenRaceWindow() {
        if (raceWindowMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(raceWindowMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    protected static String newRequestId() {
        return UUID.randomUUID().toString();
    }

    protected boolean confirmedExists(String roundId, String userId) {
        Integer count = jdbc.queryForObject(
                "select count(*) from study_reservations where round_id = ? and user_id = ? and status = ?",
                Integer.class, roundId, userId, CONFIRMED);
        return count != null && count > 0;
    }

    /** 조건부 UPDATE 한 문장으로 차감한다. 읽는 시점과 쓰는 시점이 갈라지지 않는다. */
    protected boolean decrementIfAvailable(String roundId) {
        return jdbc.update(
                "update study_rounds set remaining_count = remaining_count - 1 where id = ? and remaining_count > 0",
                roundId) == 1;
    }

    protected String findHandledStatus(String requestId) {
        var found = jdbc.queryForList(
                "select status from study_reservations where request_id = ?", String.class, requestId);
        return found.isEmpty() ? null : found.get(0);
    }

    protected Attempt record(String requestId, String roundId, String userId, String status) {
        jdbc.update("insert into study_reservations (request_id, round_id, user_id, status) values (?, ?, ?, ?)",
                requestId, roundId, userId, status);
        return new Attempt(requestId, status);
    }

    protected static TransactionTemplate newTransactionTemplate(PlatformTransactionManager transactionManager) {
        var template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        return template;
    }
}
