package com.popup.pass.study.steps;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 2단계 · 확정 예약에만 걸리는 부분 Unique Index를 더한다.
 * 애플리케이션 조회는 그대로 뚫리지만 최종 판정을 DB가 한다.
 * 위반된 Transaction은 통째로 Rollback되어 차감도 되돌아가므로, 거절 기록은 새 Transaction이 남긴다.
 */
public class Step2UniqueIndex extends JdbcStudyStep {

    protected final TransactionTemplate transactionTemplate;

    public Step2UniqueIndex(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        this(jdbc, transactionManager, DEFAULT_RACE_WINDOW_MILLIS);
    }

    public Step2UniqueIndex(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
                            long raceWindowMillis) {
        super(jdbc, raceWindowMillis);
        this.transactionTemplate = newTransactionTemplate(transactionManager);
    }

    @Override
    public String id() {
        return "step2";
    }

    @Override
    public int order() {
        return 2;
    }

    @Override
    public String name() {
        return "2단계 · 부분 Unique Index";
    }

    @Override
    public String addedGuard() {
        return "unique (round_id, user_id) where status = 'CONFIRMED'";
    }

    @Override
    public String summary() {
        return "중복의 최종 판정을 DB가 한다. 다만 재시도한 클라이언트는 원래 결과를 되찾지 못한다.";
    }

    @Override
    public boolean needsConfirmedUniqueIndex() {
        return true;
    }

    @Override
    public Attempt reserve(String idempotencyKey, String roundId, String userId) {
        var requestId = newRequestId();
        return reserveWith(requestId, roundId, userId);
    }

    protected Attempt reserveWith(String requestId, String roundId, String userId) {
        try {
            return transactionTemplate.execute(tx -> attempt(requestId, roundId, userId));
        } catch (DuplicateKeyException conflict) {
            return transactionTemplate.execute(tx -> onConflict(requestId, roundId, userId));
        }
    }

    protected Attempt onConflict(String requestId, String roundId, String userId) {
        return record(requestId, roundId, userId, REJECTED_DUPLICATE);
    }

    protected Attempt attempt(String requestId, String roundId, String userId) {
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
