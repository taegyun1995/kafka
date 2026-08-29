package com.popup.pass.study.steps;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 3단계 · 멱등성 키를 더한다. 클라이언트가 보낸 키가 그대로 requestId가 되고 PK가 된다.
 * 이미 처리한 requestId면 새로 판단하지 않고 저장된 결과를 그대로 돌려준다.
 * 프로덕션 코드({@code ReservationService})에는 세 보호 장치를 모두 반영했다.
 * 이 클래스는 각 장치가 무엇을 해결하는지 분리해서 보여 주는 학습용 단계다.
 */
public class Step3IdempotencyKey extends Step2UniqueIndex {

    public Step3IdempotencyKey(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        super(jdbc, transactionManager);
    }

    public Step3IdempotencyKey(JdbcTemplate jdbc, PlatformTransactionManager transactionManager,
                               long raceWindowMillis) {
        super(jdbc, transactionManager, raceWindowMillis);
    }

    @Override
    public String id() {
        return "step3";
    }

    @Override
    public int order() {
        return 3;
    }

    @Override
    public String name() {
        return "3단계 · 멱등성 키";
    }

    @Override
    public String addedGuard() {
        return "Idempotency-Key → request_id (PK)";
    }

    @Override
    public String summary() {
        return "클라이언트가 만든 키가 \"이 둘은 같은 업무 요청이다\"를 말해 준다.";
    }

    @Override
    public Attempt reserve(String idempotencyKey, String roundId, String userId) {
        // 키를 읽는 유일한 단계다. 없으면 서버가 만들고, 그때는 재시도가 새 요청이 된다.
        var requestId = idempotencyKey == null || idempotencyKey.isBlank() ? newRequestId() : idempotencyKey;
        return reserveWith(requestId, roundId, userId);
    }

    @Override
    protected Attempt attempt(String requestId, String roundId, String userId) {
        var handled = findHandledStatus(requestId);
        if (handled != null) {
            return new Attempt(requestId, handled);
        }
        return super.attempt(requestId, roundId, userId);
    }

    @Override
    protected Attempt onConflict(String requestId, String roundId, String userId) {
        // PK 위반이면 같은 요청이 먼저 처리된 것이고, Index 위반이면 남의 확정이 먼저 들어온 것이다.
        var handled = findHandledStatus(requestId);
        return handled != null
                ? new Attempt(requestId, handled)
                : record(requestId, roundId, userId, REJECTED_DUPLICATE);
    }
}
