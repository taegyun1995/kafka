package com.popup.pass.reservation.service;

import com.popup.pass.reservation.domain.Reservation;
import com.popup.pass.reservation.domain.ReservationStatus;
import com.popup.pass.reservation.domain.RoundStatus;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 단일 서버 예약의 기본 규칙과 멱등성 계약을 확인한다.
 *
 * <p>서로 다른 사용자의 동시 정원 경쟁은 원자적 차감 단계에서 다룬다. 여기서는 같은 키의
 * 재시도와 같은 키가 동시에 들어오는 경우를 멱등성의 기준으로 고정한다.
 */
@DisplayName("선착순 예약의 단일 서버 처리")
class ReservationServiceTest extends PostgresBackedTest {

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("정원이 남아 있으면 예약이 확정된다")
    void confirmsWhenCapacityRemains() {
        var roundId = newRound(2);

        var reservation = reservationService.reserve(roundId, "user-1", "key-confirm-1");

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
        assertThat(remainingCount(roundId)).isEqualTo(1);
    }

    @Test
    @DisplayName("확정된 사용자가 다시 요청하면 중복으로 거절하고 정원은 깎지 않는다")
    void rejectsDuplicateAfterConfirmed() {
        var roundId = newRound(5);
        reservationService.reserve(roundId, "user-1", "key-duplicate-1");

        var second = reservationService.reserve(roundId, "user-1", "key-duplicate-2");

        assertThat(second.getStatus()).isEqualTo(ReservationStatus.REJECTED_DUPLICATE);
        assertThat(remainingCount(roundId)).isEqualTo(4);
    }

    @Test
    @DisplayName("정원을 넘으면 매진으로 거절된다")
    void rejectsWhenSoldOut() {
        var roundId = newRound(1);
        reservationService.reserve(roundId, "user-1", "key-sold-out-1");

        var soldOut = reservationService.reserve(roundId, "user-2", "key-sold-out-2");

        assertThat(soldOut.getStatus()).isEqualTo(ReservationStatus.REJECTED_SOLD_OUT);
        assertThat(remainingCount(roundId)).isZero();
    }

    @Test
    @DisplayName("매진으로 거절된 사용자는 자리가 생기면 다시 예약할 수 있다")
    void allowsRetryAfterSoldOutRejection() {
        var roundId = newRound(1);
        assertThat(reservationService.reserve(roundId, "user-1", "key-retry-1").getStatus())
                .isEqualTo(ReservationStatus.CONFIRMED);
        assertThat(reservationService.reserve(roundId, "user-2", "key-retry-2").getStatus())
                .isEqualTo(ReservationStatus.REJECTED_SOLD_OUT);

        // 취소 등으로 자리가 하나 생긴 상황을 만든다.
        jdbcTemplate.update("update reservation_rounds set remaining_count = 1 where id = ?", roundId);

        // 거절 기록이 남아 있어도 확정된 예약이 없으므로 다시 예약할 수 있어야 한다.
        assertThat(reservationService.reserve(roundId, "user-2", "key-retry-3").getStatus())
                .isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("없는 회차에 요청하면 회차 없음으로 거절된다")
    void rejectsWhenRoundIsMissing() {
        var rejected = reservationService.reserve("no-such-round", "user-1", "key-missing-round");

        assertThat(rejected.getStatus()).isEqualTo(ReservationStatus.REJECTED_ROUND_NOT_FOUND);
    }

    @Test
    @DisplayName("회차를 조회하면 남은 자리가 보인다")
    void exposesRemainingCountOfRound() {
        var roundId = newRound(2);
        reservationService.reserve(roundId, "user-1", "key-view-1");

        var round = reservationService.findRound(roundId).orElseThrow();

        assertThat(round.getCapacity()).isEqualTo(2);
        assertThat(round.getRemainingCount()).isEqualTo(1);
        assertThat(round.getStatus()).isEqualTo(RoundStatus.OPEN);
    }

    @Test
    @DisplayName("마감한 회차는 자리가 남아 있어도 예약을 받지 않는다")
    void rejectsAfterRoundClosed() {
        var roundId = newRound(5);
        reservationService.closeRound(roundId);

        var rejected = reservationService.reserve(roundId, "user-1", "key-closed-1");

        // 자리가 남았는데도 거절된 것이므로 매진과 구분되어야 한다.
        assertThat(rejected.getStatus()).isEqualTo(ReservationStatus.REJECTED_ROUND_CLOSED);
        assertThat(remainingCount(roundId)).isEqualTo(5);
    }

    @Test
    @DisplayName("마감은 설정 연산이라 여러 번 호출해도 결과가 같다")
    void closingRoundIsIdempotent() {
        var roundId = newRound(1);

        var first = reservationService.closeRound(roundId).orElseThrow();
        var second = reservationService.closeRound(roundId).orElseThrow();

        assertThat(first.getStatus()).isEqualTo(RoundStatus.CLOSED);
        assertThat(second.getStatus()).isEqualTo(RoundStatus.CLOSED);
        assertThat(remainingCount(roundId)).isEqualTo(1);
    }

    @Test
    @DisplayName("결과는 requestId로 다시 조회할 수 있다")
    void findsReservationByRequestId() {
        var roundId = newRound(2);
        var reservation = reservationService.reserve(roundId, "user-1", "key-query-1");

        var found = reservationService.findReservation(reservation.getRequestId()).orElseThrow();

        assertThat(found.getRequestId()).isEqualTo(reservation.getRequestId());
        assertThat(found.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("같은 멱등성 키로 재시도하면 처음 결과를 그대로 돌려준다")
    void returnsOriginalResultForRetry() {
        var roundId = newRound(2);

        var first = reservationService.reserve(roundId, "user-1", "key-idempotent-1");
        var retry = reservationService.reserve(roundId, "user-1", "key-idempotent-1");

        assertThat(retry.getRequestId()).isEqualTo(first.getRequestId());
        assertThat(retry.getStatus()).isEqualTo(first.getStatus());
        assertThat(retry.getCreatedAt()).isEqualTo(first.getCreatedAt());
        assertThat(reservationCount(roundId)).isOne();
        assertThat(remainingCount(roundId)).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키를 다른 요청 내용에 재사용하면 충돌한다")
    void rejectsSameKeyWithDifferentRequest() {
        var roundId = newRound(2);
        reservationService.reserve(roundId, "user-1", "key-conflict-1");

        assertThatThrownBy(() -> reservationService.reserve(roundId, "user-2", "key-conflict-1"))
                .isInstanceOf(IdempotencyConflictException.class);

        assertThat(reservationCount(roundId)).isOne();
        assertThat(remainingCount(roundId)).isEqualTo(1);
    }

    @Test
    @DisplayName("같은 키의 동시 요청도 한 건의 결과만 남긴다")
    void handlesConcurrentRetryWithOneResult() throws Exception {
        var roundId = newRound(2);
        var pool = Executors.newFixedThreadPool(2);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        Callable<Reservation> request = () -> {
            ready.countDown();
            start.await(5, TimeUnit.SECONDS);
            return reservationService.reserve(roundId, "user-1", "key-concurrent-1");
        };

        try {
            var firstFuture = pool.submit(request);
            var retryFuture = pool.submit(request);
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            var first = firstFuture.get(10, TimeUnit.SECONDS);
            var retry = retryFuture.get(10, TimeUnit.SECONDS);

            assertThat(retry.getRequestId()).isEqualTo(first.getRequestId());
            assertThat(retry.getStatus()).isEqualTo(first.getStatus());
            assertThat(reservationCount(roundId)).isOne();
            assertThat(remainingCount(roundId)).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    private String newRound(int capacity) {
        var roundId = "round-" + UUID.randomUUID();
        reservationService.createRound(roundId, "테스트 회차", capacity);
        return roundId;
    }

    private int remainingCount(String roundId) {
        return jdbcTemplate.queryForObject(
                "select remaining_count from reservation_rounds where id = ?", Integer.class, roundId);
    }

    private int reservationCount(String roundId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from reservations where round_id = ?", Integer.class, roundId);
    }
}
