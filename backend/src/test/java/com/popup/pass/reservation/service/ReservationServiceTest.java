package com.popup.pass.reservation.service;

import com.popup.pass.reservation.domain.ReservationStatus;
import com.popup.pass.reservation.domain.RoundStatus;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 요청이 한 번에 하나씩 올 때의 예약 업무를 확인한다.
 *
 * <p>동시 요청과 재시도는 여기서 다루지 않는다. 지금 코드에는 그것을 지킬 장치가 없기 때문이다.
 * 겹쳐 들어오면 무엇이 어떻게 깨지는지는 {@code com.popup.pass.study}의 단계별 테스트에서 본다.
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

        var reservation = reservationService.reserve(roundId, "user-1");

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
        assertThat(remainingCount(roundId)).isEqualTo(1);
    }

    @Test
    @DisplayName("확정된 사용자가 다시 요청하면 중복으로 거절하고 정원은 깎지 않는다")
    void rejectsDuplicateAfterConfirmed() {
        var roundId = newRound(5);
        reservationService.reserve(roundId, "user-1");

        var second = reservationService.reserve(roundId, "user-1");

        assertThat(second.getStatus()).isEqualTo(ReservationStatus.REJECTED_DUPLICATE);
        assertThat(remainingCount(roundId)).isEqualTo(4);
    }

    @Test
    @DisplayName("정원을 넘으면 매진으로 거절된다")
    void rejectsWhenSoldOut() {
        var roundId = newRound(1);
        reservationService.reserve(roundId, "user-1");

        var soldOut = reservationService.reserve(roundId, "user-2");

        assertThat(soldOut.getStatus()).isEqualTo(ReservationStatus.REJECTED_SOLD_OUT);
        assertThat(remainingCount(roundId)).isZero();
    }

    @Test
    @DisplayName("매진으로 거절된 사용자는 자리가 생기면 다시 예약할 수 있다")
    void allowsRetryAfterSoldOutRejection() {
        var roundId = newRound(1);
        assertThat(reservationService.reserve(roundId, "user-1").getStatus())
                .isEqualTo(ReservationStatus.CONFIRMED);
        assertThat(reservationService.reserve(roundId, "user-2").getStatus())
                .isEqualTo(ReservationStatus.REJECTED_SOLD_OUT);

        // 취소 등으로 자리가 하나 생긴 상황을 만든다.
        jdbcTemplate.update("update reservation_rounds set remaining_count = 1 where id = ?", roundId);

        // 거절 기록이 남아 있어도 확정된 예약이 없으므로 다시 예약할 수 있어야 한다.
        assertThat(reservationService.reserve(roundId, "user-2").getStatus())
                .isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    @DisplayName("없는 회차에 요청하면 회차 없음으로 거절된다")
    void rejectsWhenRoundIsMissing() {
        var rejected = reservationService.reserve("no-such-round", "user-1");

        assertThat(rejected.getStatus()).isEqualTo(ReservationStatus.REJECTED_ROUND_NOT_FOUND);
    }

    @Test
    @DisplayName("회차를 조회하면 남은 자리가 보인다")
    void exposesRemainingCountOfRound() {
        var roundId = newRound(2);
        reservationService.reserve(roundId, "user-1");

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

        var rejected = reservationService.reserve(roundId, "user-1");

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
        var reservation = reservationService.reserve(roundId, "user-1");

        var found = reservationService.findReservation(reservation.getRequestId()).orElseThrow();

        assertThat(found.getRequestId()).isEqualTo(reservation.getRequestId());
        assertThat(found.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
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
}
