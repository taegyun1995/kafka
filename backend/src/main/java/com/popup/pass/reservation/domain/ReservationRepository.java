package com.popup.pass.reservation.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ReservationRepository extends JpaRepository<Reservation, String> {

    Optional<Reservation> findByIdempotencyKey(String idempotencyKey);

    /**
     * 거절 기록도 모두 남기므로 중복 판단은 확정된 예약만 대상으로 한다.
     * 애플리케이션의 빠른 조회와 DB의 부분 Unique Index(status = 'CONFIRMED')가 같은 기준을 쓴다.
     */
    boolean existsByRoundIdAndUserIdAndStatus(String roundId, String userId, ReservationStatus status);
}
