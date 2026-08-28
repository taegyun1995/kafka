package com.popup.pass.reservation.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, String> {

    /**
     * 거절 기록도 모두 남기므로 중복 판단은 확정된 예약만 대상으로 한다.
     * DB의 부분 Unique Index(status = 'CONFIRMED')와 같은 기준이다.
     */
    boolean existsByRoundIdAndUserIdAndStatus(String roundId, String userId, ReservationStatus status);
}
