package com.popup.pass.reservation.domain;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ReservationRoundRepository extends JpaRepository<ReservationRound, String> {

    /**
     * 남은 자리가 있을 때만 한 칸을 차감한다.
     * 읽고 다시 저장하지 않아 Lost Update를 피하고, 업데이트된 행 수가 선착순 판정이 된다.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            update reservation_rounds
               set remaining_count = remaining_count - 1
             where id = :roundId
               and status = 'OPEN'
               and remaining_count > 0
            """, nativeQuery = true)
    int decrementIfAvailable(@Param("roundId") String roundId);
}
