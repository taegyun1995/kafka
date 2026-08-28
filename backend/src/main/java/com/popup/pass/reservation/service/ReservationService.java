package com.popup.pass.reservation.service;

import com.popup.pass.reservation.domain.Reservation;
import com.popup.pass.reservation.domain.ReservationRepository;
import com.popup.pass.reservation.domain.ReservationRound;
import com.popup.pass.reservation.domain.ReservationRoundRepository;
import com.popup.pass.reservation.domain.ReservationStatus;
import com.popup.pass.reservation.domain.RoundStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * 예약 업무를 가장 단순한 방법으로 처리한다 — <strong>읽고, 판단하고, 쓴다.</strong>
 *
 * <p>동시 요청을 대비한 장치는 하나도 없다. 요청이 한 번에 하나씩 온다고 가정하며,
 * 겹쳐 들어오면 정원을 넘겨 팔거나 한 사람이 두 자리를 가져갈 수 있다.
 * 무엇이 어떻게 깨지는지는 {@code com.popup.pass.study}에서 단계별로 확인한다.
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ReservationRoundRepository roundRepository;
    private final ReservationRepository reservationRepository;

    public ReservationService(
            ReservationRoundRepository roundRepository,
            ReservationRepository reservationRepository) {
        this.roundRepository = roundRepository;
        this.reservationRepository = reservationRepository;
    }

    public ReservationRound createRound(String id, String name, int capacity) {
        return roundRepository.save(new ReservationRound(id, name, capacity));
    }

    public boolean roundExists(String roundId) {
        return roundRepository.existsById(roundId);
    }

    public Optional<ReservationRound> findRound(String roundId) {
        return roundRepository.findById(roundId);
    }

    /** 마감은 설정 연산이라 여러 번 호출해도 결과가 같다. */
    @Transactional
    public Optional<ReservationRound> closeRound(String roundId) {
        return roundRepository.findById(roundId).map(round -> {
            round.close();
            return roundRepository.save(round);
        });
    }

    /**
     * 회차를 읽어 상태와 남은 자리를 보고, 확정할 수 있으면 읽어 둔 회차에서 한 자리를 뺀다.
     * 모든 시도는 확정이든 거절이든 기록으로 남는다.
     */
    @Transactional
    public Reservation reserve(String roundId, String userId) {
        var round = roundRepository.findById(roundId).orElse(null);

        ReservationStatus status;
        if (round == null) {
            status = ReservationStatus.REJECTED_ROUND_NOT_FOUND;
        } else if (round.getStatus() != RoundStatus.OPEN) {
            status = ReservationStatus.REJECTED_ROUND_CLOSED;
        } else if (reservationRepository.existsByRoundIdAndUserIdAndStatus(
                roundId, userId, ReservationStatus.CONFIRMED)) {
            status = ReservationStatus.REJECTED_DUPLICATE;
        } else if (round.getRemainingCount() <= 0) {
            status = ReservationStatus.REJECTED_SOLD_OUT;
        } else {
            round.takeOne();
            roundRepository.save(round);
            status = ReservationStatus.CONFIRMED;
        }

        var reservation = reservationRepository.save(
                new Reservation(UUID.randomUUID().toString(), roundId, userId, status, Instant.now()));
        log.info("reservation handled requestId={} roundId={} userId={} status={}",
                reservation.getRequestId(), roundId, userId, status);
        return reservation;
    }

    public Optional<Reservation> findReservation(String requestId) {
        return reservationRepository.findById(requestId);
    }
}
