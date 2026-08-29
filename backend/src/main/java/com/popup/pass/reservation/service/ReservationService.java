package com.popup.pass.reservation.service;

import com.popup.pass.reservation.domain.Reservation;
import com.popup.pass.reservation.domain.ReservationRepository;
import com.popup.pass.reservation.domain.ReservationRound;
import com.popup.pass.reservation.domain.ReservationRoundRepository;
import com.popup.pass.reservation.domain.ReservationStatus;
import com.popup.pass.reservation.domain.RoundStatus;
import org.springframework.dao.DataIntegrityViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

/**
 * 예약 업무를 가장 단순한 방법으로 처리한다 — <strong>읽고, 판단하고, 쓴다.</strong>
 *
 * <p>정원은 DB의 조건부 원자적 차감으로 지키고, 확정 예약의 1인 1매는 부분 Unique Index로
 * 최종 판정한다. 같은 업무 요청의 재시도는 {@code Idempotency-Key}로 이미 처리한 결과를
 * 돌려준다. 처리 시간이 길어져 접수와 실행을 분리할 필요가 생기면 이 업무 경계를 별도
 * Worker와 메시지 브로커로 옮긴다.
 */
@Service
public class ReservationService {

    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 128;

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ReservationRoundRepository roundRepository;
    private final ReservationRepository reservationRepository;
    private final TransactionTemplate transactionTemplate;

    public ReservationService(
            ReservationRoundRepository roundRepository,
            ReservationRepository reservationRepository,
            PlatformTransactionManager transactionManager) {
        this.roundRepository = roundRepository;
        this.reservationRepository = reservationRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public ReservationRound createRound(String id, String name, int capacity) {
        return roundRepository.save(new ReservationRound(id, name, capacity));
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
     * 같은 Idempotency-Key는 한 번의 업무 시도로 취급하고, 처음 저장한 결과를 그대로 반환한다.
     * 키 선점과 결과 저장의 경쟁은 DB 유일성 제약이 최종 판정한다.
     */
    public Reservation reserve(String roundId, String userId, String idempotencyKey) {
        validateIdempotencyKey(idempotencyKey);

        try {
            return transactionTemplate.execute(status -> reserveInTransaction(roundId, userId, idempotencyKey));
        } catch (DataIntegrityViolationException conflict) {
            // 첫 트랜잭션이 같은 키를 먼저 저장했거나, 같은 사용자의 확정 예약이 먼저 들어간 경우다.
            // 원래 트랜잭션은 이미 롤백됐으므로, 여기서는 새 트랜잭션에서 최종 결과를 확인한다.
            return transactionTemplate.execute(status -> resolveConflictAfterRollback(
                    roundId, userId, idempotencyKey, conflict));
        }
    }

    private Reservation reserveInTransaction(String roundId, String userId, String idempotencyKey) {
        var existing = reservationRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return sameRequestOrThrow(existing.get(), roundId, userId);
        }

        var round = roundRepository.findById(roundId).orElse(null);

        ReservationStatus status;
        if (round == null) {
            status = ReservationStatus.REJECTED_ROUND_NOT_FOUND;
        } else if (round.getStatus() != RoundStatus.OPEN) {
            status = ReservationStatus.REJECTED_ROUND_CLOSED;
        } else if (reservationRepository.existsByRoundIdAndUserIdAndStatus(
                roundId, userId, ReservationStatus.CONFIRMED)) {
            status = ReservationStatus.REJECTED_DUPLICATE;
        } else if (roundRepository.decrementIfAvailable(roundId) == 0) {
            // 처음 읽은 뒤 회차가 닫혔거나, 다른 요청이 마지막 자리를 가져갔을 수 있다.
            // 최신 상태를 다시 읽어 매진과 마감을 구분한다.
            status = rejectionAfterFailedDecrement(roundId, userId);
        } else {
            status = ReservationStatus.CONFIRMED;
        }

        var reservation = saveReservation(roundId, userId, idempotencyKey, status);
        log.info("reservation handled requestId={} roundId={} userId={} status={}",
                reservation.getRequestId(), roundId, userId, status);
        return reservation;
    }

    private Reservation resolveConflictAfterRollback(
            String roundId, String userId, String idempotencyKey, DataIntegrityViolationException conflict) {
        var existing = reservationRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            // 같은 키를 가진 요청이 동시에 먼저 저장한 경우다. 첫 트랜잭션의 결과를 반환한다.
            return sameRequestOrThrow(existing.get(), roundId, userId);
        }

        // 같은 사용자의 확정 예약이 먼저 들어간 경우다. 이 요청의 거절 기록은 새 트랜잭션에 남긴다.
        if (reservationRepository.existsByRoundIdAndUserIdAndStatus(
                roundId, userId, ReservationStatus.CONFIRMED)) {
            return saveReservation(roundId, userId, idempotencyKey, ReservationStatus.REJECTED_DUPLICATE);
        }

        throw conflict;
    }

    private ReservationStatus rejectionAfterFailedDecrement(String roundId, String userId) {
        var latestRound = roundRepository.findById(roundId).orElse(null);
        if (latestRound == null) {
            return ReservationStatus.REJECTED_ROUND_NOT_FOUND;
        }
        if (latestRound.getStatus() != RoundStatus.OPEN) {
            return ReservationStatus.REJECTED_ROUND_CLOSED;
        }
        if (reservationRepository.existsByRoundIdAndUserIdAndStatus(
                roundId, userId, ReservationStatus.CONFIRMED)) {
            return ReservationStatus.REJECTED_DUPLICATE;
        }
        return ReservationStatus.REJECTED_SOLD_OUT;
    }

    private Reservation saveReservation(
            String roundId, String userId, String idempotencyKey, ReservationStatus status) {
        var reservation = reservationRepository.save(
                new Reservation(UUID.randomUUID().toString(), idempotencyKey, roundId, userId, status,
                        Instant.now().truncatedTo(ChronoUnit.MICROS)));
        // 같은 키 또는 같은 사용자의 동시 충돌을 이 트랜잭션 안에서 확인한다.
        reservationRepository.flush();
        return reservation;
    }

    private Reservation sameRequestOrThrow(Reservation existing, String roundId, String userId) {
        if (!existing.getRoundId().equals(roundId) || !existing.getUserId().equals(userId)) {
            throw new IdempotencyConflictException();
        }
        return existing;
    }

    private static void validateIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()
                || idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new InvalidIdempotencyKeyException(MAX_IDEMPOTENCY_KEY_LENGTH);
        }
    }

    public Optional<Reservation> findReservation(String requestId) {
        return reservationRepository.findById(requestId);
    }
}
