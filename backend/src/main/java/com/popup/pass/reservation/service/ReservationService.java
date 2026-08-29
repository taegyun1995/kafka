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
 * <p>정원 차감과 1인 1매를 위한 동시성 장치는 아직 없다. 서로 다른 요청이 겹쳐 들어오면
 * 정원을 넘겨 팔거나 한 사람이 두 자리를 가져갈 수 있다. 같은 업무 요청의 재시도는
 * {@code Idempotency-Key}로 이미 처리한 결과를 돌려준다.
 * 무엇이 어떻게 깨지는지는 {@code com.popup.pass.study}에서 단계별로 확인한다.
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
            // 같은 키를 가진 요청이 동시에 먼저 저장한 경우다. 첫 트랜잭션의 결과를 읽어 반환한다.
            return reservationRepository.findByIdempotencyKey(idempotencyKey)
                    .map(existing -> sameRequestOrThrow(existing, roundId, userId))
                    .orElseThrow(() -> conflict);
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
        } else if (round.getRemainingCount() <= 0) {
            status = ReservationStatus.REJECTED_SOLD_OUT;
        } else {
            round.takeOne();
            roundRepository.save(round);
            status = ReservationStatus.CONFIRMED;
        }

        var reservation = reservationRepository.save(
                new Reservation(UUID.randomUUID().toString(), idempotencyKey, roundId, userId, status,
                        Instant.now().truncatedTo(ChronoUnit.MICROS)));
        reservationRepository.flush();
        log.info("reservation handled requestId={} roundId={} userId={} status={}",
                reservation.getRequestId(), roundId, userId, status);
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
