package com.popup.pass.reservation.api;

import com.popup.pass.reservation.domain.Reservation;
import com.popup.pass.reservation.domain.ReservationRound;
import com.popup.pass.reservation.service.ReservationService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;

@RestController
@RequestMapping
public class ReservationController {

    private final ReservationService reservationService;

    public ReservationController(ReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @PostMapping("/reservation-rounds")
    public ResponseEntity<RoundResponse> createRound(@Valid @RequestBody CreateRoundRequest request) {
        var round = reservationService.createRound(request.id(), request.name(), request.capacity());
        return ResponseEntity.status(HttpStatus.CREATED).body(RoundResponse.from(round));
    }

    @GetMapping("/reservation-rounds/{roundId}")
    public RoundResponse getRound(@PathVariable String roundId) {
        return reservationService.findRound(roundId)
                .map(RoundResponse::from)
                .orElseThrow(() -> roundNotFound(roundId));
    }

    /** 마감은 설정 연산이라 여러 번 호출해도 결과가 같다. 그래서 200으로 현재 상태를 돌려준다. */
    @PostMapping("/reservation-rounds/{roundId}/close")
    public RoundResponse closeRound(@PathVariable String roundId) {
        return reservationService.closeRound(roundId)
                .map(RoundResponse::from)
                .orElseThrow(() -> roundNotFound(roundId));
    }

    @PostMapping("/reservation-rounds/{roundId}/reservations")
    public ResponseEntity<ReservationResponse> requestReservation(
            @PathVariable String roundId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody CreateReservationRequest request) {
        var reservation = reservationService.reserve(roundId, request.userId(), idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(ReservationResponse.from(reservation));
    }

    @GetMapping("/reservations/{requestId}")
    public ReservationResponse getReservation(@PathVariable String requestId) {
        var reservation = reservationService.findReservation(requestId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "reservation is still pending or unknown"));
        return ReservationResponse.from(reservation);
    }

    private static ResponseStatusException roundNotFound(String roundId) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "reservation round not found: " + roundId);
    }

    public record CreateRoundRequest(
            @NotBlank String id,
            @NotBlank String name,
            @Min(1) int capacity) {
    }

    public record CreateReservationRequest(@NotBlank String userId) {
    }

    public record RoundResponse(String id, String name, int capacity, int remainingCount, String status) {
        static RoundResponse from(ReservationRound round) {
            return new RoundResponse(round.getId(), round.getName(), round.getCapacity(), round.getRemainingCount(), round.getStatus().name());
        }
    }

    public record ReservationResponse(
            String requestId,
            String roundId,
            String userId,
            String status,
            Instant createdAt) {
        static ReservationResponse from(Reservation reservation) {
            return new ReservationResponse(
                    reservation.getRequestId(),
                    reservation.getRoundId(),
                    reservation.getUserId(),
                    reservation.getStatus().name(),
                    reservation.getCreatedAt());
        }
    }
}
