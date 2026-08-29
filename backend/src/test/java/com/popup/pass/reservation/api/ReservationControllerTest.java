package com.popup.pass.reservation.api;

import com.popup.pass.reservation.service.ReservationService;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@DisplayName("예약 API의 멱등성 계약")
class ReservationControllerTest extends PostgresBackedTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ReservationService reservationService;

    @Test
    @DisplayName("Idempotency-Key가 없으면 요청을 받지 않는다")
    void requiresIdempotencyKey() throws Exception {
        mockMvc.perform(post("/reservation-rounds/round-1/reservations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"user-1\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("같은 키의 API 재시도는 같은 응답 본문을 돌려준다")
    void replaysTheSameResponseForRetry() throws Exception {
        var roundId = "api-idempotency-" + UUID.randomUUID();
        reservationService.createRound(roundId, "API 멱등성 테스트", 2);
        var request = post("/reservation-rounds/{roundId}/reservations", roundId)
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", "api-idempotency-key-1")
                .content("{\"userId\":\"user-1\"}");

        var first = mockMvc.perform(request).andExpect(status().isCreated()).andReturn();
        var retry = mockMvc.perform(request).andExpect(status().isCreated()).andReturn();

        assertThat(retry.getResponse().getContentAsString())
                .isEqualTo(first.getResponse().getContentAsString());
    }
}
