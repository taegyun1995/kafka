package com.popup.pass.load;

import com.popup.pass.reservation.service.ReservationService;
import com.popup.pass.study.StudyTables;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 요청이 한꺼번에 몰릴 때 무엇이 먼저 무너지는지 고정한다.
 *
 * <p>{@code com.popup.pass.study}가 "답이 맞는가"를 고정했다면 여기는 "제때 대답하는가"를 고정한다.
 * 결과가 <strong>틀리지 않는다는 것</strong>까지 함께 확인해야 이 실험이 지연만 재고 있다는 것이 분명해진다.
 */
@DisplayName("동기 처리에 요청이 몰릴 때")
class LoadRunnerTest extends PostgresBackedTest {

    /** 커넥션 풀(기본 10)보다 확실히 크게 잡아야 줄 서는 모습이 관측된다. */
    private static final int CROWDED = 100;

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    private LoadRunner runner;

    @BeforeEach
    void 러너를_만든다() {
        runner = new LoadRunner(reservationService, new StudyTables(jdbcTemplate), jdbcTemplate, dataSource);
    }

    @Test
    @DisplayName("몰려 들어와도 요청은 전부 처리된다 — 틀리는 것이 아니라 늦어진다")
    void 요청은_전부_처리된다() {
        var result = runner.run(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(result.succeeded()).isEqualTo(CROWDED);
        assertThat(result.failed()).isZero();
        assertThat(result.failures()).isEmpty();
    }

    @Test
    @DisplayName("정원을 넉넉히 잡아 매진도 중복도 섞이지 않는다 — 남는 변수는 시간뿐이다")
    void 거절이_섞이지_않는다() {
        var result = runner.run(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(result.capacity()).isEqualTo(CROWDED * 2);
        assertThat(confirmedCountOf(result.roundId())).isEqualTo(CROWDED);
    }

    @Test
    @DisplayName("커넥션보다 많이 보내면 나머지는 풀 앞에서 줄을 선다")
    void 풀_앞에_줄이_선다() {
        var result = runner.run(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(result.pool().maxPoolSize()).isPositive();
        assertThat(CROWDED).isGreaterThan(result.pool().maxPoolSize());
        assertThat(result.pool().peakAwaiting()).isPositive();
        assertThat(result.verdict()).isNotEqualTo(LoadRunResult.Verdict.CALM);
    }

    @Test
    @DisplayName("같은 일을 한 요청들의 응답 시간이 갈라진다 — 먼저 잡은 쪽과 기다린 쪽")
    void 지연의_꼬리가_길어진다() {
        var latency = runner.run(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS).latency();

        assertThat(latency.minMicros()).isLessThanOrEqualTo(latency.p50Micros());
        assertThat(latency.p50Micros()).isLessThanOrEqualTo(latency.p95Micros());
        assertThat(latency.p95Micros()).isLessThanOrEqualTo(latency.p99Micros());
        assertThat(latency.p99Micros()).isLessThanOrEqualTo(latency.maxMicros());
        assertThat(latency.tailRatio()).isGreaterThan(1.0);
    }

    @Test
    @DisplayName("기다려 줄 시간이 짧으면 서버가 다 처리해도 실패로 친다")
    void 기다리다_떠났을_요청을_센다() {
        var result = runner.run(CROWDED, 1);

        assertThat(result.failed()).isZero();
        assertThat(result.wouldHaveGivenUp()).isPositive();
        assertThat(result.verdict()).isEqualTo(LoadRunResult.Verdict.FAILING);
        assertThat(result.notes()).anyMatch(note -> note.contains("큐가 필요한 이유"));
    }

    @Test
    @DisplayName("정원이 넉넉해도 순정 코드라 차감은 유실된다 — 매진으로 드러나지 않을 뿐이다")
    void 차감이_유실된다() {
        var ledger = runner.run(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS).ledger();

        assertThat(ledger.confirmed()).isEqualTo(CROWDED);
        assertThat(ledger.seatsTaken()).isLessThan(CROWDED);
        assertThat(ledger.lostDecrements()).isEqualTo(CROWDED - ledger.seatsTaken());
    }

    @Test
    @DisplayName("첫 요청의 느림이 측정에 섞이지 않게 미리 데운다 — 데운 건수는 결과에 남긴다")
    void 본_측정_전에_데운다() {
        var result = runner.run(1, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(result.warmupRequests()).isEqualTo(LoadRunner.WARMUP_REQUESTS);
        // 데우기는 정원이 따로인 회차에서 하므로 본 회차의 자리를 먹지 않는다.
        assertThat(confirmedCountOf(result.roundId())).isEqualTo(1);
    }

    @Test
    @DisplayName("커넥션 수 안에 들어오는 요청은 아무도 기다리지 않는다")
    void 한_건은_기다리지_않는다() {
        var result = runner.run(1, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(result.pool().peakAwaiting()).isZero();
        assertThat(result.verdict()).isEqualTo(LoadRunResult.Verdict.CALM);
    }

    @Test
    @DisplayName("세울 수 없는 동시 요청 수는 실행 전에 거절한다")
    void 범위를_벗어나면_거절한다() {
        assertThatThrownBy(() -> runner.run(0, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> runner.run(LoadRunner.MAX_CONCURRENCY + 1, 1_000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private int confirmedCountOf(String roundId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from reservations where round_id = ? and status = 'CONFIRMED'",
                Integer.class, roundId);
    }
}
