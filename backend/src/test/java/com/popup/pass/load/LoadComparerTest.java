package com.popup.pass.load;

import com.popup.pass.reservation.service.ReservationService;
import com.popup.pass.study.StudyTables;
import com.popup.pass.study.steps.Step0NoGuard;
import com.popup.pass.study.steps.Step3IdempotencyKey;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 보호 장치를 다 켜면 무엇이 고쳐지고 무엇이 남는지 고정한다.
 *
 * <p>이 프로젝트가 Kafka로 넘어가는 근거가 정확히 여기다 —
 * <strong>정합성은 DB 제약으로 끝나지만 응답 지연은 그 뒤에도 남는다.</strong>
 */
@DisplayName("보호 장치를 다 켜면 빨라지는가")
class LoadComparerTest extends PostgresBackedTest {

    /** 커넥션 풀(기본 10)보다 확실히 커야 경합도, 줄 서기도 생긴다. */
    private static final int CROWDED = 100;

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource;

    private LoadComparer comparer;

    @BeforeEach
    void 비교기를_만든다() {
        var runner = new LoadRunner(
                reservationService, new StudyTables(jdbcTemplate), jdbcTemplate, dataSource);
        // 경합 창 연출을 켜 두면 재는 것이 코드가 아니라 sleep이 된다.
        comparer = new LoadComparer(runner,
                new Step0NoGuard(jdbcTemplate, 0),
                new Step3IdempotencyKey(jdbcTemplate, transactionManager, 0));
    }

    @Test
    @DisplayName("장부는 고쳐진다 — 차감 유실이 사라진다")
    void 정합성은_고쳐진다() {
        var comparison = comparer.compare(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(comparison.unguarded().ledger().lostDecrements()).isPositive();
        assertThat(comparison.guarded().ledger().lostDecrements()).isZero();
        assertThat(comparison.guarded().ledger().seatsTaken())
                .isEqualTo((int) comparison.guarded().ledger().confirmed());
    }

    @Test
    @DisplayName("줄은 그대로 선다 — 병목은 판단 로직이 아니라 동기 처리 자체다")
    void 줄서기는_그대로다() {
        var comparison = comparer.compare(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(comparison.unguarded().pool().peakAwaiting()).isPositive();
        assertThat(comparison.guarded().pool().peakAwaiting()).isPositive();
    }

    @Test
    @DisplayName("두 실행은 같은 조건이어야 비교가 성립한다")
    void 같은_조건으로_잰다() {
        var comparison = comparer.compare(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(comparison.unguarded().concurrency()).isEqualTo(comparison.guarded().concurrency());
        assertThat(comparison.unguarded().capacity()).isEqualTo(comparison.guarded().capacity());
        assertThat(comparison.unguarded().pool().maxPoolSize())
                .isEqualTo(comparison.guarded().pool().maxPoolSize());
        assertThat(comparison.unguarded().targetId()).isEqualTo("step0");
        assertThat(comparison.guarded().targetId()).isEqualTo("step3");
    }

    @Test
    @DisplayName("판정과 설명은 방금 잰 숫자에서 만든다")
    void 판정을_숫자에서_만든다() {
        var comparison = comparer.compare(CROWDED, LoadRunner.DEFAULT_CLIENT_PATIENCE_MILLIS);

        assertThat(comparison.verdict()).isNotEqualTo(LoadComparison.Verdict.INCONCLUSIVE);
        assertThat(comparison.headline()).isNotBlank();
        assertThat(comparison.notes()).isNotEmpty();
        assertThat(comparison.notes()).anyMatch(note -> note.contains("차감 유실이"));
    }
}
