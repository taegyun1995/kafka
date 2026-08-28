package com.popup.pass.study;

import com.popup.pass.study.steps.Step0NoGuard;
import com.popup.pass.study.steps.Step1AtomicDecrement;
import com.popup.pass.study.steps.Step2UniqueIndex;
import com.popup.pass.study.steps.Step3IdempotencyKey;
import com.popup.pass.study.steps.StepWithoutDuplicateRule;
import com.popup.pass.support.PostgresBackedTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.stream.Stream;

import static com.popup.pass.study.StudyRunResult.Verdict.BROKEN;
import static com.popup.pass.study.StudyRunResult.Verdict.OK;
import static com.popup.pass.study.StudyRunResult.Verdict.PARTIAL;
import static com.popup.pass.study.StudyScenario.CONCURRENT_RETRY;
import static com.popup.pass.study.StudyScenario.DOUBLE_BOOKING;
import static com.popup.pass.study.StudyScenario.OVERSELL;
import static com.popup.pass.study.StudyScenario.RETRY;
import static com.popup.pass.study.steps.StudyStep.CONFIRMED;
import static com.popup.pass.study.steps.StudyStep.REJECTED_DUPLICATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * 화면의 버튼이 보여 줄 결과를 그대로 고정한다.
 *
 * <p>아래 표가 이 프로젝트가 하려는 이야기 전부다. 같은 상황을 단계마다 들이대면 결과가 갈린다.
 * BROKEN 칸이 있다는 것은 실패가 아니라 <strong>그 단계가 아직 막지 못하는 것</strong>을 고정해 둔 것이다.
 */
@DisplayName("단계별 보호 장치가 무엇을 막고 무엇을 남기는가")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StudyStepsTest extends PostgresBackedTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private StudyRunner runner;

    @BeforeEach
    void setUp() {
        runner = new StudyRunner(new StudyTables(jdbc), List.of(
                new Step0NoGuard(jdbc),
                new Step1AtomicDecrement(jdbc),
                new Step2UniqueIndex(jdbc, transactionManager),
                new Step3IdempotencyKey(jdbc, transactionManager),
                new StepWithoutDuplicateRule(jdbc)));
    }

    static Stream<Arguments> matrix() {
        return Stream.of(
                arguments("step0", OVERSELL, BROKEN),
                arguments("step0", DOUBLE_BOOKING, BROKEN),
                arguments("step0", RETRY, BROKEN),
                arguments("step0", CONCURRENT_RETRY, BROKEN),

                arguments("step1", OVERSELL, OK),
                arguments("step1", DOUBLE_BOOKING, BROKEN),
                arguments("step1", RETRY, PARTIAL),
                arguments("step1", CONCURRENT_RETRY, BROKEN),

                arguments("step2", OVERSELL, OK),
                arguments("step2", DOUBLE_BOOKING, OK),
                arguments("step2", RETRY, PARTIAL),
                arguments("step2", CONCURRENT_RETRY, PARTIAL),

                arguments("step3", OVERSELL, OK),
                arguments("step3", DOUBLE_BOOKING, OK),
                arguments("step3", RETRY, OK),
                arguments("step3", CONCURRENT_RETRY, OK),

                // 1인 1매 규칙이 사라진 세계. 원자적 차감만으로는 재시도를 막지 못한다.
                arguments("no-duplicate-rule", OVERSELL, OK),
                arguments("no-duplicate-rule", DOUBLE_BOOKING, BROKEN),
                arguments("no-duplicate-rule", RETRY, BROKEN),
                arguments("no-duplicate-rule", CONCURRENT_RETRY, BROKEN));
    }

    @ParameterizedTest(name = "{0} × {1} → {2}")
    @MethodSource("matrix")
    @Order(1)
    @DisplayName("단계 × 상황")
    void verdicts(String stepId, StudyScenario scenario, StudyRunResult.Verdict expected) {
        var result = runner.run(runner.step(stepId).orElseThrow(), scenario);

        report(result);

        assertThat(result.verdict()).isEqualTo(expected);
    }

    @Test
    @Order(2)
    @DisplayName("2단계의 재시도는 자리를 지키지만 확정된 예약을 잃는다")
    void retryLosesTheOriginalResultAtStep2() {
        var result = runner.run(runner.step("step2").orElseThrow(), RETRY);

        var first = result.attempts().get(0);
        var retry = result.attempts().get(1);

        assertThat(first.status()).isEqualTo(CONFIRMED);
        assertThat(retry.status()).isEqualTo(REJECTED_DUPLICATE);
        assertThat(result.seatsTaken()).isEqualTo(1);
        // 클라이언트가 같은 키를 보냈는데도 서버가 읽지 않아 두 시도의 식별자가 갈렸다.
        assertThat(retry.requestId()).isNotEqualTo(first.requestId());
    }

    @Test
    @Order(3)
    @DisplayName("3단계의 재시도는 같은 requestId로 같은 결과를 돌려받는다")
    void retryReturnsTheFirstResultAtStep3() {
        var result = runner.run(runner.step("step3").orElseThrow(), RETRY);

        var first = result.attempts().get(0);
        var retry = result.attempts().get(1);

        assertThat(first.status()).isEqualTo(CONFIRMED);
        assertThat(retry.status()).isEqualTo(CONFIRMED);
        assertThat(retry.requestId()).isEqualTo(first.requestId());
        assertThat(result.seatsTaken()).isEqualTo(1);
        assertThat(result.savedRows()).isEqualTo(1);
    }

    private static void report(StudyRunResult result) {
        var out = new StringBuilder("\n  [%s] %s × %s\n".formatted(
                result.verdict(), result.stepName(), result.scenarioName()));
        out.append("    → ").append(result.headline()).append('\n');
        out.append("      확정 %d · 매진 %d · 중복 %d · 남은 자리 %d/%d · 저장된 행 %d · %dms%n".formatted(
                result.confirmed(), result.rejectedSoldOut(), result.rejectedDuplicate(),
                result.remainingCount(), result.capacity(), result.savedRows(), result.elapsedMillis()));
        for (var note : result.notes()) {
            out.append("      · ").append(note).append('\n');
        }
        System.out.println(out);
    }
}
