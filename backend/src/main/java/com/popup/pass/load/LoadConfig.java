package com.popup.pass.load;

import com.popup.pass.reservation.service.ReservationService;
import com.popup.pass.study.StudyTables;
import com.popup.pass.study.steps.Step0NoGuard;
import com.popup.pass.study.steps.Step3IdempotencyKey;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * 부하 관측도 {@code study} 프로필에서만 올라온다. 평소 실행에는 없는 창구다.
 *
 * <pre>gradle bootRun --args='--spring.profiles.active=study'</pre>
 */
@Configuration
@Profile("study")
public class LoadConfig {

    /** 경합 창을 벌리는 것은 학습용 연출이다. 응답 시간을 재는 자리에서는 꺼야 한다. */
    private static final long NO_RACE_WINDOW = 0;

    @Bean
    LoadRunner loadRunner(ReservationService reservations, StudyTables studyTables,
                          JdbcTemplate jdbc, DataSource dataSource) {
        return new LoadRunner(reservations, studyTables, jdbc, dataSource);
    }

    /**
     * 비교용 단계는 <strong>Bean으로 올리지 않는다.</strong>
     * {@code List<StudyStep>}으로 주입되는 {@link com.popup.pass.study.StudyRunner}에 섞이면
     * 보호 장치 매트릭스에 같은 단계가 두 줄로 보이기 때문이다.
     */
    @Bean
    LoadComparer loadComparer(LoadRunner runner, JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        return new LoadComparer(runner,
                new Step0NoGuard(jdbc, NO_RACE_WINDOW),
                new Step3IdempotencyKey(jdbc, transactionManager, NO_RACE_WINDOW));
    }
}
