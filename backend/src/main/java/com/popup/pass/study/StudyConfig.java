package com.popup.pass.study;

import com.popup.pass.study.steps.Step0NoGuard;
import com.popup.pass.study.steps.Step1AtomicDecrement;
import com.popup.pass.study.steps.Step2UniqueIndex;
import com.popup.pass.study.steps.Step3IdempotencyKey;
import com.popup.pass.study.steps.StepWithoutDuplicateRule;
import com.popup.pass.study.steps.StudyStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

/**
 * 학습용 단계들은 {@code study} 프로필에서만 올라온다.
 * 평소 실행에는 없는 코드이므로 실제 예약 API와 섞이지 않는다.
 *
 * <pre>gradle bootRun --args='--spring.profiles.active=study'</pre>
 *
 * <p>단계 구현에는 Spring 애너테이션을 붙이지 않았다. 표본으로 읽어야 하는 코드라
 * 보호 장치 외의 것이 눈에 들어오지 않게 두었고, 조립은 여기서만 한다.
 */
@Configuration
@Profile("study")
public class StudyConfig {

    @Bean
    StudyTables studyTables(JdbcTemplate jdbc) {
        return new StudyTables(jdbc);
    }

    @Bean
    Step0NoGuard step0NoGuard(JdbcTemplate jdbc) {
        return new Step0NoGuard(jdbc);
    }

    @Bean
    Step1AtomicDecrement step1AtomicDecrement(JdbcTemplate jdbc) {
        return new Step1AtomicDecrement(jdbc);
    }

    @Bean
    Step2UniqueIndex step2UniqueIndex(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        return new Step2UniqueIndex(jdbc, transactionManager);
    }

    @Bean
    Step3IdempotencyKey step3IdempotencyKey(JdbcTemplate jdbc, PlatformTransactionManager transactionManager) {
        return new Step3IdempotencyKey(jdbc, transactionManager);
    }

    @Bean
    StepWithoutDuplicateRule stepWithoutDuplicateRule(JdbcTemplate jdbc) {
        return new StepWithoutDuplicateRule(jdbc);
    }

    @Bean
    StudyRunner studyRunner(StudyTables studyTables, List<StudyStep> steps) {
        return new StudyRunner(studyTables, steps);
    }
}
