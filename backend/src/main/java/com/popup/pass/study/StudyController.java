package com.popup.pass.study;

import com.popup.pass.study.steps.StudyStep;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.List;

/**
 * 단계별 보호 장치를 브라우저에서 눌러 볼 수 있게 연다.
 *
 * <p>{@code study} 프로필에서만 올라오는 학습용 창구다. 실제 예약 API와 데이터가 겹치지 않는다.
 * CORS는 {@link StudyCorsConfig}가 같은 프로필에서 함께 연다.
 */
@RestController
@RequestMapping("/study")
@Profile("study")
public class StudyController {

    private final StudyRunner runner;

    public StudyController(StudyRunner runner) {
        this.runner = runner;
    }

    @GetMapping
    public Catalog catalog() {
        return new Catalog(steps(), scenarios());
    }

    @GetMapping("/steps")
    public List<StepView> steps() {
        return runner.steps().stream().map(StepView::from).toList();
    }

    @GetMapping("/scenarios")
    public List<ScenarioView> scenarios() {
        return Arrays.stream(StudyScenario.values()).map(ScenarioView::from).toList();
    }

    @PostMapping("/steps/{stepId}/scenarios/{scenarioId}")
    public StudyRunResult run(@PathVariable String stepId, @PathVariable String scenarioId) {
        var step = runner.step(stepId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown step: " + stepId));
        var scenario = StudyScenario.byId(scenarioId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "unknown scenario: " + scenarioId));
        return runner.run(step, scenario);
    }

    public record Catalog(List<StepView> steps, List<ScenarioView> scenarios) {
    }

    public record StepView(String id, int order, String name, String addedGuard, String summary,
                           boolean confirmedUniqueIndex) {
        static StepView from(StudyStep step) {
            return new StepView(step.id(), step.order(), step.name(), step.addedGuard(), step.summary(),
                    step.needsConfirmedUniqueIndex());
        }
    }

    public record ScenarioView(String id, String name, String description, int capacity, int attempts,
                               boolean sameUser, boolean concurrent, boolean sendsIdempotencyKey) {
        static ScenarioView from(StudyScenario scenario) {
            return new ScenarioView(scenario.id(), scenario.title(), scenario.description(),
                    scenario.capacity(), scenario.attempts(), scenario.sameUser(), scenario.concurrent(),
                    scenario.sendsIdempotencyKey());
        }
    }
}
