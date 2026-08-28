package com.popup.pass.study;

import com.popup.pass.study.steps.StudyStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.popup.pass.study.steps.StudyStep.CONFIRMED;
import static com.popup.pass.study.steps.StudyStep.REJECTED_DUPLICATE;
import static com.popup.pass.study.steps.StudyStep.REJECTED_SOLD_OUT;

/** 한 단계에 한 상황을 들이대고, 나온 숫자로 판정까지 만든다. */
public class StudyRunner {

    private static final Logger log = LoggerFactory.getLogger(StudyRunner.class);

    private final StudyTables tables;
    private final Map<String, StudyStep> stepsById;
    private final List<StudyStep> orderedSteps;

    public StudyRunner(StudyTables tables, List<StudyStep> steps) {
        this.tables = tables;
        this.orderedSteps = steps.stream().sorted(Comparator.comparingInt(StudyStep::order)).toList();
        this.stepsById = steps.stream().collect(Collectors.toMap(StudyStep::id, Function.identity()));
    }

    public List<StudyStep> steps() {
        return orderedSteps;
    }

    public Optional<StudyStep> step(String stepId) {
        return Optional.ofNullable(stepsById.get(stepId));
    }

    public StudyRunResult run(StudyStep step, StudyScenario scenario) {
        tables.reset(step.needsConfirmedUniqueIndex(), scenario.capacity());

        // 클라이언트가 재시도에 붙이는 키. 이 값을 읽는 단계는 3단계뿐이다.
        var idempotencyKey = scenario.sendsIdempotencyKey() ? UUID.randomUUID().toString() : null;

        long startedAt = System.nanoTime();
        List<StudyStep.Attempt> attempts = scenario.concurrent()
                ? runConcurrently(step, scenario, idempotencyKey)
                : runSequentially(step, scenario, idempotencyKey);
        long elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000;

        var views = new ArrayList<StudyRunResult.AttemptView>();
        for (int i = 0; i < attempts.size(); i++) {
            views.add(new StudyRunResult.AttemptView(
                    i + 1, scenario.userIdFor(i), attempts.get(i).requestId(), attempts.get(i).status()));
        }

        long confirmed = count(attempts, CONFIRMED);
        int remaining = tables.remainingCount();
        var judgement = judge(scenario, attempts, confirmed, remaining);

        log.info("study run step={} scenario={} confirmed={} remaining={} verdict={}",
                step.id(), scenario.id(), confirmed, remaining, judgement.verdict());

        return new StudyRunResult(
                step.id(), step.name(), step.addedGuard(), step.summary(),
                scenario.id(), scenario.title(), scenario.description(),
                scenario.capacity(), remaining, scenario.capacity() - remaining, tables.rowCount(),
                confirmed, count(attempts, REJECTED_SOLD_OUT), count(attempts, REJECTED_DUPLICATE),
                views, elapsedMillis,
                judgement.verdict(), judgement.headline(), judgement.notes());
    }

    private List<StudyStep.Attempt> runSequentially(StudyStep step, StudyScenario scenario, String key) {
        var results = new ArrayList<StudyStep.Attempt>();
        for (int i = 0; i < scenario.attempts(); i++) {
            results.add(step.reserve(key, StudyTables.ROUND_ID, scenario.userIdFor(i)));
        }
        return results;
    }

    /** 모든 스레드를 같은 순간에 출발시켜 실제 경합을 만든다. */
    private List<StudyStep.Attempt> runConcurrently(StudyStep step, StudyScenario scenario, String key) {
        var pool = Executors.newFixedThreadPool(scenario.attempts());
        var start = new CountDownLatch(1);
        try {
            List<Future<StudyStep.Attempt>> futures = new ArrayList<>();
            for (int i = 0; i < scenario.attempts(); i++) {
                var userId = scenario.userIdFor(i);
                Callable<StudyStep.Attempt> task = () -> {
                    start.await();
                    return step.reserve(key, StudyTables.ROUND_ID, userId);
                };
                futures.add(pool.submit(task));
            }
            start.countDown();

            var results = new ArrayList<StudyStep.Attempt>();
            for (var future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            return results;
        } catch (Exception failure) {
            throw new IllegalStateException("동시 실행에 실패했다", failure);
        } finally {
            pool.shutdownNow();
        }
    }

    private Judgement judge(StudyScenario scenario, List<StudyStep.Attempt> attempts, long confirmed, int remaining) {
        var notes = new ArrayList<String>();
        return switch (scenario) {
            case OVERSELL -> {
                if (confirmed > scenario.capacity()) {
                    notes.add("다섯이 같은 값을 읽고 같은 값을 덮어썼다. 남이 깎은 것을 내 UPDATE가 지운다.");
                    if (remaining != scenario.capacity() - confirmed) {
                        notes.add("장부는 %d자리 남았다는데 확정은 %d건이다. 기록과 실제가 어긋난다."
                                .formatted(remaining, confirmed));
                    }
                    yield new Judgement(StudyRunResult.Verdict.BROKEN,
                            "정원 %d인데 %d건이 확정됐다 — %d건을 넘겨 팔았다"
                                    .formatted(scenario.capacity(), confirmed, confirmed - scenario.capacity()),
                            notes);
                }
                notes.add("조건부 UPDATE 한 문장이 읽는 시점과 쓰는 시점을 붙여 놓았다.");
                yield new Judgement(StudyRunResult.Verdict.OK,
                        "정원만큼만 확정됐다 — %d/%d".formatted(confirmed, scenario.capacity()), notes);
            }
            case DOUBLE_BOOKING -> {
                if (confirmed > 1) {
                    notes.add("둘 다 '확정 없음'을 읽고 지나갔다. 조회와 삽입 사이가 비어 있다.");
                    notes.add("정원은 정확히 %d 줄었다. 줄면 안 될 자리였을 뿐이다.".formatted(confirmed));
                    yield new Judgement(StudyRunResult.Verdict.BROKEN,
                            "한 사람이 %d자리를 가져갔다".formatted(confirmed), notes);
                }
                notes.add("위반된 Transaction이 Rollback되며 차감도 함께 되돌아갔다.");
                yield new Judgement(StudyRunResult.Verdict.OK, "한 사람에게 한 자리만 나갔다", notes);
            }
            case RETRY, CONCURRENT_RETRY -> {
                var first = attempts.get(0);
                var retry = attempts.get(1);
                if (CONFIRMED.equals(first.status()) && CONFIRMED.equals(retry.status())
                        && first.requestId().equals(retry.requestId())) {
                    notes.add("재시도가 새로 판단하지 않고 저장된 결과를 그대로 돌려줬다.");
                    notes.add("클라이언트가 만든 키가 \"이 둘은 같은 업무 요청이다\"를 말해 준다.");
                    yield new Judgement(StudyRunResult.Verdict.OK,
                            "재시도가 처음 결과를 그대로 돌려받았다 — 저장된 행은 %d건".formatted(tables.rowCount()), notes);
                }
                if (confirmed > 1) {
                    notes.add("서버가 키를 읽지 않으니 재시도가 완전히 새 요청이 된다.");
                    yield new Judgement(StudyRunResult.Verdict.BROKEN,
                            "한 번의 업무 요청이 %d자리를 먹었다".formatted(confirmed), notes);
                }
                notes.add("자리를 지킨 것은 멱등성 키가 아니라 1인 1매라는 중복 규칙이다.");
                notes.add("거절된 쪽이 쥔 것은 %s 뿐이고, 확정된 예약의 %s 은 되찾을 방법이 없다."
                        .formatted(retry.requestId(), first.requestId()));
                yield new Judgement(StudyRunResult.Verdict.PARTIAL,
                        "자리는 지켰지만 확정된 예약을 되찾을 수 없다", notes);
            }
        };
    }

    private static long count(List<StudyStep.Attempt> attempts, String status) {
        return attempts.stream().filter(attempt -> status.equals(attempt.status())).count();
    }

    private record Judgement(StudyRunResult.Verdict verdict, String headline, List<String> notes) {
    }
}
