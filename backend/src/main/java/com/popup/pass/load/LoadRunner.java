package com.popup.pass.load;

import com.popup.pass.reservation.service.ReservationService;
import com.popup.pass.study.StudyTables;
import com.popup.pass.study.steps.StudyStep;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 예약 처리에 요청을 한꺼번에 밀어 넣고, <strong>얼마나 걸렸는지</strong>를 잰다.
 *
 * <p>{@code com.popup.pass.study}가 "결과가 틀리는가"를 물었다면 여기는 "제때 대답하는가"를 묻는다.
 * 두 질문의 답이 다르기 때문에 처방도 다르다 — 앞의 것은 DB 제약으로 고치지만,
 * 뒤의 것은 아무리 정확해도 커넥션 하나가 한 번에 한 건씩만 처리한다는 사실이 남는다.
 *
 * <p>고르는 것은 동시 요청 수 하나다. 정원은 넉넉히 잡아 매진이 섞이지 않게 하고,
 * 사용자도 전원 다르게 두어 중복 거절이 섞이지 않게 한다. 남는 변수는 시간뿐이다.
 *
 * <p>부하는 두 곳에 걸 수 있다 — 순정 프로덕션 경로({@link ReservationService})와, 보호 장치를 켠 학습용 단계.
 * 후자가 있어야 <em>"다 고치면 빨라지는가"</em>를 주장이 아니라 관측으로 답할 수 있다.
 */
public class LoadRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadRunner.class);

    /** 로컬 한 대에서 스레드를 세우는 실험이라 위쪽을 막아 둔다. */
    public static final int MAX_CONCURRENCY = 600;

    /** 사람이 결과 화면을 기다려 주는 시간. 이 선을 넘긴 요청은 "이미 떠났을 요청"으로 센다. */
    public static final int DEFAULT_CLIENT_PATIENCE_MILLIS = 1_000;

    /**
     * 첫 요청은 클래스 로딩과 JIT 때문에 유독 느리다. 그대로 두면 "동시 1건"이 가장 느린 지점으로 찍혀
     * 지연 곡선이 뒤집힌다. 본 측정과 섞이지 않게 미리 데우되, 몇 건을 데웠는지는 결과에 남긴다.
     */
    public static final int WARMUP_REQUESTS = 10;

    private static final long SAMPLE_INTERVAL_MILLIS = 2;

    private final ReservationService reservations;
    private final StudyTables studyTables;
    private final JdbcTemplate jdbc;
    private final DataSource dataSource;

    public LoadRunner(ReservationService reservations, StudyTables studyTables,
                      JdbcTemplate jdbc, DataSource dataSource) {
        this.reservations = reservations;
        this.studyTables = studyTables;
        this.jdbc = jdbc;
        this.dataSource = dataSource;
    }

    public int maxPoolSize() {
        return dataSource instanceof HikariDataSource hikari ? hikari.getMaximumPoolSize() : -1;
    }

    /** 순정 프로덕션 경로에 부하를 건다. */
    public LoadRunResult run(int concurrency, int clientPatienceMillis) {
        return runOn(new ProductionTarget(), concurrency, clientPatienceMillis);
    }

    /** 보호 장치를 켠 학습용 단계에 같은 부하를 건다. */
    public LoadRunResult runOnStep(StudyStep step, int concurrency, int clientPatienceMillis) {
        return runOn(new StepTarget(step), concurrency, clientPatienceMillis);
    }

    private LoadRunResult runOn(Target target, int concurrency, int clientPatienceMillis) {
        if (concurrency < 1 || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException("동시 요청 수는 1..%d 사이여야 한다: %d".formatted(MAX_CONCURRENCY, concurrency));
        }

        // 정원은 요청 수보다 넉넉히 둔다. 매진 거절이 섞이면 재는 것이 지연인지 거절인지 흐려진다.
        int capacity = concurrency * 2;
        clearPreviousRuns();
        target.warmUp();
        String roundId = target.prepare(capacity);

        var sampler = new PoolSampler(poolMBean());
        var samplerThread = new Thread(sampler, "load-pool-sampler");
        samplerThread.setDaemon(true);

        var pool = Executors.newFixedThreadPool(concurrency);
        var start = new CountDownLatch(1);
        var ready = new CountDownLatch(concurrency);
        try {
            List<Future<Outcome>> futures = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                String userId = "load-user-" + i;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    long began = System.nanoTime();
                    try {
                        target.call(roundId, userId);
                        return new Outcome(System.nanoTime() - began, null);
                    } catch (Exception failure) {
                        return new Outcome(System.nanoTime() - began, reasonOf(failure));
                    }
                }));
            }

            // 스레드가 다 자리를 잡은 뒤에 출발시켜야 "동시에 들이닥친" 모양이 된다.
            ready.await(30, TimeUnit.SECONDS);
            samplerThread.start();
            long startedAt = System.nanoTime();
            start.countDown();

            var outcomes = new ArrayList<Outcome>();
            for (var future : futures) {
                outcomes.add(future.get(120, TimeUnit.SECONDS));
            }
            long elapsedMillis = Math.max(1, (System.nanoTime() - startedAt) / 1_000_000);
            sampler.stop();

            return summarise(target, concurrency, capacity, roundId, clientPatienceMillis,
                    elapsedMillis, outcomes, sampler);
        } catch (Exception failure) {
            throw new IllegalStateException("부하 실행에 실패했다", failure);
        } finally {
            sampler.stop();
            pool.shutdownNow();
        }
    }

    /** 실험 데이터는 실험끼리만 쌓이게 둔다. 데모 페이지에서 만든 회차는 건드리지 않는다. */
    private void clearPreviousRuns() {
        jdbc.update("delete from reservations where round_id like 'load-%'");
        jdbc.update("delete from reservation_rounds where id like 'load-%'");
    }

    private LoadRunResult summarise(Target target, int concurrency, int capacity, String roundId,
                                    int clientPatienceMillis, long elapsedMillis,
                                    List<Outcome> outcomes, PoolSampler sampler) {
        var sortedMicros = outcomes.stream().mapToLong(outcome -> outcome.nanos() / 1_000).sorted().toArray();
        var latency = new LoadRunResult.Latency(
                sortedMicros[0],
                percentile(sortedMicros, 50),
                percentile(sortedMicros, 95),
                percentile(sortedMicros, 99),
                sortedMicros[sortedMicros.length - 1]);

        int failed = (int) outcomes.stream().filter(outcome -> outcome.reason() != null).count();
        int succeeded = outcomes.size() - failed;
        long patienceMicros = clientPatienceMillis * 1_000L;
        int gaveUp = (int) outcomes.stream().filter(outcome -> outcome.nanos() / 1_000 > patienceMicros).count();
        double throughput = outcomes.size() * 1_000.0 / elapsedMillis;

        var failures = new ArrayList<LoadRunResult.FailureView>();
        countByReason(outcomes).forEach((reason, count) -> failures.add(new LoadRunResult.FailureView(reason, count)));
        failures.sort(Comparator.comparingInt(LoadRunResult.FailureView::count).reversed());

        var poolView = new LoadRunResult.Pool(
                maxPoolSize(), sampler.peakActive(), sampler.peakAwaiting(), sampler.samples());
        var ledger = target.ledger(roundId, capacity);
        var judgement = judge(concurrency, clientPatienceMillis, gaveUp, failed, throughput, latency, poolView, ledger);

        log.info("load run target={} concurrency={} elapsedMs={} throughput={} p95Micros={} peakAwaiting={} lost={} verdict={}",
                target.id(), concurrency, elapsedMillis, Math.round(throughput), latency.p95Micros(),
                poolView.peakAwaiting(), ledger.lostDecrements(), judgement.verdict());

        return new LoadRunResult(target.id(), target.label(), target.addedGuard(),
                concurrency, capacity, roundId, elapsedMillis, throughput,
                succeeded, failed, clientPatienceMillis, WARMUP_REQUESTS, ledger, gaveUp, failures, latency, poolView,
                judgement.verdict(), judgement.headline(), judgement.notes());
    }

    private Judgement judge(int concurrency, int patienceMillis, int gaveUp, int failed, double throughput,
                            LoadRunResult.Latency latency, LoadRunResult.Pool pool, LoadRunResult.Ledger ledger) {
        var notes = new ArrayList<String>();
        long p95Millis = latency.p95Micros() / 1_000;

        if (pool.peakAwaiting() > 0) {
            notes.add("커넥션은 %d개인데 요청은 %d건이었다. 최대 %d건이 풀 앞에서 자기 차례를 기다렸다."
                    .formatted(pool.maxPoolSize(), concurrency, pool.peakAwaiting()));
        } else if (pool.maxPoolSize() > 0 && concurrency <= pool.maxPoolSize()) {
            notes.add("동시 요청 %d건이 커넥션 %d개 안에 들어왔다. 아무도 기다리지 않았다."
                    .formatted(concurrency, pool.maxPoolSize()));
        }
        notes.add("가장 빠른 응답 %s, 가장 느린 응답 %s — %.0f배 차이다. 같은 코드가 같은 일을 했는데도 그렇다."
                .formatted(millis(latency.minMicros()), millis(latency.maxMicros()), latency.tailRatio()));
        notes.add("처리량은 초당 %.0f건이다. 동시 요청을 늘려도 이 값은 커넥션 수에 갇힌다 — 늘어나는 것은 대기 시간뿐이다."
                .formatted(throughput));
        if (ledger.lostDecrements() > 0) {
            notes.add("한편 확정은 %d건인데 정원에서 빠진 자리는 %d칸뿐이다 — 차감 %d칸이 증발했다. "
                    .formatted(ledger.confirmed(), ledger.seatsTaken(), ledger.lostDecrements())
                    + "보호 장치가 없으니 Lost Update는 여기서도 일어난다. 정원을 넉넉히 잡아 매진으로 드러나지 않았을 뿐이다.");
            notes.add("이 어긋남은 조건부 UPDATE 한 줄로 고쳐진다. 위의 지연은 그 뒤에도 남는다 — 그래서 처방이 다르다.");
        } else {
            notes.add("확정 %d건에 빠진 자리도 정확히 %d칸이다. 장부가 맞는다."
                    .formatted(ledger.confirmed(), ledger.seatsTaken()));
        }

        if (failed > 0) {
            notes.add("먼저 무너지는 것은 정확성이 아니라 응답이다. 계산은 맞았지만 응답이 오지 않았다.");
            return new Judgement(LoadRunResult.Verdict.FAILING,
                    "동시 %d건 중 %d건이 예외로 끝났다".formatted(concurrency, failed), notes);
        }
        if (gaveUp > 0) {
            notes.add("서버는 %d건을 모두 처리했다. 문제는 그 사이 사람이 기다리고 있었다는 것이다."
                    .formatted(concurrency));
            notes.add("접수와 처리를 떼어 놓으면 이 대기는 응답 시간에서 사라진다. 큐가 필요한 이유가 여기다.");
            return new Judgement(LoadRunResult.Verdict.FAILING,
                    "%dms 안에 답을 못 받은 요청이 %d건 — p95가 %dms다".formatted(patienceMillis, gaveUp, p95Millis),
                    notes);
        }
        if (pool.peakAwaiting() > 0) {
            notes.add("아직은 다 참아 줄 만한 시간이다. 동시 요청을 더 올리면 이 대기가 그대로 응답 시간이 된다.");
            return new Judgement(LoadRunResult.Verdict.QUEUEING,
                    "전부 %dms 안에 응답했지만 최대 %d건이 줄을 섰다".formatted(patienceMillis, pool.peakAwaiting()),
                    notes);
        }
        return new Judgement(LoadRunResult.Verdict.CALM,
                "동시 %d건을 p95 %s에 처리했다".formatted(concurrency, millis(latency.p95Micros())), notes);
    }

    private static String millis(long micros) {
        return micros < 1_000 ? "%.2fms".formatted(micros / 1_000.0) : "%,dms".formatted(micros / 1_000);
    }

    /** 정렬된 배열에서 가장 가까운 순위를 고른다. 표본이 작아도 답이 나와야 한다. */
    private static long percentile(long[] sortedMicros, int percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedMicros.length) - 1;
        return sortedMicros[Math.max(0, Math.min(index, sortedMicros.length - 1))];
    }

    private static Map<String, Integer> countByReason(List<Outcome> outcomes) {
        var counts = new LinkedHashMap<String, Integer>();
        for (var outcome : outcomes) {
            if (outcome.reason() != null) {
                counts.merge(outcome.reason(), 1, Integer::sum);
            }
        }
        return counts;
    }

    /** 스택 트레이스 대신 "무엇 때문에"만 남긴다. 화면에서 세어 볼 수 있어야 하기 때문이다. */
    private static String reasonOf(Throwable failure) {
        var root = failure;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = String.valueOf(root.getMessage());
        if (message.contains("request timed out")) {
            return "커넥션 풀 대기 시간 초과";
        }
        return root.getClass().getSimpleName();
    }

    private HikariPoolMXBean poolMBean() {
        return dataSource instanceof HikariDataSource hikari ? hikari.getHikariPoolMXBean() : null;
    }

    /** 부하를 어디에 거는지. 준비·호출·장부 읽기만 다르고 재는 방법은 같아야 비교가 성립한다. */
    private interface Target {
        String id();

        String label();

        String addedGuard();

        void warmUp();

        /** 회차를 준비하고 그 id를 돌려준다. */
        String prepare(int capacity);

        void call(String roundId, String userId);

        LoadRunResult.Ledger ledger(String roundId, int capacity);
    }

    /** 순정 프로덕션 경로. 지금 {@code ReservationService}에는 보호 장치가 하나도 없다. */
    private final class ProductionTarget implements Target {

        @Override
        public String id() {
            return "plain";
        }

        @Override
        public String label() {
            return "순정 · 프로덕션 경로";
        }

        @Override
        public String addedGuard() {
            return "없음";
        }

        @Override
        public void warmUp() {
            // 워밍업은 정원이 따로인 회차에서 한다. 본 회차의 자리를 먹으면 매진이 섞인다.
            String warmupRound = "load-warmup-" + System.currentTimeMillis();
            reservations.createRound(warmupRound, "부하 관측 워밍업", WARMUP_REQUESTS);
            for (int i = 0; i < WARMUP_REQUESTS; i++) {
                reservations.reserve(warmupRound, "warmup-user-" + i);
            }
        }

        @Override
        public String prepare(int capacity) {
            String roundId = "load-" + System.currentTimeMillis();
            reservations.createRound(roundId, "부하 관측 회차", capacity);
            return roundId;
        }

        @Override
        public void call(String roundId, String userId) {
            reservations.reserve(roundId, userId);
        }

        @Override
        public LoadRunResult.Ledger ledger(String roundId, int capacity) {
            Integer remaining = jdbc.queryForObject(
                    "select remaining_count from reservation_rounds where id = ?", Integer.class, roundId);
            Long confirmed = jdbc.queryForObject(
                    "select count(*) from reservations where round_id = ? and status = 'CONFIRMED'",
                    Long.class, roundId);
            return toLedger(capacity, remaining, confirmed);
        }
    }

    /** 보호 장치를 켠 학습용 단계. 경합 창 연출은 꺼 둔 인스턴스를 받는다. */
    private final class StepTarget implements Target {

        private final StudyStep step;

        private StepTarget(StudyStep step) {
            this.step = step;
        }

        @Override
        public String id() {
            return step.id();
        }

        @Override
        public String label() {
            return step.name();
        }

        @Override
        public String addedGuard() {
            return step.addedGuard();
        }

        @Override
        public void warmUp() {
            studyTables.reset(step.needsConfirmedUniqueIndex(), WARMUP_REQUESTS);
            for (int i = 0; i < WARMUP_REQUESTS; i++) {
                step.reserve(null, StudyTables.ROUND_ID, "warmup-user-" + i);
            }
        }

        @Override
        public String prepare(int capacity) {
            // 데운 자국을 지우고 본 회차를 새로 깐다.
            studyTables.reset(step.needsConfirmedUniqueIndex(), capacity);
            return StudyTables.ROUND_ID;
        }

        @Override
        public void call(String roundId, String userId) {
            // 부하에서는 재시도가 아니라 서로 다른 요청이므로 키도 매번 새로 만든다.
            step.reserve(UUID.randomUUID().toString(), roundId, userId);
        }

        @Override
        public LoadRunResult.Ledger ledger(String roundId, int capacity) {
            Integer remaining = jdbc.queryForObject(
                    "select remaining_count from study_rounds where id = ?", Integer.class, roundId);
            Long confirmed = jdbc.queryForObject(
                    "select count(*) from study_reservations where round_id = ? and status = 'CONFIRMED'",
                    Long.class, roundId);
            return toLedger(capacity, remaining, confirmed);
        }
    }

    private static LoadRunResult.Ledger toLedger(int capacity, Integer remaining, Long confirmed) {
        int seatsTaken = capacity - (remaining == null ? capacity : remaining);
        long confirmedCount = confirmed == null ? 0 : confirmed;
        return new LoadRunResult.Ledger(confirmedCount, seatsTaken, (int) (confirmedCount - seatsTaken));
    }

    private record Outcome(long nanos, String reason) {
    }

    private record Judgement(LoadRunResult.Verdict verdict, String headline, List<String> notes) {
    }

    /** 실행 중에 풀을 계속 들여다본다. 끝난 뒤에 물어보면 이미 다 반납된 뒤라 아무것도 안 보인다. */
    private static final class PoolSampler implements Runnable {

        private final HikariPoolMXBean pool;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private volatile int peakActive;
        private volatile int peakAwaiting;
        private volatile int samples;

        private PoolSampler(HikariPoolMXBean pool) {
            this.pool = pool;
        }

        @Override
        public void run() {
            if (pool == null) {
                return;
            }
            while (running.get()) {
                peakActive = Math.max(peakActive, pool.getActiveConnections());
                peakAwaiting = Math.max(peakAwaiting, pool.getThreadsAwaitingConnection());
                samples++;
                try {
                    TimeUnit.MILLISECONDS.sleep(SAMPLE_INTERVAL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        void stop() {
            running.set(false);
        }

        int peakActive() {
            return peakActive;
        }

        int peakAwaiting() {
            return peakAwaiting;
        }

        int samples() {
            return samples;
        }
    }
}
