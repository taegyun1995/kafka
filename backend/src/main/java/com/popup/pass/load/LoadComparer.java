package com.popup.pass.load;

import com.popup.pass.study.steps.StudyStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * 같은 부하를 두 코드에 차례로 걸어 비교한다.
 *
 * <p>두 실행은 <strong>같은 DB, 같은 커넥션 풀, 같은 재는 방법</strong>을 쓴다.
 * 다른 것은 보호 장치뿐이어야 비교가 성립하므로, 경합 창을 벌리는 학습용 연출({@code widenRaceWindow})은
 * 두 단계 모두 꺼 둔 인스턴스를 받는다. 켠 채로 재면 재는 것이 코드가 아니라 그 {@code sleep}이 된다.
 */
public class LoadComparer {

    private static final Logger log = LoggerFactory.getLogger(LoadComparer.class);

    /** 지연이 "그대로"인지 판단하는 선. 이보다 덜 줄었으면 줄었다고 하지 않는다. */
    private static final double MEANINGFUL_DROP = 0.9;

    private final LoadRunner runner;
    private final StudyStep unguarded;
    private final StudyStep guarded;

    public LoadComparer(LoadRunner runner, StudyStep unguarded, StudyStep guarded) {
        this.runner = runner;
        this.unguarded = unguarded;
        this.guarded = guarded;
    }

    public LoadComparison compare(int concurrency, int clientPatienceMillis) {
        var before = runner.runOnStep(unguarded, concurrency, clientPatienceMillis);
        var after = runner.runOnStep(guarded, concurrency, clientPatienceMillis);

        var notes = new ArrayList<String>();
        long beforeP95 = before.latency().p95Micros();
        long afterP95 = after.latency().p95Micros();
        int lostBefore = before.ledger().lostDecrements();
        int lostAfter = after.ledger().lostDecrements();
        boolean consistencyFixed = lostBefore > 0 && lostAfter == 0;
        boolean latencyDropped = afterP95 < beforeP95 * MEANINGFUL_DROP;

        if (consistencyFixed) {
            notes.add("차감 유실이 %d칸에서 0칸으로 사라졌다. 조건부 UPDATE와 부분 Unique Index가 정확히 그것을 고쳤다."
                    .formatted(lostBefore));
        } else if (lostBefore == 0) {
            notes.add("보호 없는 쪽에서도 차감이 유실되지 않았다. 동시 요청이 적어 경합이 안 생긴 것이니 수를 올려 본다.");
        } else {
            notes.add("보호를 켠 쪽에서도 %d칸이 유실됐다. 이 단계가 막는 것이 무엇인지 다시 봐야 한다.".formatted(lostAfter));
        }

        notes.add("p95 응답 지연은 %s에서 %s가 됐다 — %s.".formatted(
                millis(beforeP95), millis(afterP95), describe(beforeP95, afterP95)));
        notes.add("풀 앞에 선 줄도 %d건에서 %d건이다. 병목은 판단 로직이 아니라 한 요청이 커넥션을 붙잡고 있는 것 자체다."
                .formatted(before.pool().peakAwaiting(), after.pool().peakAwaiting()));

        Verdictish judgement = judge(consistencyFixed, latencyDropped, beforeP95, afterP95, lostBefore, notes);

        log.info("load compare concurrency={} lostBefore={} lostAfter={} p95Before={} p95After={} verdict={}",
                concurrency, lostBefore, lostAfter, beforeP95, afterP95, judgement.verdict());

        return new LoadComparison(concurrency, clientPatienceMillis, before, after,
                judgement.verdict(), judgement.headline(), notes);
    }

    private Verdictish judge(boolean consistencyFixed, boolean latencyDropped,
                             long beforeP95, long afterP95, int lostBefore, ArrayList<String> notes) {
        if (!consistencyFixed) {
            return new Verdictish(LoadComparison.Verdict.INCONCLUSIVE,
                    lostBefore == 0
                            ? "이번 실행에서는 경합이 생기지 않았다 — 동시 요청 수를 올려 본다"
                            : "보호를 켠 쪽에서도 차감이 유실됐다");
        }
        if (latencyDropped) {
            notes.add("이번 실행에서는 응답도 빨라졌다. 여러 번 눌러 보고 계속 그런지 확인한다 — 절대값은 실행마다 흔들린다.");
            return new Verdictish(LoadComparison.Verdict.BOTH_IMPROVED,
                    "장부는 맞았고 응답도 %s → %s로 빨라졌다".formatted(millis(beforeP95), millis(afterP95)));
        }
        notes.add("보호 장치는 결과를 지키는 물건이지 빨리 대답하게 하는 물건이 아니다. "
                + "검사를 하나 더 하므로 오히려 조금 더 걸린다.");
        notes.add("여기서 남은 지연을 없애려면 접수와 처리를 떼어 놓아야 한다. 큐로 넘어가는 근거가 이 두 열의 차이다.");
        return new Verdictish(LoadComparison.Verdict.LATENCY_REMAINS,
                "장부는 맞았는데 응답은 %s → %s로 그대로다".formatted(millis(beforeP95), millis(afterP95)));
    }

    private static String describe(long before, long after) {
        if (after < before * MEANINGFUL_DROP) {
            return "%.0f%% 줄었다".formatted((1 - (double) after / before) * 100);
        }
        if (after > before * 1.1) {
            return "오히려 %.0f%% 늘었다".formatted(((double) after / before - 1) * 100);
        }
        return "사실상 그대로다";
    }

    private static String millis(long micros) {
        return micros < 1_000 ? "%.2fms".formatted(micros / 1_000.0) : "%,dms".formatted(micros / 1_000);
    }

    private record Verdictish(LoadComparison.Verdict verdict, String headline) {
    }
}
