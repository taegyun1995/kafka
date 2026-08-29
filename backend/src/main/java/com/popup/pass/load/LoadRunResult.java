package com.popup.pass.load;

import java.util.List;

/**
 * 부하 한 번의 결과. 지금까지 본 것이 <em>정합성</em> 문제였다면 여기서 보는 것은 <em>처리량과 지연</em>이다.
 *
 * <p>판정과 설명은 하드코딩이 아니라 방금 잰 숫자에서 만든다.
 */
public record LoadRunResult(
        String targetId,
        String targetLabel,
        String addedGuard,
        int concurrency,
        int capacity,
        String roundId,
        long elapsedMillis,
        double throughputPerSecond,
        int succeeded,
        int failed,
        int clientPatienceMillis,
        int warmupRequests,
        Ledger ledger,
        int wouldHaveGivenUp,
        List<FailureView> failures,
        Latency latency,
        Pool pool,
        Verdict verdict,
        String headline,
        List<String> notes) {

    /**
     * 요청 하나가 응답을 받기까지 걸린 시간의 분포. 마이크로초로 재 둔다 —
     * 빠른 요청까지 0ms로 뭉개면 "꼬리가 길어졌다"가 보이지 않는다.
     */
    public record Latency(long minMicros, long p50Micros, long p95Micros, long p99Micros, long maxMicros) {

        /** 가장 느린 요청이 가장 빠른 요청의 몇 배였는지. 동기 처리에서 제일 먼저 벌어지는 값이다. */
        public double tailRatio() {
            return minMicros == 0 ? 0 : (double) maxMicros / minMicros;
        }
    }

    /** 회차 장부. 확정 건수와 실제로 빠진 자리가 어긋나는지 확인해 정합성을 함께 본다. */
    public record Ledger(long confirmed, int seatsTaken, int lostDecrements) {
    }

    /** 커넥션 풀을 실행 중에 계속 들여다본 기록. 병목이 어디였는지는 여기에 남는다. */
    public record Pool(int maxPoolSize, int peakActive, int peakAwaiting, int samples) {
    }

    /** 실패는 사유별로 묶는다. 스택 트레이스가 아니라 "무엇 때문에 몇 건"이 필요하다. */
    public record FailureView(String reason, int count) {
    }

    public enum Verdict {
        /** 풀 앞에 줄이 서지 않았다. 동시 요청이 커넥션 수 안에 들어왔다. */
        CALM,
        /** 커넥션을 기다린 요청이 있었지만, 그래도 모두 참을 만한 시간 안에 응답했다. */
        QUEUEING,
        /** 예외가 났거나, 기다리다 클라이언트라면 이미 포기했을 요청이 나왔다. */
        FAILING
    }
}
