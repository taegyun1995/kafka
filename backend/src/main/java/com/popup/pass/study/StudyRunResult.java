package com.popup.pass.study;

import java.util.List;

/** 버튼 한 번의 결과. 판정과 설명은 하드코딩이 아니라 방금 나온 숫자에서 만든다. */
public record StudyRunResult(
        String stepId,
        String stepName,
        String addedGuard,
        String stepSummary,
        String scenarioId,
        String scenarioName,
        String scenarioDescription,
        int capacity,
        int remainingCount,
        int seatsTaken,
        int savedRows,
        long confirmed,
        long rejectedSoldOut,
        long rejectedDuplicate,
        List<AttemptView> attempts,
        long elapsedMillis,
        Verdict verdict,
        String headline,
        List<String> notes) {

    /** 시도 한 건. requestId까지 보여야 "원래 결과를 되찾을 수 있는가"가 눈에 보인다. */
    public record AttemptView(int order, String userId, String requestId, String status) {
    }

    public enum Verdict {
        /** 실제로 깨졌다. */
        BROKEN,
        /** 막기는 했는데 대가가 있다. */
        PARTIAL,
        /** 막혔다. */
        OK
    }
}
