package com.popup.pass.load;

import java.util.List;

/**
 * 같은 부하를 <strong>보호 없는 코드</strong>와 <strong>다 켠 코드</strong>에 한 번씩 걸어 나란히 둔 결과.
 *
 * <p>이 화면이 답하려는 질문은 하나다 — <em>"다 고치면 빨라지는가."</em>
 * 정합성과 지연은 같은 실행에서 함께 깨지지만 처방이 다르다는 것을, 주장이 아니라 두 열의 숫자로 보여야 한다.
 */
public record LoadComparison(
        int concurrency,
        int clientPatienceMillis,
        LoadRunResult unguarded,
        LoadRunResult guarded,
        Verdict verdict,
        String headline,
        List<String> notes) {

    public enum Verdict {
        /** 정합성은 고쳐졌는데 응답은 그대로거나 더 느려졌다. 큐가 필요한 자리다. */
        LATENCY_REMAINS,
        /** 보호 장치가 응답 시간까지 줄였다. */
        BOTH_IMPROVED,
        /** 정합성 차이가 나타나지 않았다. 부하가 너무 작아 경합이 안 생겼을 수 있다. */
        INCONCLUSIVE
    }
}
