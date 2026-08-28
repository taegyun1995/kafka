package com.popup.pass.study;

import java.util.Arrays;
import java.util.Optional;

/** 모든 단계에 똑같이 들이대는 상황들. 같은 상황인데 단계마다 결과가 다른 것이 요점이다. */
public enum StudyScenario {

    OVERSELL("oversell", "정원 2에 5명이 동시에",
            "서로 다른 다섯 사람이 같은 순간에 요청한다. 남은 자리는 둘뿐이다.",
            2, 5, false, true, false),

    DOUBLE_BOOKING("double-booking", "한 사람이 동시에 두 번",
            "같은 사람의 요청 두 건이 같은 순간에 도착한다. 흔한 더블 클릭이다.",
            5, 2, true, true, false),

    RETRY("retry", "응답을 못 받아 다시 보낸다",
            "확정은 됐는데 응답이 유실됐다. 클라이언트가 같은 키를 붙여 다시 보낸다.",
            5, 2, true, false, true),

    CONCURRENT_RETRY("concurrent-retry", "같은 키로 동시에 두 번",
            "재시도가 원래 요청과 겹쳤다. 같은 키를 단 두 건이 같은 순간에 도착한다.",
            5, 2, true, true, true);

    private final String id;
    private final String title;
    private final String description;
    private final int capacity;
    private final int attempts;
    private final boolean sameUser;
    private final boolean concurrent;
    private final boolean sendsIdempotencyKey;

    StudyScenario(String id, String title, String description, int capacity, int attempts,
                  boolean sameUser, boolean concurrent, boolean sendsIdempotencyKey) {
        this.id = id;
        this.title = title;
        this.description = description;
        this.capacity = capacity;
        this.attempts = attempts;
        this.sameUser = sameUser;
        this.concurrent = concurrent;
        this.sendsIdempotencyKey = sendsIdempotencyKey;
    }

    public static Optional<StudyScenario> byId(String id) {
        return Arrays.stream(values()).filter(scenario -> scenario.id.equals(id)).findFirst();
    }

    public String id() {
        return id;
    }

    /** enum의 name()은 final이라 화면에 쓸 이름은 따로 둔다. */
    public String title() {
        return title;
    }

    public String description() {
        return description;
    }

    public int capacity() {
        return capacity;
    }

    public int attempts() {
        return attempts;
    }

    public boolean sameUser() {
        return sameUser;
    }

    public boolean concurrent() {
        return concurrent;
    }

    public boolean sendsIdempotencyKey() {
        return sendsIdempotencyKey;
    }

    public String userIdFor(int index) {
        return sameUser ? "user-1" : "user-" + (index + 1);
    }
}
