package com.popup.pass.reservation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "reservation_rounds")
public class ReservationRound {

    @Id
    @Column(length = 80)
    private String id;

    @Column(nullable = false, length = 160)
    private String name;

    @Column(nullable = false)
    private int capacity;

    @Column(name = "remaining_count", nullable = false)
    private int remainingCount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private RoundStatus status;

    protected ReservationRound() {
    }

    public ReservationRound(String id, String name, int capacity) {
        this.id = id;
        this.name = name;
        this.capacity = capacity;
        this.remainingCount = capacity;
        this.status = RoundStatus.OPEN;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getRemainingCount() {
        return remainingCount;
    }

    public RoundStatus getStatus() {
        return status;
    }

    /** 자리 하나를 뺀다. 남은 자리가 있는지는 부르는 쪽이 먼저 확인한다. */
    public void takeOne() {
        this.remainingCount = this.remainingCount - 1;
    }

    /** 마감은 설정 연산이라 이미 닫혀 있어도 결과가 같다. 되돌리는 기능은 두지 않는다. */
    public void close() {
        this.status = RoundStatus.CLOSED;
    }
}
