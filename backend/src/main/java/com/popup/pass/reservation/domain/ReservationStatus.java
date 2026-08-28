package com.popup.pass.reservation.domain;

public enum ReservationStatus {
    CONFIRMED,
    REJECTED_SOLD_OUT,
    REJECTED_DUPLICATE,
    REJECTED_ROUND_CLOSED,
    REJECTED_ROUND_NOT_FOUND
}
