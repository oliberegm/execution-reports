package com.maxcapital.executionreports.domain;

public enum OrderStatus {
    NEW,
    PARTIALLY_FILLED,
    FILLED,
    CANCELLED;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED;
    }
}
