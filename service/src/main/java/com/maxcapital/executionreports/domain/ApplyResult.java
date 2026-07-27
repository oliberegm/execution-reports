package com.maxcapital.executionreports.domain;

public sealed interface ApplyResult permits ApplyResult.Success, ApplyResult.Rejection, ApplyResult.AlreadyProcessed {

    enum RejectionReason {
        ORPHAN_REPORT,
        TERMINAL_STATE
    }

    record Success(Order order, boolean settlementRequired) implements ApplyResult {
    }

    record Rejection(RejectionReason reason, String message) implements ApplyResult {
    }

    record AlreadyProcessed(Long fixId, Long numericOrderId) implements ApplyResult {
    }
}
