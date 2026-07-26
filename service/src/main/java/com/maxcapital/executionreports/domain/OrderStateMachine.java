package com.maxcapital.executionreports.domain;

public class OrderStateMachine {

    public ApplyResult apply(Order currentOrder, ExecutionReport incomingEr) {
        if (currentOrder == null) {
            if (incomingEr.status() != OrderStatus.NEW) {
                return new ApplyResult.Rejection(
                        ApplyResult.RejectionReason.ORPHAN_REPORT,
                        "Cannot apply non-NEW ExecutionReport (" + incomingEr.status()
                                + ") to non-existent order " + incomingEr.numericOrderId()
                );
            }
            Order newOrder = Order.createFromFirstER(incomingEr);
            boolean settlementRequired = (newOrder.status() == OrderStatus.FILLED);
            return new ApplyResult.Success(newOrder, settlementRequired);
        }

        if (currentOrder.status().isTerminal()) {
            return new ApplyResult.Rejection(
                    ApplyResult.RejectionReason.TERMINAL_STATE,
                    "Cannot apply ExecutionReport to order " + currentOrder.numericOrderId()
                            + " already in terminal state " + currentOrder.status()
            );
        }

        Order updatedOrder = currentOrder.withUpdatedState(incomingEr);
        boolean settlementRequired = (updatedOrder.status() == OrderStatus.FILLED);
        return new ApplyResult.Success(updatedOrder, settlementRequired);
    }
}
