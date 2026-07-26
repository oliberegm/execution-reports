package com.maxcapital.executionreports.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStateMachineTest {

    private OrderStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new OrderStateMachine();
    }

    @Test
    @DisplayName("NEW report on non-existent order creates new order with executionsAppliedCount = 1")
    void newOrder_fromNewER_createsOrder() {
        ExecutionReport newEr = createER(5001L, 1001L, OrderStatus.NEW,
                new BigDecimal("100"), new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(null, newEr);

        assertThat(result).isInstanceOf(ApplyResult.Success.class);
        ApplyResult.Success success = (ApplyResult.Success) result;

        Order order = success.order();
        assertThat(order.numericOrderId()).isEqualTo(1001L);
        assertThat(order.status()).isEqualTo(OrderStatus.NEW);
        assertThat(order.executionsAppliedCount()).isEqualTo(1);
        assertThat(order.lastAppliedFixId()).isEqualTo(5001L);
        assertThat(order.leavesNominalAmount()).isEqualByComparingTo("100");
        assertThat(order.accumulativeNominalAmount()).isEqualByComparingTo("0");
        assertThat(success.settlementRequired()).isFalse();
    }

    @Test
    @DisplayName("PARTIALLY_FILLED report on non-existent order is rejected as ORPHAN_REPORT")
    void partiallyFilledWithoutPriorNew_isRejected() {
        ExecutionReport partialEr = createER(5002L, 1001L, OrderStatus.PARTIALLY_FILLED,
                new BigDecimal("60"), new BigDecimal("40"), new BigDecimal("40"), new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(null, partialEr);

        assertThat(result).isInstanceOf(ApplyResult.Rejection.class);
        ApplyResult.Rejection rejection = (ApplyResult.Rejection) result;
        assertThat(rejection.reason()).isEqualTo(ApplyResult.RejectionReason.ORPHAN_REPORT);
    }

    @Test
    @DisplayName("Full sequence NEW -> PARTIALLY_FILLED -> FILLED results in correct final state and count = 3")
    void fullSequence_newPartialFilled_resultsInCorrectFinalState() {
        // Step 1: NEW
        ExecutionReport newEr = createER(5001L, 1001L, OrderStatus.NEW,
                new BigDecimal("100"), new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"));
        ApplyResult.Success res1 = (ApplyResult.Success) stateMachine.apply(null, newEr);
        Order order1 = res1.order();

        // Step 2: PARTIALLY_FILLED
        ExecutionReport partialEr = createER(5002L, 1001L, OrderStatus.PARTIALLY_FILLED,
                new BigDecimal("100"), new BigDecimal("40"), new BigDecimal("60"), new BigDecimal("100"));
        ApplyResult.Success res2 = (ApplyResult.Success) stateMachine.apply(order1, partialEr);
        Order order2 = res2.order();

        assertThat(order2.executionsAppliedCount()).isEqualTo(2);
        assertThat(order2.leavesNominalAmount()).isEqualByComparingTo("40");
        assertThat(order2.accumulativeNominalAmount()).isEqualByComparingTo("60");
        assertThat(res2.settlementRequired()).isFalse();

        // Step 3: FILLED
        ExecutionReport filledEr = createER(5003L, 1001L, OrderStatus.FILLED,
                new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"), new BigDecimal("100"));
        ApplyResult.Success res3 = (ApplyResult.Success) stateMachine.apply(order2, filledEr);
        Order order3 = res3.order();

        assertThat(order3.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(order3.executionsAppliedCount()).isEqualTo(3);
        assertThat(order3.leavesNominalAmount()).isEqualByComparingTo("0");
        assertThat(order3.accumulativeNominalAmount()).isEqualByComparingTo("100");
        assertThat(res3.settlementRequired()).isTrue();
    }

    @Test
    @DisplayName("Report on already FILLED order is rejected as TERMINAL_STATE")
    void erOnAlreadyFilledOrder_isRejectedAndStateUnchanged() {
        Order filledOrder = new Order(1001L, "MKT1", "VSCPC", "BUY", "STOCK",
                OrderStatus.FILLED, new BigDecimal("100"), new BigDecimal("100"),
                BigDecimal.ZERO, new BigDecimal("100"), new BigDecimal("100"), 3, 5003L);

        ExecutionReport extraEr = createER(5004L, 1001L, OrderStatus.PARTIALLY_FILLED,
                new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"), new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(filledOrder, extraEr);

        assertThat(result).isInstanceOf(ApplyResult.Rejection.class);
        ApplyResult.Rejection rejection = (ApplyResult.Rejection) result;
        assertThat(rejection.reason()).isEqualTo(ApplyResult.RejectionReason.TERMINAL_STATE);
    }

    @Test
    @DisplayName("Report on already CANCELLED order is rejected as TERMINAL_STATE")
    void erOnAlreadyCancelledOrder_isRejectedAndStateUnchanged() {
        Order cancelledOrder = new Order(1001L, "MKT1", "VSCPC", "BUY", "STOCK",
                OrderStatus.CANCELLED, new BigDecimal("100"), new BigDecimal("100"),
                new BigDecimal("50"), new BigDecimal("50"), new BigDecimal("100"), 2, 5002L);

        ExecutionReport extraEr = createER(5003L, 1001L, OrderStatus.FILLED,
                new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"), new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(cancelledOrder, extraEr);

        assertThat(result).isInstanceOf(ApplyResult.Rejection.class);
        ApplyResult.Rejection rejection = (ApplyResult.Rejection) result;
        assertThat(rejection.reason()).isEqualTo(ApplyResult.RejectionReason.TERMINAL_STATE);
    }

    @Test
    @DisplayName("Transition to FILLED marks settlementRequired = true")
    void transitionToFilled_marksSettlementRequired() {
        Order currentOrder = new Order(1001L, "MKT1", "VSCPC", "BUY", "STOCK",
                OrderStatus.PARTIALLY_FILLED, new BigDecimal("100"), new BigDecimal("100"),
                new BigDecimal("50"), new BigDecimal("50"), new BigDecimal("100"), 2, 5002L);

        ExecutionReport filledEr = createER(5003L, 1001L, OrderStatus.FILLED,
                new BigDecimal("100"), BigDecimal.ZERO, new BigDecimal("100"), new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(currentOrder, filledEr);

        assertThat(result).isInstanceOf(ApplyResult.Success.class);
        ApplyResult.Success success = (ApplyResult.Success) result;
        assertThat(success.order().status()).isEqualTo(OrderStatus.FILLED);
        assertThat(success.settlementRequired()).isTrue();
    }

    @Test
    @DisplayName("Transition to CANCELLED marks settlementRequired = false")
    void transitionToCancelled_doesNotMarkSettlement() {
        Order currentOrder = new Order(1001L, "MKT1", "VSCPC", "BUY", "STOCK",
                OrderStatus.PARTIALLY_FILLED, new BigDecimal("100"), new BigDecimal("100"),
                new BigDecimal("50"), new BigDecimal("50"), new BigDecimal("100"), 2, 5002L);

        ExecutionReport cancelledEr = createER(5003L, 1001L, OrderStatus.CANCELLED,
                new BigDecimal("100"), new BigDecimal("50"), new BigDecimal("50"), new BigDecimal("100"));

        ApplyResult result = stateMachine.apply(currentOrder, cancelledEr);

        assertThat(result).isInstanceOf(ApplyResult.Success.class);
        ApplyResult.Success success = (ApplyResult.Success) result;
        assertThat(success.order().status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(success.settlementRequired()).isFalse();
    }

    private ExecutionReport createER(Long fixId, Long numericOrderId, OrderStatus status,
                                      BigDecimal totalAmount, BigDecimal leavesAmount,
                                      BigDecimal accumulativeAmount, BigDecimal price) {
        return new ExecutionReport(
                fixId,
                numericOrderId,
                "MKT" + numericOrderId,
                "VSCPC",
                "BUY",
                "COMMON_STOCK",
                status,
                price,
                totalAmount,
                leavesAmount,
                accumulativeAmount,
                BigDecimal.ZERO,
                price,
                price,
                "SEC" + fixId,
                "OP" + fixId,
                LocalDateTime.now()
        );
    }
}
