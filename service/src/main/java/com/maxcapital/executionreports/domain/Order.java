package com.maxcapital.executionreports.domain;

import java.math.BigDecimal;

public record Order(
        Long numericOrderId,
        String marketOrderId,
        String ticker,
        String side,
        String securityType,
        OrderStatus status,
        BigDecimal orderPrice,
        BigDecimal nominalAmounts,
        BigDecimal leavesNominalAmount,
        BigDecimal accumulativeNominalAmount,
        BigDecimal avgPrice,
        int executionsAppliedCount,
        Long lastAppliedFixId
) {
    public static Order createFromFirstER(ExecutionReport er) {
        return new Order(
                er.numericOrderId(),
                er.marketOrderId(),
                er.ticker(),
                er.side(),
                er.securityType(),
                er.status(),
                er.orderPrice(),
                er.nominalAmounts(),
                er.leavesNominalAmount(),
                er.accumulativeNominalAmount(),
                er.avgPrice(),
                1, // First execution report applied (NEW)
                er.fixId()
        );
    }

    public Order withUpdatedState(ExecutionReport er) {
        return new Order(
                this.numericOrderId,
                er.marketOrderId() != null ? er.marketOrderId() : this.marketOrderId,
                er.ticker() != null ? er.ticker() : this.ticker,
                er.side() != null ? er.side() : this.side,
                er.securityType() != null ? er.securityType() : this.securityType,
                er.status(),
                er.orderPrice() != null ? er.orderPrice() : this.orderPrice,
                er.nominalAmounts() != null ? er.nominalAmounts() : this.nominalAmounts,
                er.leavesNominalAmount() != null ? er.leavesNominalAmount() : this.leavesNominalAmount,
                er.accumulativeNominalAmount() != null ? er.accumulativeNominalAmount() : this.accumulativeNominalAmount,
                er.avgPrice() != null ? er.avgPrice() : this.avgPrice,
                this.executionsAppliedCount + 1,
                er.fixId()
        );
    }
}
