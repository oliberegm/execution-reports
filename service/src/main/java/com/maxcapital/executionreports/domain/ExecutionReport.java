package com.maxcapital.executionreports.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record ExecutionReport(
        Long fixId,
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
        BigDecimal executionNominalAmount,
        BigDecimal executionPrice,
        BigDecimal avgPrice,
        String secondaryTradeId,
        String operationNumber,
        LocalDateTime transactionTime
) {
}
