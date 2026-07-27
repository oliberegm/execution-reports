package com.maxcapital.executionreports.application.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record OrderResponseDto(
        Long numericOrderId,
        String marketOrderId,
        String ticker,
        String side,
        String securityType,
        String status,
        BigDecimal orderPrice,
        BigDecimal nominalAmounts,
        BigDecimal leavesNominalAmount,
        BigDecimal accumulativeNominalAmount,
        BigDecimal avgPrice,
        Integer executionsAppliedCount,
        Long lastAppliedFixId,
        LocalDateTime createdAt,
        LocalDateTime updatedAt,
        List<OrderLedgerResponseDto> ledger
) {
}
