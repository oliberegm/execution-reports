package com.maxcapital.executionreports.application.dto;

import java.time.LocalDateTime;

public record OrderLedgerResponseDto(
        Long id,
        Long numericOrderId,
        Long fixId,
        String status,
        Object payload,
        LocalDateTime appliedAt
) {
}
