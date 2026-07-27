package com.maxcapital.executionreports.application.dto;

import java.util.List;

public record SeedResultDto(
        int seededOrdersCount,
        int totalEventsPublished,
        int injectedDuplicatesCount,
        List<Long> generatedOrderIds
) {
}
