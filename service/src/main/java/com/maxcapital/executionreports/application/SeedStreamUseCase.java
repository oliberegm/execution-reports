package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.application.dto.SeedResultDto;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Slf4j
@Service
@RequiredArgsConstructor
public class SeedStreamUseCase {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.execution-reports:execution-reports}")
    private String executionReportsTopic;

    public SeedResultDto seed(int ordersCount, boolean injectDuplicates) {
        if (ordersCount <= 0) {
            ordersCount = 5;
        }

        long baseOrderId = 90000L + (System.currentTimeMillis() % 10000L);
        List<Long> orderIds = new ArrayList<>();
        List<List<ExecutionReport>> orderStreams = new ArrayList<>();

        for (int i = 1; i <= ordersCount; i++) {
            long numericOrderId = baseOrderId + i;
            orderIds.add(numericOrderId);
            orderStreams.add(generateReportsForOrder(numericOrderId, i));
        }

        List<ExecutionReport> interleavedStream = interleaveStreams(orderStreams, injectDuplicates);

        log.info("Seeding {} orders into topic '{}' with total {} events (interleaved, duplicatesInjected={})",
                ordersCount, executionReportsTopic, interleavedStream.size(), injectDuplicates);

        int duplicatesCount = 0;
        for (ExecutionReport er : interleavedStream) {
            try {
                kafkaTemplate.send(executionReportsTopic, er.numericOrderId().toString(), er).get();
            } catch (Exception e) {
                log.error("Error sending seeded event fixId={} for order={}", er.fixId(), er.numericOrderId(), e);
            }
        }

        if (injectDuplicates) {
            duplicatesCount = (int) interleavedStream.stream()
                    .map(ExecutionReport::fixId)
                    .filter(fixId -> interleavedStream.stream().filter(e -> e.fixId().equals(fixId)).count() > 1)
                    .distinct()
                    .count();
        }

        return new SeedResultDto(ordersCount, interleavedStream.size(), duplicatesCount, orderIds);
    }

    private List<ExecutionReport> generateReportsForOrder(long numericOrderId, int index) {
        List<ExecutionReport> reports = new ArrayList<>();
        long baseFixId = numericOrderId * 100L;
        BigDecimal price = new BigDecimal("10.00").add(new BigDecimal(index));
        BigDecimal totalQty = new BigDecimal("100.00");

        // 1. NEW
        reports.add(createReport(baseFixId + 1, numericOrderId, OrderStatus.NEW, price, totalQty, totalQty, BigDecimal.ZERO));

        if (index % 3 == 0) {
            // Cancelled stream
            reports.add(createReport(baseFixId + 2, numericOrderId, OrderStatus.CANCELLED, price, totalQty, totalQty, BigDecimal.ZERO));
        } else if (index % 2 == 0) {
            // Partial -> Filled stream
            BigDecimal halfQty = new BigDecimal("50.00");
            reports.add(createReport(baseFixId + 2, numericOrderId, OrderStatus.PARTIALLY_FILLED, price, totalQty, halfQty, halfQty));
            reports.add(createReport(baseFixId + 3, numericOrderId, OrderStatus.FILLED, price, totalQty, BigDecimal.ZERO, totalQty));
        } else {
            // Direct Filled stream
            reports.add(createReport(baseFixId + 2, numericOrderId, OrderStatus.FILLED, price, totalQty, BigDecimal.ZERO, totalQty));
        }

        return reports;
    }

    private List<ExecutionReport> interleaveStreams(List<List<ExecutionReport>> orderStreams, boolean injectDuplicates) {
        List<ExecutionReport> result = new ArrayList<>();
        int maxLen = orderStreams.stream().mapToInt(List::size).max().orElse(0);

        for (int step = 0; step < maxLen; step++) {
            for (List<ExecutionReport> stream : orderStreams) {
                if (step < stream.size()) {
                    ExecutionReport er = stream.get(step);
                    result.add(er);

                    // Inyectar duplicado del primer mensaje de la primera orden en la mitad del stream
                    if (injectDuplicates && step == 0 && result.size() == 2) {
                        result.add(er); // Duplicado intencional de fix_id
                    }
                }
            }
        }

        return result;
    }

    private ExecutionReport createReport(Long fixId, Long numericOrderId, OrderStatus status,
                                         BigDecimal price, BigDecimal totalQty, BigDecimal leaves, BigDecimal accum) {
        return new ExecutionReport(
                fixId, numericOrderId, "MKT-" + numericOrderId, "GGAL", "BUY", "COMMON_STOCK",
                status, price, totalQty, leaves, accum, BigDecimal.ZERO,
                price, price, "SEC-" + fixId, "OP-" + fixId, LocalDateTime.now()
        );
    }
}
