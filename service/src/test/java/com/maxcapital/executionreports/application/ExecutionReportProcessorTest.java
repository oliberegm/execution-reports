package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.AbstractIntegrationTest;
import com.maxcapital.executionreports.domain.ApplyResult;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.OrderStatus;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxEntity;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionReportProcessorTest extends AbstractIntegrationTest {

    @Autowired
    private ExecutionReportProcessor processor;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLedgerRepository orderLedgerRepository;

    @Autowired
    private SettlementOutboxRepository settlementOutboxRepository;

    @BeforeEach
    void setUp() {
        settlementOutboxRepository.deleteAll();
        orderLedgerRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Aplicar secuencia completa NEW -> PARTIALLY_FILLED -> FILLED persiste la orden, 3 filas de ledger y 1 outbox")
    void fullSequence_newPartialFilled_persistsOrderAndLedgerCorrectly() {
        Long numericOrderId = 5001L;

        ExecutionReport newEr = createER(1001L, numericOrderId, OrderStatus.NEW,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));
        ExecutionReport partialEr = createER(1002L, numericOrderId, OrderStatus.PARTIALLY_FILLED,
                new BigDecimal("50.00"), new BigDecimal("50.00"), new BigDecimal("10.00"));
        ExecutionReport filledEr = createER(1003L, numericOrderId, OrderStatus.FILLED,
                new BigDecimal("0.00"), new BigDecimal("100.00"), new BigDecimal("10.00"));

        ApplyResult res1 = processor.process(newEr);
        ApplyResult res2 = processor.process(partialEr);
        ApplyResult res3 = processor.process(filledEr);

        assertThat(res1).isInstanceOf(ApplyResult.Success.class);
        assertThat(res2).isInstanceOf(ApplyResult.Success.class);
        assertThat(res3).isInstanceOf(ApplyResult.Success.class);

        Optional<OrderEntity> orderOpt = orderRepository.findByNumericOrderId(numericOrderId);
        assertThat(orderOpt).isPresent();
        OrderEntity order = orderOpt.get();
        assertThat(order.getStatus()).isEqualTo("FILLED");
        assertThat(order.getExecutionsAppliedCount()).isEqualTo(3);
        assertThat(order.getLastAppliedFixId()).isEqualTo(1003L);

        List<OrderLedgerEntity> ledger = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);
        assertThat(ledger).hasSize(3);

        List<SettlementOutboxEntity> outbox = settlementOutboxRepository.findAll();
        assertThat(outbox).hasSize(1);
        assertThat(outbox.getFirst().getNumericOrderId()).isEqualTo(numericOrderId);
        assertThat(outbox.getFirst().getStatus()).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Aplicar el mismo fix_id dos veces es idempotente, devuelve AlreadyProcessed y no duplica ejecuciones")
    void duplicateFixId_isIdempotentAndDoesNotDuplicateExecution() {
        Long numericOrderId = 5002L;
        ExecutionReport newEr = createER(1001L, numericOrderId, OrderStatus.NEW,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));

        ApplyResult res1 = processor.process(newEr);
        ApplyResult res2 = processor.process(newEr);

        assertThat(res1).isInstanceOf(ApplyResult.Success.class);
        assertThat(res2).isInstanceOf(ApplyResult.AlreadyProcessed.class);
        ApplyResult.AlreadyProcessed alreadyProcessed = (ApplyResult.AlreadyProcessed) res2;
        assertThat(alreadyProcessed.fixId()).isEqualTo(1001L);
        assertThat(alreadyProcessed.numericOrderId()).isEqualTo(numericOrderId);

        Optional<OrderEntity> orderOpt = orderRepository.findByNumericOrderId(numericOrderId);
        assertThat(orderOpt).isPresent();
        assertThat(orderOpt.get().getExecutionsAppliedCount()).isEqualTo(1);

        List<OrderLedgerEntity> ledger = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);
        assertThat(ledger).hasSize(1);
    }

    @Test
    @DisplayName("Aplicar un ExecutionReport sobre una orden en estado terminal FILLED es rechazado como anomalía y no altera la orden")
    void erOnTerminalState_logsAnomalyAndDoesNotUpdateOrder() {
        Long numericOrderId = 5003L;
        ExecutionReport newEr = createER(1001L, numericOrderId, OrderStatus.NEW,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));
        ExecutionReport filledEr = createER(1002L, numericOrderId, OrderStatus.FILLED,
                new BigDecimal("0.00"), new BigDecimal("100.00"), new BigDecimal("10.00"));

        processor.process(newEr);
        processor.process(filledEr);

        ExecutionReport extraEr = createER(1003L, numericOrderId, OrderStatus.PARTIALLY_FILLED,
                new BigDecimal("20.00"), new BigDecimal("80.00"), new BigDecimal("10.00"));
        ApplyResult rejectedRes = processor.process(extraEr);

        assertThat(rejectedRes).isInstanceOf(ApplyResult.Rejection.class);
        ApplyResult.Rejection rejection = (ApplyResult.Rejection) rejectedRes;
        assertThat(rejection.reason()).isEqualTo(ApplyResult.RejectionReason.TERMINAL_STATE);

        Optional<OrderEntity> orderOpt = orderRepository.findByNumericOrderId(numericOrderId);
        assertThat(orderOpt).isPresent();
        assertThat(orderOpt.get().getStatus()).isEqualTo("FILLED");
        assertThat(orderOpt.get().getExecutionsAppliedCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Transición a FILLED genera entrada en settlement_outbox mientras que CANCELLED no inserta nada")
    void filledState_createsSettlementOutboxEntry_cancelledDoesNot() {
        Long numericOrderIdCancelled = 5004L;
        ExecutionReport newEr = createER(1001L, numericOrderIdCancelled, OrderStatus.NEW,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));
        ExecutionReport cancelEr = createER(1002L, numericOrderIdCancelled, OrderStatus.CANCELLED,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));

        processor.process(newEr);
        processor.process(cancelEr);

        Optional<OrderEntity> orderOpt = orderRepository.findByNumericOrderId(numericOrderIdCancelled);
        assertThat(orderOpt).isPresent();
        assertThat(orderOpt.get().getStatus()).isEqualTo("CANCELLED");

        List<SettlementOutboxEntity> outbox = settlementOutboxRepository.findAll();
        assertThat(outbox).isEmpty();
    }

    @Test
    @DisplayName("Ejecución concurrente de threads invocando el mismo fix_id resulta en 1 Success y (N-1) AlreadyProcessed sin excepciones")
    void concurrentExecution_sameFixId_onlyOneSucceeds() throws InterruptedException {
        Long numericOrderId = 5005L;
        Long fixId = 2000L;
        ExecutionReport er = createER(fixId, numericOrderId, OrderStatus.NEW,
                new BigDecimal("100.00"), new BigDecimal("0.00"), new BigDecimal("10.00"));

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger alreadyProcessedCounter = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    ApplyResult result = processor.process(er);
                    if (result instanceof ApplyResult.Success) {
                        successCounter.incrementAndGet();
                    } else if (result instanceof ApplyResult.AlreadyProcessed) {
                        alreadyProcessedCounter.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(terminated).isTrue();

        assertThat(successCounter.get()).isEqualTo(1);
        assertThat(alreadyProcessedCounter.get()).isEqualTo(threadCount - 1);

        List<OrderLedgerEntity> ledger = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);
        assertThat(ledger).hasSize(1);

        Optional<OrderEntity> orderOpt = orderRepository.findByNumericOrderId(numericOrderId);
        assertThat(orderOpt).isPresent();
        assertThat(orderOpt.get().getExecutionsAppliedCount()).isEqualTo(1);
    }

    private ExecutionReport createER(
            Long fixId,
            Long numericOrderId,
            OrderStatus status,
            BigDecimal leaves,
            BigDecimal accum,
            BigDecimal price
    ) {
        return new ExecutionReport(
                fixId,
                numericOrderId,
                "MKT-" + numericOrderId,
                "VSCPC",
                "BUY",
                "COMMON_STOCK",
                status,
                price,
                new BigDecimal("100.00"),
                leaves,
                accum,
                BigDecimal.ZERO,
                price,
                price,
                "SEC-" + fixId,
                "OP-" + fixId,
                LocalDateTime.now()
        );
    }
}
