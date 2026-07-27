package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.application.ExecutionReportProcessor;
import com.maxcapital.executionreports.domain.ApplyResult;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.Order;
import com.maxcapital.executionreports.domain.OrderStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionReportListenerTest {

    @Mock
    private ExecutionReportProcessor processor;

    @Mock
    private Acknowledgment ack;

    @InjectMocks
    private ExecutionReportListener listener;

    @Test
    @DisplayName("Reporte válido procesado con éxito invoca processor y realiza ack manual")
    void validReport_success_invokesProcessorAndAcknowledges() {
        ExecutionReport er = createER(1001L, 5001L, OrderStatus.NEW);
        ConsumerRecord<String, ExecutionReport> record = new ConsumerRecord<>("execution-reports", 0, 0L, "5001", er);

        Order order = new Order(5001L, "MKT-5001", "VSCPC", "BUY", "COMMON_STOCK", OrderStatus.NEW,
                new BigDecimal("10.00"), new BigDecimal("100.00"), new BigDecimal("100.00"), BigDecimal.ZERO, new BigDecimal("10.00"), 1, 1001L);

        when(processor.process(er)).thenReturn(new ApplyResult.Success(order, false));

        listener.listen(record, ack);

        verify(processor).process(er);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Reporte rechazado por dominio invoca processor y realiza ack manual")
    void validReport_rejection_invokesProcessorAndAcknowledges() {
        ExecutionReport er = createER(1002L, 5002L, OrderStatus.FILLED);
        ConsumerRecord<String, ExecutionReport> record = new ConsumerRecord<>("execution-reports", 0, 0L, "5002", er);

        when(processor.process(er)).thenReturn(new ApplyResult.Rejection(ApplyResult.RejectionReason.TERMINAL_STATE, "Terminal state"));

        listener.listen(record, ack);

        verify(processor).process(er);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Reporte duplicado (AlreadyProcessed) invoca processor y realiza ack manual")
    void validReport_alreadyProcessed_invokesProcessorAndAcknowledges() {
        ExecutionReport er = createER(1003L, 5003L, OrderStatus.NEW);
        ConsumerRecord<String, ExecutionReport> record = new ConsumerRecord<>("execution-reports", 0, 0L, "5003", er);

        when(processor.process(er)).thenReturn(new ApplyResult.AlreadyProcessed(1003L, 5003L));

        listener.listen(record, ack);

        verify(processor).process(er);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Registro con valor nulo lanza DeserializationException y no invoca processor ni ack")
    void nullRecordValue_throwsDeserializationException_doesNotAck() {
        ConsumerRecord<String, ExecutionReport> record = new ConsumerRecord<>("execution-reports", 0, 0L, "CORRUPT", null);

        assertThatThrownBy(() -> listener.listen(record, ack))
                .isInstanceOf(DeserializationException.class);

        verifyNoInteractions(processor);
        verifyNoInteractions(ack);
    }

    private ExecutionReport createER(Long fixId, Long numericOrderId, OrderStatus status) {
        BigDecimal price = new BigDecimal("10.00");
        return new ExecutionReport(
                fixId, numericOrderId, "MKT-" + numericOrderId, "VSCPC", "BUY", "COMMON_STOCK",
                status, price, new BigDecimal("100.00"), price, BigDecimal.ZERO, BigDecimal.ZERO,
                price, price, "SEC-" + fixId, "OP-" + fixId, LocalDateTime.now()
        );
    }
}
