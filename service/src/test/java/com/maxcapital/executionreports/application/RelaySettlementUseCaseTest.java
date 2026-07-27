package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxEntity;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RelaySettlementUseCaseTest {

    @Mock
    private SettlementOutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private RelaySettlementUseCase useCase;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(useCase, "settlementTopic", "settlement");
        ReflectionTestUtils.setField(useCase, "batchSize", 50);
    }

    @Test
    @DisplayName("Caso de uso sin filas PENDING no realiza publicaciones a Kafka")
    void emptyOutbox_doesNothing() {
        when(outboxRepository.findPendingForUpdateSkipLocked(50)).thenReturn(Collections.emptyList());

        useCase.execute();

        verify(outboxRepository).findPendingForUpdateSkipLocked(50);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("Caso de uso con filas PENDING publica a Kafka y guarda copia toBuilder con status SENT")
    void pendingRecords_relaysToKafkaAndMarksSent() {
        SettlementOutboxEntity entity = SettlementOutboxEntity.builder()
                .id(1L)
                .numericOrderId(9001L)
                .payload("{\"numericOrderId\":9001,\"status\":\"FILLED\"}")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        when(outboxRepository.findPendingForUpdateSkipLocked(50)).thenReturn(List.of(entity));

        @SuppressWarnings("unchecked")
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send("settlement", "9001", entity.getPayload()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        useCase.execute();

        verify(kafkaTemplate).send("settlement", "9001", entity.getPayload());

        ArgumentCaptor<SettlementOutboxEntity> captor = ArgumentCaptor.forClass(SettlementOutboxEntity.class);
        verify(outboxRepository).save(captor.capture());

        SettlementOutboxEntity savedEntity = captor.getValue();
        assertThat(savedEntity.getId()).isEqualTo(1L);
        assertThat(savedEntity.getNumericOrderId()).isEqualTo(9001L);
        assertThat(savedEntity.getStatus()).isEqualTo("SENT");
        assertThat(savedEntity.getSentAt()).isNotNull();
    }

    @Test
    @DisplayName("Fallo de publicacion en Kafka detiene el procesamiento sin guardar cambio a SENT")
    void kafkaSendFailure_doesNotMarkSentAndStopsLoop() {
        SettlementOutboxEntity entity = SettlementOutboxEntity.builder()
                .id(2L)
                .numericOrderId(9002L)
                .payload("{\"numericOrderId\":9002,\"status\":\"FILLED\"}")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        when(outboxRepository.findPendingForUpdateSkipLocked(50)).thenReturn(List.of(entity));

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker down"));
        when(kafkaTemplate.send("settlement", "9002", entity.getPayload()))
                .thenReturn(failedFuture);

        useCase.execute();

        verify(kafkaTemplate).send("settlement", "9002", entity.getPayload());
        verifyNoMoreInteractions(outboxRepository);
        assertThat(entity.getStatus()).isEqualTo("PENDING");
        assertThat(entity.getSentAt()).isNull();
    }
}
