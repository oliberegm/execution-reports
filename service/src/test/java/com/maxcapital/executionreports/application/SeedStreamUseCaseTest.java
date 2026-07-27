package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.application.dto.SeedResultDto;
import com.maxcapital.executionreports.domain.ExecutionReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SeedStreamUseCaseTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private SeedStreamUseCase useCase;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(useCase, "executionReportsTopic", "execution-reports");
    }

    @Test
    @DisplayName("seed() genera ordenes intercaladas, inyecta duplicados y publica a Kafka")
    void seed_generatesInterleavedStreamAndPublishesToKafka() {
        @SuppressWarnings("unchecked")
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send(eq("execution-reports"), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        SeedResultDto result = useCase.seed(5, true);

        assertThat(result.seededOrdersCount()).isEqualTo(5);
        assertThat(result.generatedOrderIds()).hasSize(5);
        assertThat(result.totalEventsPublished()).isGreaterThan(5);
        assertThat(result.injectedDuplicatesCount()).isGreaterThan(0);

        verify(kafkaTemplate, atLeastOnce()).send(eq("execution-reports"), any(), any(ExecutionReport.class));
    }
}
