package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxEntity;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class RelaySettlementUseCase {

    private final SettlementOutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.settlement:settlement}")
    private String settlementTopic;

    @Value("${app.outbox.relay.batch-size:50}")
    private int batchSize;

    @Transactional
    public void execute() {
        List<SettlementOutboxEntity> pendingRecords = outboxRepository.findPendingForUpdateSkipLocked(batchSize);
        if (pendingRecords.isEmpty()) {
            return;
        }

        log.info("Executing RelaySettlementUseCase for {} pending settlement outbox records.", pendingRecords.size());

        for (SettlementOutboxEntity record : pendingRecords) {
            try {
                kafkaTemplate.send(settlementTopic, record.getNumericOrderId().toString(), record.getPayload()).get();

                SettlementOutboxEntity sentEntity = record.toBuilder()
                        .status("SENT")
                        .sentAt(LocalDateTime.now())
                        .build();

                outboxRepository.save(sentEntity);
                log.info("Successfully relayed settlement outbox id={} for numericOrderId={} to topic={}",
                        record.getId(), record.getNumericOrderId(), settlementTopic);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while sending settlement outbox id={} to Kafka", record.getId(), e);
                break;
            } catch (ExecutionException e) {
                log.error("Failed to send settlement outbox id={} to Kafka. Will retry next cycle.", record.getId(), e.getCause());
                break;
            }
        }
    }
}
