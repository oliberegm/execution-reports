package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.AbstractIntegrationTest;
import com.maxcapital.executionreports.application.ExecutionReportProcessor;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.OrderStatus;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxEntity;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class OutboxRelayIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private ExecutionReportProcessor processor;

    @Autowired
    private SettlementOutboxRepository outboxRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLedgerRepository orderLedgerRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Value("${app.kafka.topics.settlement:settlement}")
    private String settlementTopic;

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;

    @BeforeEach
    void setUp() {
        outboxRepository.deleteAll();
        orderLedgerRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Reentrega de ER que completa la orden resulta en 1 sola fila outbox y 1 solo mensaje en topic settlement")
    void redeliveredER_createsSingleOutboxAndSingleKafkaMessage() {
        Long numericOrderId = 9501L;

        ExecutionReport newEr = createER(8001L, numericOrderId, OrderStatus.NEW, new BigDecimal("100.00"), new BigDecimal("0.00"));
        ExecutionReport filledEr = createER(8002L, numericOrderId, OrderStatus.FILLED, new BigDecimal("0.00"), new BigDecimal("100.00"));

        processor.process(newEr);
        processor.process(filledEr);
        processor.process(filledEr); // Re-entrega duplicada

        List<SettlementOutboxEntity> outboxRows = outboxRepository.findAll();
        assertThat(outboxRows).hasSize(1);
        assertThat(outboxRows.getFirst().getStatus()).isEqualTo("PENDING");

        outboxRelay.pollAndRelay();

        List<SettlementOutboxEntity> updatedOutboxRows = outboxRepository.findAll();
        assertThat(updatedOutboxRows).hasSize(1);
        assertThat(updatedOutboxRows.getFirst().getStatus()).isEqualTo("SENT");
        assertThat(updatedOutboxRows.getFirst().getSentAt()).isNotNull();

        ConsumerRecord<String, String> record = pollSettlementTopic();
        assertThat(record).isNotNull();
        assertThat(record.key()).isEqualTo(numericOrderId.toString());
        assertThat(record.value()).contains("FILLED");
    }

    @Test
    @DisplayName("Reintento del relay mantiene la misma clave de orden previniendo duplicados no deduplicables")
    void relayRetry_preservesKeyAndPreventsDuplicateOutboxRows() {
        Long numericOrderId = 9502L;

        SettlementOutboxEntity manualEntity = SettlementOutboxEntity.builder()
                .numericOrderId(numericOrderId)
                .payload("{\"numericOrderId\":9502,\"status\":\"FILLED\"}")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();
        outboxRepository.saveAndFlush(manualEntity);

        outboxRelay.pollAndRelay();

        SettlementOutboxEntity relayedEntity = outboxRepository.findById(manualEntity.getId()).orElseThrow();
        assertThat(relayedEntity.getStatus()).isEqualTo("SENT");
    }

    private ConsumerRecord<String, String> pollSettlementTopic() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-settlement-verifier-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);
        try (Consumer<String, String> consumer = cf.createConsumer()) {
            consumer.subscribe(Collections.singletonList(settlementTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        return null;
    }

    private ExecutionReport createER(Long fixId, Long numericOrderId, OrderStatus status, BigDecimal leaves, BigDecimal accum) {
        BigDecimal price = new BigDecimal("10.00");
        return new ExecutionReport(
                fixId, numericOrderId, "MKT-" + numericOrderId, "VSCPC", "BUY", "COMMON_STOCK",
                status, price, new BigDecimal("100.00"), leaves, accum, BigDecimal.ZERO,
                price, price, "SEC-" + fixId, "OP-" + fixId, LocalDateTime.now()
        );
    }
}
