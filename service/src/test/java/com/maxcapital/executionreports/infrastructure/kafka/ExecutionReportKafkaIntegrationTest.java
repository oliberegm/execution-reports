package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.AbstractIntegrationTest;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.OrderStatus;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
class ExecutionReportKafkaIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLedgerRepository orderLedgerRepository;

    @Autowired
    private SettlementOutboxRepository settlementOutboxRepository;

    @Value("${app.kafka.topics.execution-reports:execution-reports}")
    private String executionReportsTopic;

    @Value("${app.kafka.topics.execution-reports-dlq:execution-reports.dlq}")
    private String dlqTopic;

    @Value("${spring.embedded.kafka.brokers}")
    private String embeddedKafkaBrokers;

    @BeforeEach
    void setUp() {
        settlementOutboxRepository.deleteAll();
        orderLedgerRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Publicar stream intercalado de 4 órdenes con duplicado en el medio procesa correctamente y absorbe duplicado")
    void interleavedStreamWithDuplicates_processesCorrectlyAndIgnoresDuplicates() {
        Long order1 = 8001L;
        Long order2 = 8002L;
        Long order3 = 8003L;

        ExecutionReport er1_new = createER(7001L, order1, OrderStatus.NEW, new BigDecimal("100.00"), new BigDecimal("0.00"));
        ExecutionReport er2_new = createER(7002L, order2, OrderStatus.NEW, new BigDecimal("200.00"), new BigDecimal("0.00"));
        ExecutionReport er1_dup = createER(7001L, order1, OrderStatus.NEW, new BigDecimal("100.00"), new BigDecimal("0.00"));
        ExecutionReport er3_new = createER(7003L, order3, OrderStatus.NEW, new BigDecimal("300.00"), new BigDecimal("0.00"));
        ExecutionReport er1_fill = createER(7004L, order1, OrderStatus.FILLED, new BigDecimal("0.00"), new BigDecimal("100.00"));

        kafkaTemplate.send(executionReportsTopic, order1.toString(), er1_new);
        kafkaTemplate.send(executionReportsTopic, order2.toString(), er2_new);
        kafkaTemplate.send(executionReportsTopic, order1.toString(), er1_dup);
        kafkaTemplate.send(executionReportsTopic, order3.toString(), er3_new);
        kafkaTemplate.send(executionReportsTopic, order1.toString(), er1_fill);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<OrderEntity> o1 = orderRepository.findByNumericOrderId(order1);
            Optional<OrderEntity> o2 = orderRepository.findByNumericOrderId(order2);
            Optional<OrderEntity> o3 = orderRepository.findByNumericOrderId(order3);

            assertThat(o1).isPresent();
            assertThat(o1.get().getStatus()).isEqualTo("FILLED");
            assertThat(o1.get().getExecutionsAppliedCount()).isEqualTo(2);

            assertThat(o2).isPresent();
            assertThat(o2.get().getStatus()).isEqualTo("NEW");

            assertThat(o3).isPresent();
            assertThat(o3.get().getStatus()).isEqualTo("NEW");

            List<OrderLedgerEntity> ledger1 = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(order1);
            assertThat(ledger1).hasSize(2);
        });
    }

    @Test
    @DisplayName("Publicar un mensaje corrupto lo envía a execution-reports.dlq y el consumidor continúa procesando")
    void corruptedMessage_isSentToDlqAndConsumerContinues() {
        Long validOrderId = 8010L;
        ExecutionReport validEr = createER(7010L, validOrderId, OrderStatus.NEW, new BigDecimal("500.00"), new BigDecimal("0.00"));

        kafkaTemplate.send(executionReportsTopic, "CORRUPT_KEY", "INVALID_NON_JSON_STRING");
        kafkaTemplate.send(executionReportsTopic, validOrderId.toString(), validEr);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<OrderEntity> order = orderRepository.findByNumericOrderId(validOrderId);
            assertThat(order).isPresent();
            assertThat(order.get().getStatus()).isEqualTo("NEW");
        });

        ConsumerRecord<String, String> dlqRecord = pollDlqRecord();
        assertThat(dlqRecord).isNotNull();
        assertThat(dlqRecord.key()).isEqualTo("CORRUPT_KEY");
        assertThat(dlqRecord.value()).isNotNull();
    }

    private ConsumerRecord<String, String> pollDlqRecord() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBrokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-verifier-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);
        try (Consumer<String, String> consumer = cf.createConsumer()) {
            consumer.subscribe(Collections.singletonList(dlqTopic));
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
