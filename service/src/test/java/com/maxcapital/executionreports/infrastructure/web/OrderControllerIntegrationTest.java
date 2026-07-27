package com.maxcapital.executionreports.infrastructure.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.executionreports.AbstractIntegrationTest;
import com.maxcapital.executionreports.application.dto.OrderResponseDto;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.OrderStatus;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class OrderControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLedgerRepository orderLedgerRepository;

    @Autowired
    private SettlementOutboxRepository outboxRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${app.kafka.topics.execution-reports:execution-reports}")
    private String executionReportsTopic;

    @BeforeEach
    void setUp() {
        outboxRepository.deleteAll();
        orderLedgerRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Flujo E2E: publicar ERs via Kafka y consultar GET /orders/{numericOrderId} verificando estado y ledger ordenado")
    void endToEnd_publishKafkaEvents_and_queryOrderEndpoint() throws Exception {
        Long numericOrderId = 88001L;

        ExecutionReport erNew = createER(10001L, numericOrderId, OrderStatus.NEW, new BigDecimal("100.00"), new BigDecimal("0.00"));
        ExecutionReport erPartial = createER(10002L, numericOrderId, OrderStatus.PARTIALLY_FILLED, new BigDecimal("40.00"), new BigDecimal("60.00"));
        ExecutionReport erFilled = createER(10003L, numericOrderId, OrderStatus.FILLED, new BigDecimal("0.00"), new BigDecimal("100.00"));

        kafkaTemplate.send(executionReportsTopic, numericOrderId.toString(), erNew).get();
        kafkaTemplate.send(executionReportsTopic, numericOrderId.toString(), erPartial).get();
        kafkaTemplate.send(executionReportsTopic, numericOrderId.toString(), erFilled).get();

        // Esperar a que la orden alcance FILLED en la base de datos
        await().atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> orderRepository.findByNumericOrderId(numericOrderId)
                        .map(o -> "FILLED".equals(o.getStatus()))
                        .orElse(false));

        // Consultar el endpoint HTTP
        MvcResult mvcResult = mockMvc.perform(get("/orders/{numericOrderId}", numericOrderId)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.numericOrderId").value(numericOrderId))
                .andExpect(jsonPath("$.status").value("FILLED"))
                .andExpect(jsonPath("$.executionsAppliedCount").value(3))
                .andExpect(jsonPath("$.lastAppliedFixId").value(10003))
                .andExpect(jsonPath("$.ledger.length()").value(3))
                .andExpect(jsonPath("$.ledger[0].fixId").value(10001))
                .andExpect(jsonPath("$.ledger[0].status").value("NEW"))
                .andExpect(jsonPath("$.ledger[1].fixId").value(10002))
                .andExpect(jsonPath("$.ledger[1].status").value("PARTIALLY_FILLED"))
                .andExpect(jsonPath("$.ledger[2].fixId").value(10003))
                .andExpect(jsonPath("$.ledger[2].status").value("FILLED"))
                .andReturn();

        String responseJson = mvcResult.getResponse().getContentAsString();
        OrderResponseDto responseDto = objectMapper.readValue(responseJson, OrderResponseDto.class);

        assertThat(responseDto.ledger()).hasSize(3);
        assertThat(responseDto.ledger().get(0).id()).isLessThan(responseDto.ledger().get(1).id());
        assertThat(responseDto.ledger().get(1).id()).isLessThan(responseDto.ledger().get(2).id());
    }

    private ExecutionReport createER(Long fixId, Long numericOrderId, OrderStatus status, BigDecimal leaves, BigDecimal accum) {
        BigDecimal price = new BigDecimal("15.50");
        return new ExecutionReport(
                fixId, numericOrderId, "MKT-" + numericOrderId, "GGAL", "BUY", "COMMON_STOCK",
                status, price, new BigDecimal("100.00"), leaves, accum, BigDecimal.ZERO,
                price, price, "SEC-" + fixId, "OP-" + fixId, LocalDateTime.now()
        );
    }
}
