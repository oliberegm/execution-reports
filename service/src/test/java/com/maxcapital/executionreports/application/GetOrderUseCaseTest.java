package com.maxcapital.executionreports.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.executionreports.application.dto.OrderResponseDto;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GetOrderUseCaseTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderLedgerRepository orderLedgerRepository;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private GetOrderUseCase getOrderUseCase;

    private Long numericOrderId;

    @BeforeEach
    void setUp() {
        numericOrderId = 7701L;
    }

    @Test
    @DisplayName("execute() retorna Optional.empty() cuando la orden no existe en BD")
    void execute_returnsEmpty_whenOrderDoesNotExist() {
        when(orderRepository.findByNumericOrderId(numericOrderId)).thenReturn(Optional.empty());

        Optional<OrderResponseDto> result = getOrderUseCase.execute(numericOrderId);

        assertThat(result).isEmpty();
        verify(orderRepository).findByNumericOrderId(numericOrderId);
        verifyNoInteractions(orderLedgerRepository);
    }

    @Test
    @DisplayName("execute() retorna OrderResponseDto mapeado correctamente con su lista de ledger ordenado")
    void execute_returnsOrderDto_whenOrderExists() {
        OrderEntity orderEntity = OrderEntity.builder()
                .numericOrderId(numericOrderId)
                .marketOrderId("MKT-7701")
                .ticker("GGAL")
                .side("BUY")
                .securityType("COMMON_STOCK")
                .status("FILLED")
                .orderPrice(new BigDecimal("10.00"))
                .nominalAmounts(new BigDecimal("100.00"))
                .leavesNominalAmount(BigDecimal.ZERO)
                .accumulativeNominalAmount(new BigDecimal("100.00"))
                .avgPrice(new BigDecimal("10.00"))
                .executionsAppliedCount(2)
                .lastAppliedFixId(2002L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        OrderLedgerEntity ledger1 = OrderLedgerEntity.builder()
                .id(1L)
                .numericOrderId(numericOrderId)
                .fixId(2001L)
                .status("NEW")
                .payload("{\"status\":\"NEW\"}")
                .appliedAt(LocalDateTime.now())
                .build();

        OrderLedgerEntity ledger2 = OrderLedgerEntity.builder()
                .id(2L)
                .numericOrderId(numericOrderId)
                .fixId(2002L)
                .status("FILLED")
                .payload("{\"status\":\"FILLED\"}")
                .appliedAt(LocalDateTime.now())
                .build();

        when(orderRepository.findByNumericOrderId(numericOrderId)).thenReturn(Optional.of(orderEntity));
        when(orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId)).thenReturn(List.of(ledger1, ledger2));

        Optional<OrderResponseDto> result = getOrderUseCase.execute(numericOrderId);

        assertThat(result).isPresent();
        OrderResponseDto dto = result.get();
        assertThat(dto.numericOrderId()).isEqualTo(numericOrderId);
        assertThat(dto.marketOrderId()).isEqualTo("MKT-7701");
        assertThat(dto.status()).isEqualTo("FILLED");
        assertThat(dto.executionsAppliedCount()).isEqualTo(2);
        assertThat(dto.ledger()).hasSize(2);
        assertThat(dto.ledger().get(0).fixId()).isEqualTo(2001L);
        assertThat(dto.ledger().get(1).fixId()).isEqualTo(2002L);
    }
}
