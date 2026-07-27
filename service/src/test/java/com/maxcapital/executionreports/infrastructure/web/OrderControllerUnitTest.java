package com.maxcapital.executionreports.infrastructure.web;

import com.maxcapital.executionreports.application.GetOrderUseCase;
import com.maxcapital.executionreports.application.dto.OrderLedgerResponseDto;
import com.maxcapital.executionreports.application.dto.OrderResponseDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private GetOrderUseCase getOrderUseCase;

    @Test
    @DisplayName("GET /orders/{numericOrderId} cuando existe retorna 200 OK con datos de orden y ledger")
    void getOrder_returns200OK_whenOrderExists() throws Exception {
        Long numericOrderId = 1234L;
        OrderLedgerResponseDto ledgerItem = new OrderLedgerResponseDto(
                1L, numericOrderId, 5001L, "NEW", "{}", LocalDateTime.now()
        );
        OrderResponseDto dto = new OrderResponseDto(
                numericOrderId, "MKT-1234", "VSCPC", "BUY", "COMMON_STOCK", "NEW",
                new BigDecimal("10.00"), new BigDecimal("100.00"), new BigDecimal("100.00"), BigDecimal.ZERO,
                new BigDecimal("10.00"), 1, 5001L, LocalDateTime.now(), LocalDateTime.now(),
                List.of(ledgerItem)
        );

        when(getOrderUseCase.execute(numericOrderId)).thenReturn(Optional.of(dto));

        mockMvc.perform(get("/orders/{numericOrderId}", numericOrderId)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.numericOrderId").value(1234))
                .andExpect(jsonPath("$.marketOrderId").value("MKT-1234"))
                .andExpect(jsonPath("$.status").value("NEW"))
                .andExpect(jsonPath("$.executionsAppliedCount").value(1))
                .andExpect(jsonPath("$.ledger").isArray())
                .andExpect(jsonPath("$.ledger[0].fixId").value(5001));
    }

    @Test
    @DisplayName("GET /orders/{numericOrderId} cuando no existe retorna 404 NOT FOUND")
    void getOrder_returns404NotFound_whenOrderDoesNotExist() throws Exception {
        Long numericOrderId = 9999L;
        when(getOrderUseCase.execute(numericOrderId)).thenReturn(Optional.empty());

        mockMvc.perform(get("/orders/{numericOrderId}", numericOrderId)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
    }
}
