package com.maxcapital.executionreports.infrastructure.web;

import com.maxcapital.executionreports.application.GetOrderUseCase;
import com.maxcapital.executionreports.application.dto.OrderResponseDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final GetOrderUseCase getOrderUseCase;

    @GetMapping("/{numericOrderId}")
    public ResponseEntity<OrderResponseDto> getOrder(@PathVariable Long numericOrderId) {
        return getOrderUseCase.execute(numericOrderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
