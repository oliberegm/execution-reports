package com.maxcapital.executionreports.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.executionreports.application.dto.OrderLedgerResponseDto;
import com.maxcapital.executionreports.application.dto.OrderResponseDto;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class GetOrderUseCase {

    private final OrderRepository orderRepository;
    private final OrderLedgerRepository orderLedgerRepository;
    private final ObjectMapper objectMapper;

    @Transactional(readOnly = true)
    public Optional<OrderResponseDto> execute(Long numericOrderId) {
        Optional<OrderEntity> orderEntityOpt = orderRepository.findByNumericOrderId(numericOrderId);
        if (orderEntityOpt.isEmpty()) {
            return Optional.empty();
        }

        OrderEntity order = orderEntityOpt.get();
        List<OrderLedgerEntity> ledgerEntities = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);

        return Optional.of(toOrderResponseDto(order, ledgerEntities));
    }

    private OrderResponseDto toOrderResponseDto(OrderEntity order, List<OrderLedgerEntity> ledgerEntities) {
        List<OrderLedgerResponseDto> ledgerDtos = ledgerEntities.stream()
                .map(this::toLedgerDto)
                .toList();

        return new OrderResponseDto(
                order.getNumericOrderId(),
                order.getMarketOrderId(),
                order.getTicker(),
                order.getSide(),
                order.getSecurityType(),
                order.getStatus(),
                order.getOrderPrice(),
                order.getNominalAmounts(),
                order.getLeavesNominalAmount(),
                order.getAccumulativeNominalAmount(),
                order.getAvgPrice(),
                order.getExecutionsAppliedCount(),
                order.getLastAppliedFixId(),
                order.getCreatedAt(),
                order.getUpdatedAt(),
                ledgerDtos
        );
    }

    private OrderLedgerResponseDto toLedgerDto(OrderLedgerEntity entity) {
        Object payloadObject;
        try {
            payloadObject = objectMapper.readTree(entity.getPayload());
        } catch (Exception e) {
            payloadObject = entity.getPayload();
        }

        return new OrderLedgerResponseDto(
                entity.getId(),
                entity.getNumericOrderId(),
                entity.getFixId(),
                entity.getStatus(),
                payloadObject,
                entity.getAppliedAt()
        );
    }
}
