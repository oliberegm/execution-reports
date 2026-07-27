package com.maxcapital.executionreports.application;

import com.maxcapital.executionreports.domain.Order;
import com.maxcapital.executionreports.domain.OrderStatus;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;

import java.time.LocalDateTime;

public class OrderEntityMapper {

    private OrderEntityMapper() {
    }

    public static Order toDomain(OrderEntity entity) {
        if (entity == null) {
            return null;
        }
        return new Order(
                entity.getNumericOrderId(),
                entity.getMarketOrderId(),
                entity.getTicker(),
                entity.getSide(),
                entity.getSecurityType(),
                OrderStatus.valueOf(entity.getStatus()),
                entity.getOrderPrice(),
                entity.getNominalAmounts(),
                entity.getLeavesNominalAmount(),
                entity.getAccumulativeNominalAmount(),
                entity.getAvgPrice(),
                entity.getExecutionsAppliedCount(),
                entity.getLastAppliedFixId()
        );
    }

    public static OrderEntity toEntity(Order order, OrderEntity existingEntity) {
        if (existingEntity != null) {
            existingEntity.updateState(order);
            return existingEntity;
        }
        LocalDateTime now = LocalDateTime.now();
        return OrderEntity.builder()
                .numericOrderId(order.numericOrderId())
                .marketOrderId(order.marketOrderId())
                .ticker(order.ticker())
                .side(order.side())
                .securityType(order.securityType())
                .status(order.status().name())
                .orderPrice(order.orderPrice())
                .nominalAmounts(order.nominalAmounts())
                .leavesNominalAmount(order.leavesNominalAmount())
                .accumulativeNominalAmount(order.accumulativeNominalAmount())
                .avgPrice(order.avgPrice())
                .executionsAppliedCount(order.executionsAppliedCount())
                .lastAppliedFixId(order.lastAppliedFixId())
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
