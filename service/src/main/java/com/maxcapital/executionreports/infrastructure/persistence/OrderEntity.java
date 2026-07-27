package com.maxcapital.executionreports.infrastructure.persistence;

import com.maxcapital.executionreports.domain.Order;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders")
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderEntity {

    @Id
    @Column(name = "numeric_order_id", nullable = false)
    private Long numericOrderId;

    @Column(name = "market_order_id")
    private String marketOrderId;

    @Column(name = "ticker")
    private String ticker;

    @Column(name = "side")
    private String side;

    @Column(name = "security_type")
    private String securityType;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "order_price", precision = 18, scale = 6)
    private BigDecimal orderPrice;

    @Column(name = "nominal_amounts", precision = 18, scale = 6)
    private BigDecimal nominalAmounts;

    @Column(name = "leaves_nominal_amount", precision = 18, scale = 6)
    private BigDecimal leavesNominalAmount;

    @Column(name = "accumulative_nominal_amount", precision = 18, scale = 6)
    private BigDecimal accumulativeNominalAmount;

    @Column(name = "avg_price", precision = 18, scale = 6)
    private BigDecimal avgPrice;

    @Column(name = "executions_applied_count", nullable = false)
    private Integer executionsAppliedCount;

    @Column(name = "last_applied_fix_id")
    private Long lastAppliedFixId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public void updateState(Order order) {
        this.marketOrderId = order.marketOrderId();
        this.ticker = order.ticker();
        this.side = order.side();
        this.securityType = order.securityType();
        this.status = order.status().name();
        this.orderPrice = order.orderPrice();
        this.nominalAmounts = order.nominalAmounts();
        this.leavesNominalAmount = order.leavesNominalAmount();
        this.accumulativeNominalAmount = order.accumulativeNominalAmount();
        this.avgPrice = order.avgPrice();
        this.executionsAppliedCount = order.executionsAppliedCount();
        this.lastAppliedFixId = order.lastAppliedFixId();
        this.updatedAt = LocalDateTime.now();
    }
}
