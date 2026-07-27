package com.maxcapital.executionreports.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.executionreports.domain.ApplyResult;
import com.maxcapital.executionreports.domain.ExecutionReport;
import com.maxcapital.executionreports.domain.Order;
import com.maxcapital.executionreports.domain.OrderStateMachine;
import com.maxcapital.executionreports.infrastructure.persistence.OrderEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerEntity;
import com.maxcapital.executionreports.infrastructure.persistence.OrderLedgerRepository;
import com.maxcapital.executionreports.infrastructure.persistence.OrderRepository;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxEntity;
import com.maxcapital.executionreports.infrastructure.persistence.SettlementOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionReportProcessor {

    private final OrderRepository orderRepository;
    private final OrderLedgerRepository orderLedgerRepository;
    private final SettlementOutboxRepository settlementOutboxRepository;
    private final ObjectMapper objectMapper;
    private final OrderStateMachine stateMachine = new OrderStateMachine();

    @Transactional
    public ApplyResult process(ExecutionReport er) {
        if (orderLedgerRepository.existsByFixId(er.fixId())) {
            log.info("ExecutionReport fixId={} already processed for numericOrderId={}. Skipping.",
                    er.fixId(), er.numericOrderId());
            return null;
        }

        Optional<OrderEntity> orderEntityOpt = orderRepository.findByNumericOrderIdWithLock(er.numericOrderId());
        Order currentOrder = orderEntityOpt.map(OrderEntityMapper::toDomain).orElse(null);

        ApplyResult result = stateMachine.apply(currentOrder, er);

        switch (result) {
            case ApplyResult.Success success -> {
                Order updatedOrder = success.order();
                OrderEntity entityToSave = OrderEntityMapper.toEntity(updatedOrder, orderEntityOpt.orElse(null));

                try {
                    orderRepository.saveAndFlush(entityToSave);
                } catch (DataIntegrityViolationException e) {
                    log.info("Concurrent insert caught on orders for numericOrderId={}. Transaction will rollback.", er.numericOrderId());
                    throw e;
                }

                String erPayloadJson = toJson(er);
                OrderLedgerEntity ledgerEntity = OrderLedgerEntity.builder()
                        .numericOrderId(er.numericOrderId())
                        .fixId(er.fixId())
                        .status(er.status().name())
                        .payload(erPayloadJson)
                        .appliedAt(LocalDateTime.now())
                        .build();

                try {
                    orderLedgerRepository.saveAndFlush(ledgerEntity);
                } catch (DataIntegrityViolationException e) {
                    log.info("Duplicate fixId={} caught on ledger insert for numericOrderId={}. Transaction will rollback.",
                            er.fixId(), er.numericOrderId());
                    throw e;
                }

                if (success.settlementRequired()) {
                    String settlementPayloadJson = toJson(updatedOrder);
                    SettlementOutboxEntity outboxEntity = SettlementOutboxEntity.builder()
                            .numericOrderId(updatedOrder.numericOrderId())
                            .payload(settlementPayloadJson)
                            .status("PENDING")
                            .createdAt(LocalDateTime.now())
                            .build();
                    settlementOutboxRepository.save(outboxEntity);
                }
            }
            case ApplyResult.Rejection rejection -> {
                log.warn("ExecutionReport fixId={} rejected for numericOrderId={}: [{}] {}",
                        er.fixId(), er.numericOrderId(), rejection.reason(), rejection.message());
            }
        }

        return result;
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize object to JSON", e);
        }
    }
}
