package com.maxcapital.executionreports.infrastructure.persistence;

import com.maxcapital.executionreports.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class PersistenceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderLedgerRepository orderLedgerRepository;

    @Autowired
    private SettlementOutboxRepository settlementOutboxRepository;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        settlementOutboxRepository.deleteAll();
        orderLedgerRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("Insert order and multiple ledger entries with distinct fix_id succeeds")
    void insertOrderAndLedgerEntries_success() {
        Long numericOrderId = 13144742L;

        OrderEntity order = OrderEntity.builder()
                .numericOrderId(numericOrderId)
                .marketOrderId("O0S6tDQoQqVy")
                .ticker("VSCPC")
                .side("BUY")
                .securityType("COMMON_STOCK")
                .status("NEW")
                .orderPrice(new BigDecimal("104.25"))
                .nominalAmounts(new BigDecimal("4956"))
                .leavesNominalAmount(new BigDecimal("4956"))
                .accumulativeNominalAmount(BigDecimal.ZERO)
                .avgPrice(BigDecimal.ZERO)
                .executionsAppliedCount(1)
                .lastAppliedFixId(523130930000301L)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        orderRepository.save(order);

        OrderLedgerEntity ledger1 = OrderLedgerEntity.builder()
                .numericOrderId(numericOrderId)
                .fixId(523130930000301L)
                .status("NEW")
                .payload("{\"fixId\":523130930000301}")
                .appliedAt(LocalDateTime.now())
                .build();

        OrderLedgerEntity ledger2 = OrderLedgerEntity.builder()
                .numericOrderId(numericOrderId)
                .fixId(523130930000302L)
                .status("PARTIALLY_FILLED")
                .payload("{\"fixId\":523130930000302}")
                .appliedAt(LocalDateTime.now())
                .build();

        orderLedgerRepository.save(ledger1);
        orderLedgerRepository.save(ledger2);

        Optional<OrderEntity> retrievedOrder = orderRepository.findByNumericOrderId(numericOrderId);
        assertThat(retrievedOrder).isPresent();
        assertThat(retrievedOrder.get().getStatus()).isEqualTo("NEW");

        List<OrderLedgerEntity> ledgerEntries = orderLedgerRepository.findByNumericOrderIdOrderByIdAsc(numericOrderId);
        assertThat(ledgerEntries).hasSize(2);
        assertThat(ledgerEntries.get(0).getFixId()).isEqualTo(523130930000301L);
        assertThat(ledgerEntries.get(1).getFixId()).isEqualTo(523130930000302L);
    }

    @Test
    @DisplayName("Insert two ledger entries with duplicate fix_id throws DataIntegrityViolationException")
    void duplicateFixId_throwsDataIntegrityViolationException() {
        Long numericOrderId = 99990001L;
        Long duplicateFixId = 77770001L;

        OrderEntity order = OrderEntity.builder()
                .numericOrderId(numericOrderId)
                .status("NEW")
                .executionsAppliedCount(1)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
        orderRepository.save(order);

        OrderLedgerEntity ledger1 = OrderLedgerEntity.builder()
                .numericOrderId(numericOrderId)
                .fixId(duplicateFixId)
                .status("NEW")
                .payload("{}")
                .appliedAt(LocalDateTime.now())
                .build();
        orderLedgerRepository.saveAndFlush(ledger1);

        OrderLedgerEntity ledger2 = OrderLedgerEntity.builder()
                .numericOrderId(numericOrderId)
                .fixId(duplicateFixId)
                .status("PARTIALLY_FILLED")
                .payload("{}")
                .appliedAt(LocalDateTime.now())
                .build();

        assertThatThrownBy(() -> orderLedgerRepository.saveAndFlush(ledger2))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @Transactional
    @DisplayName("Pessimistic lock query findByNumericOrderIdWithLock executes successfully")
    void pessimisticLockQuery_executesSuccessfully() {
        Long numericOrderId = 88880001L;

        OrderEntity order = OrderEntity.builder()
                .numericOrderId(numericOrderId)
                .status("NEW")
                .executionsAppliedCount(1)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
        orderRepository.save(order);

        Optional<OrderEntity> lockedOrder = orderRepository.findByNumericOrderIdWithLock(numericOrderId);
        assertThat(lockedOrder).isPresent();
        assertThat(lockedOrder.get().getNumericOrderId()).isEqualTo(numericOrderId);
    }

    @Test
    @Transactional
    @DisplayName("Outbox findPendingForUpdateSkipLocked query fetches pending rows")
    void outboxSkipLockedQuery_executesSuccessfully() {
        SettlementOutboxEntity outbox1 = SettlementOutboxEntity.builder()
                .numericOrderId(1111L)
                .payload("{\"status\":\"FILLED\"}")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        SettlementOutboxEntity outbox2 = SettlementOutboxEntity.builder()
                .numericOrderId(2222L)
                .payload("{\"status\":\"FILLED\"}")
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .build();

        settlementOutboxRepository.save(outbox1);
        settlementOutboxRepository.save(outbox2);

        List<SettlementOutboxEntity> pending = settlementOutboxRepository.findPendingForUpdateSkipLocked(10);
        assertThat(pending).hasSizeGreaterThanOrEqualTo(2);
    }
}
