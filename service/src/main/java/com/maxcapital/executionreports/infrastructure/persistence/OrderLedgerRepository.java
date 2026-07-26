package com.maxcapital.executionreports.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OrderLedgerRepository extends JpaRepository<OrderLedgerEntity, Long> {

    List<OrderLedgerEntity> findByNumericOrderIdOrderByIdAsc(Long numericOrderId);

    boolean existsByFixId(Long fixId);

    Optional<OrderLedgerEntity> findByFixId(Long fixId);
}
