package com.maxcapital.executionreports.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SettlementOutboxRepository extends JpaRepository<SettlementOutboxEntity, Long> {

    @Query(value = "SELECT * FROM settlement_outbox WHERE status = 'PENDING' ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<SettlementOutboxEntity> findPendingForUpdateSkipLocked(@Param("limit") int limit);
}
