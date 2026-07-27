package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.application.RelaySettlementUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final RelaySettlementUseCase relaySettlementUseCase;

    @Scheduled(fixedDelayString = "${app.outbox.relay.fixed-delay-ms:1000}")
    public void pollAndRelay() {
        relaySettlementUseCase.execute();
    }
}
