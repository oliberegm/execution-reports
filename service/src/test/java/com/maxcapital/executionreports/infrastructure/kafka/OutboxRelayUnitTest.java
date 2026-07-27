package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.application.RelaySettlementUseCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxRelayUnitTest {

    @Mock
    private RelaySettlementUseCase relaySettlementUseCase;

    @InjectMocks
    private OutboxRelay outboxRelay;

    @Test
    @DisplayName("Gatillo de scheduler delega la ejecucion en RelaySettlementUseCase")
    void pollAndRelay_delegatesToUseCase() {
        outboxRelay.pollAndRelay();

        verify(relaySettlementUseCase).execute();
    }
}
