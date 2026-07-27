package com.maxcapital.executionreports.infrastructure.kafka;

import com.maxcapital.executionreports.application.ExecutionReportProcessor;
import com.maxcapital.executionreports.domain.ApplyResult;
import com.maxcapital.executionreports.domain.ExecutionReport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionReportListener {

    private final ExecutionReportProcessor processor;

    @KafkaListener(
            topics = "${app.kafka.topics.execution-reports:execution-reports}",
            groupId = "${spring.kafka.consumer.group-id:execution-reports-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, ExecutionReport> record, Acknowledgment ack) {
        if (record.value() == null) {
            log.error("Received null ExecutionReport record at offset {} (possible deserialization error). Throwing exception for DLQ recovery.", record.offset());
            throw new DeserializationException("Deserialization failed for payload at offset " + record.offset(), null, false, null);
        }

        ExecutionReport er = record.value();
        log.debug("Received ExecutionReport fixId={} for numericOrderId={} from partition={} offset={}",
                er.fixId(), er.numericOrderId(), record.partition(), record.offset());

        ApplyResult result = processor.process(er);

        switch (result) {
            case ApplyResult.Success success ->
                    log.info("ExecutionReport fixId={} processed successfully. Order status={}",
                            er.fixId(), success.order().status());
            case ApplyResult.Rejection rejection ->
                    log.warn("ExecutionReport fixId={} rejected: [{}] {}",
                            er.fixId(), rejection.reason(), rejection.message());
            case ApplyResult.AlreadyProcessed alreadyProcessed ->
                    log.info("ExecutionReport fixId={} already processed previously.", er.fixId());
        }

        ack.acknowledge();
    }
}
