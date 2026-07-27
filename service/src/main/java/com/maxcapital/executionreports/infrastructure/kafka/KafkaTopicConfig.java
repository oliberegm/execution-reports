package com.maxcapital.executionreports.infrastructure.kafka;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.execution-reports:execution-reports}")
    private String executionReportsTopic;

    @Value("${app.kafka.topics.execution-reports-dlq:execution-reports.dlq}")
    private String executionReportsDlqTopic;

    @Value("${app.kafka.topics.settlement:settlement}")
    private String settlementTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic executionReportsTopic() {
        return TopicBuilder.name(executionReportsTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic executionReportsDlqTopic() {
        return TopicBuilder.name(executionReportsDlqTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic settlementTopic() {
        return TopicBuilder.name(settlementTopic)
                .partitions(6)
                .replicas(1)
                .build();
    }
}
