package com.maxcapital.executionreports;

import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@EmbeddedKafka(
        partitions = 6,
        topics = {"execution-reports", "execution-reports.dlq", "settlement"}
)
public abstract class AbstractIntegrationTest {

    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/execution_reports";
    private static String jdbcUrl = DEFAULT_JDBC_URL;
    private static String username = "postgres";
    private static String password = "postgres";

    static {
        try {
            PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("execution_reports")
                    .withUsername("postgres")
                    .withPassword("postgres");
            postgres.start();
            jdbcUrl = postgres.getJdbcUrl();
            username = postgres.getUsername();
            password = postgres.getPassword();
            System.out.println("Testcontainers PostgreSQL started successfully at " + jdbcUrl);
        } catch (Exception e) {
            System.err.println("Testcontainers Postgres Docker unavailable: " + e.getMessage());
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> jdbcUrl);
        registry.add("spring.datasource.username", () -> username);
        registry.add("spring.datasource.password", () -> password);
    }
}
