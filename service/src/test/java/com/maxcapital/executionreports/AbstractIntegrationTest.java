package com.maxcapital.executionreports;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

public abstract class AbstractIntegrationTest {

    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/execution_reports";
    private static String jdbcUrl = DEFAULT_JDBC_URL;
    private static String username = "postgres";
    private static String password = "postgres";

    static {
        try {
            PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("execution_reports")
                    .withUsername("postgres")
                    .withPassword("postgres");
            container.start();
            jdbcUrl = container.getJdbcUrl();
            username = container.getUsername();
            password = container.getPassword();
            System.out.println("Testcontainers PostgreSQL started successfully at " + jdbcUrl);
        } catch (Exception e) {
            System.err.println("Testcontainers Docker unavailable. Falling back to local PostgreSQL at " + DEFAULT_JDBC_URL + ": " + e.getMessage());
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> jdbcUrl);
        registry.add("spring.datasource.username", () -> username);
        registry.add("spring.datasource.password", () -> password);
    }
}
