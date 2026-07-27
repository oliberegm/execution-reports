package com.maxcapital.executionreports;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ExecutionReportsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExecutionReportsApplication.class, args);
    }
}
