package com.example.fraud;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FraudIngestionApplication {
    public static void main(String[] args) { SpringApplication.run(FraudIngestionApplication.class, args); }
}
