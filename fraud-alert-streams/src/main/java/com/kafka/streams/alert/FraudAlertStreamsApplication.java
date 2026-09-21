package com.kafka.streams.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FraudAlertStreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertStreamsApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(FraudAlertStreamsApplication.class, args);
        log.info("Fraud Alert Kafka Streams consumer is running!");
    }
}
