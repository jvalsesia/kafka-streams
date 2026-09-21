package com.kafka.streams.producer;

import com.kafka.streams.producer.service.TransactionProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class TransactionProducerApplication {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(TransactionProducerApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner(TransactionProducerService producerService,
                                    @Value("${app.producer.auto-start-on-launch:false}") boolean autoStart,
                                    @Value("${app.producer.default-count:1000}") int count) {
        return args -> {
            if (autoStart) {
                log.info("Auto-start enabled. Producing {} transactions immediately on startup...", count);
                producerService.produceTransactions(count);
            } else {
                log.info("Ready! Trigger transaction production via POST or GET http://localhost:8081/api/transactions/trigger?count=1000");
            }
        };
    }
}
