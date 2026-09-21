package com.kafka.streams.producer.controller;

import com.kafka.streams.producer.model.ProductionSummary;
import com.kafka.streams.producer.service.TransactionProducerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/transactions")
public class TransactionController {

    private final TransactionProducerService producerService;

    @Value("${app.producer.default-count:1000}")
    private int defaultCount;

    @Value("${app.kafka.topics.transactions:transactions}")
    private String transactionsTopic;

    public TransactionController(TransactionProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/trigger")
    public ResponseEntity<ProductionSummary> triggerPost(
            @RequestParam(name = "count", required = false) Integer count) {
        int targetCount = (count != null && count > 0) ? count : defaultCount;
        ProductionSummary summary = producerService.produceTransactions(targetCount);
        return ResponseEntity.ok(summary);
    }

    @GetMapping("/trigger")
    public ResponseEntity<ProductionSummary> triggerGet(
            @RequestParam(name = "count", required = false) Integer count) {
        int targetCount = (count != null && count > 0) ? count : defaultCount;
        ProductionSummary summary = producerService.produceTransactions(targetCount);
        return ResponseEntity.ok(summary);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "service", "transaction-producer",
                "status", "UP",
                "targetTopic", transactionsTopic,
                "defaultBatchCount", defaultCount
        ));
    }
}
