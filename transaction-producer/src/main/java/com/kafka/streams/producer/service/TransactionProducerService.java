package com.kafka.streams.producer.service;

import com.kafka.streams.producer.model.ProductionSummary;
import com.kafka.streams.producer.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class TransactionProducerService {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducerService.class);
    private static final BigDecimal FRAUD_THRESHOLD = BigDecimal.valueOf(3000.00);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionGeneratorService generatorService;

    @Value("${app.kafka.topics.transactions:transactions}")
    private String transactionsTopic;

    public TransactionProducerService(KafkaTemplate<String, Object> kafkaTemplate,
                                      TransactionGeneratorService generatorService) {
        this.kafkaTemplate = kafkaTemplate;
        this.generatorService = generatorService;
    }

    public ProductionSummary produceTransactions(int count) {
        long startTime = System.currentTimeMillis();
        log.info("Starting production of {} transactions to topic '{}'...", count, transactionsTopic);

        AtomicInteger successCounter = new AtomicInteger(0);
        AtomicInteger fraudCandidateCounter = new AtomicInteger(0);
        List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>(count);

        for (int i = 1; i <= count; i++) {
            Transaction tx = generatorService.generateTransaction(i);
            if (tx.getAmount().compareTo(FRAUD_THRESHOLD) > 0) {
                fraudCandidateCounter.incrementAndGet();
            }

            // Partition key: accountId ensures sequential ordering per account
            String key = tx.getAccountId();
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(transactionsTopic, key, tx);
            futures.add(future);

            final int currentIdx = i;
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to deliver transaction #{} [{}]: {}", currentIdx, tx.getTransactionId(), ex.getMessage());
                } else {
                    int sent = successCounter.incrementAndGet();
                    if (sent % 100 == 0 || sent == count) {
                        log.info("Progress: Sent {}/{} transactions (offset: {}, partition: {})",
                                sent, count,
                                result.getRecordMetadata().offset(),
                                result.getRecordMetadata().partition());
                    }
                }
            });
        }

        // Wait for all messages to be acknowledged by Kafka brokers
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long duration = System.currentTimeMillis() - startTime;
        log.info("Completed batch production of {} transactions in {} ms. Suspected fraud (> 3000): {}",
                count, duration, fraudCandidateCounter.get());

        return new ProductionSummary(
                count,
                successCounter.get(),
                fraudCandidateCounter.get(),
                FRAUD_THRESHOLD.doubleValue(),
                duration,
                Instant.now(),
                "SUCCESS"
        );
    }
}
