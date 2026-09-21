package com.kafka.streams.alert.topology;

import com.kafka.streams.alert.model.AlertEvent;
import com.kafka.streams.alert.model.FraudDetectedEvent;
import com.kafka.streams.alert.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Configuration
public class FraudAlertTopology {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertTopology.class);

    @Value("${app.kafka.topics.fraud-input:fraud-detected-transactions}")
    private String fraudInputTopic;

    @Value("${app.kafka.topics.alerts-output:alerts}")
    private String alertsOutputTopic;

    @Bean
    public KStream<String, FraudDetectedEvent> buildTopology(StreamsBuilder streamsBuilder) {
        JsonSerde<FraudDetectedEvent> fraudSerde = new JsonSerde<>(FraudDetectedEvent.class);
        JsonSerde<AlertEvent> alertSerde = new JsonSerde<>(AlertEvent.class);

        KStream<String, FraudDetectedEvent> fraudStream = streamsBuilder.stream(
                fraudInputTopic,
                Consumed.with(Serdes.String(), fraudSerde)
        );

        fraudStream
                .filter((key, value) -> value != null && value.getAmount() != null)
                .peek((key, fraud) -> log.info("⚡ [KAFKA STREAMS] Consumed Fraud Event: {} | Account: {} | Amount: ${}",
                        fraud.getFraudId(), fraud.getAccountId(), fraud.getAmount()))
                .mapValues(this::createAlertFromFraud)
                .peek((key, alert) -> log.warn("🚨 [ALERT CREATED] Alert ID: {} | Fraud ID: {} | Severity: {} | Account: {}",
                        alert.getAlertId(), alert.getFraudId(), alert.getSeverity(), alert.getAccountId()))
                .to(alertsOutputTopic, Produced.with(Serdes.String(), alertSerde));

        return fraudStream;
    }

    private AlertEvent createAlertFromFraud(FraudDetectedEvent fraud) {
        String alertId = "ALT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        BigDecimal amount = fraud.getAmount();

        String severity = (fraud.getSeverity() != null && !fraud.getSeverity().isBlank())
                ? fraud.getSeverity()
                : (amount.compareTo(BigDecimal.valueOf(5000.00)) > 0 ? "CRITICAL" : "HIGH");

        List<String> actions;
        if ("CRITICAL".equalsIgnoreCase(severity)) {
            actions = List.of("FREEZE_ACCOUNT_TEMPORARILY", "DISPATCH_URGENT_SMS", "ESCALATE_TO_SECURITY_OPS");
        } else if ("HIGH".equalsIgnoreCase(severity)) {
            actions = List.of("RESTRICT_TRANSACTIONS", "SEND_PUSH_VERIFICATION", "LOG_AUDIT_TRAIL");
        } else {
            actions = List.of("FLAG_ACCOUNT_FOR_MONITORING", "SEND_EMAIL_NOTIFICATION");
        }

        String message = String.format(
                "Suspicious transaction alert for account %s: Amount %.2f %s exceeds threshold. Fraud Ref: %s",
                fraud.getAccountId(), amount, fraud.getCurrency(), fraud.getFraudId()
        );

        return new AlertEvent(
                alertId,
                fraud.getFraudId(),
                fraud.getTransactionId(),
                fraud.getAccountId(),
                amount,
                fraud.getCurrency(),
                severity,
                "HIGH_VALUE_TRANSACTION_ALERT",
                message,
                actions,
                Instant.now(),
                "ACTIVE"
        );
    }
}
