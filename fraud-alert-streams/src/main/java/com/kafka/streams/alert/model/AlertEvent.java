package com.kafka.streams.alert.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class AlertEvent implements Serializable {

    private String alertId;
    private String fraudId;
    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String severity;
    private String alertType;
    private String message;
    private List<String> recommendedActions;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant triggeredAt;

    private String status;

    public AlertEvent() {
    }

    public AlertEvent(String alertId, String fraudId, String transactionId, String accountId,
                      BigDecimal amount, String currency, String severity, String alertType,
                      String message, List<String> recommendedActions, Instant triggeredAt, String status) {
        this.alertId = alertId;
        this.fraudId = fraudId;
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.severity = severity;
        this.alertType = alertType;
        this.message = message;
        this.recommendedActions = recommendedActions;
        this.triggeredAt = triggeredAt;
        this.status = status;
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getFraudId() {
        return fraudId;
    }

    public void setFraudId(String fraudId) {
        this.fraudId = fraudId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getAlertType() {
        return alertType;
    }

    public void setAlertType(String alertType) {
        this.alertType = alertType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getRecommendedActions() {
        return recommendedActions;
    }

    public void setRecommendedActions(List<String> recommendedActions) {
        this.recommendedActions = recommendedActions;
    }

    public Instant getTriggeredAt() {
        return triggeredAt;
    }

    public void setTriggeredAt(Instant triggeredAt) {
        this.triggeredAt = triggeredAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "AlertEvent{" +
                "alertId='" + alertId + '\'' +
                ", fraudId='" + fraudId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", severity='" + severity + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
