package com.kafka.streams.alert.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FraudDetectedEvent implements Serializable {

    private String fraudId;
    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String timestamp;
    private String merchant;
    private String category;
    private String location;
    private String detectedAt;
    private String reason;
    private String severity;

    public FraudDetectedEvent() {
    }

    public FraudDetectedEvent(String fraudId, String transactionId, String accountId, BigDecimal amount,
                              String currency, String timestamp, String merchant, String category,
                              String location, String detectedAt, String reason, String severity) {
        this.fraudId = fraudId;
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.timestamp = timestamp;
        this.merchant = merchant;
        this.category = category;
        this.location = location;
        this.detectedAt = detectedAt;
        this.reason = reason;
        this.severity = severity;
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

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDetectedAt() {
        return detectedAt;
    }

    public void setDetectedAt(String detectedAt) {
        this.detectedAt = detectedAt;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    @Override
    public String toString() {
        return "FraudDetectedEvent{" +
                "fraudId='" + fraudId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", severity='" + severity + '\'' +
                '}';
    }
}
