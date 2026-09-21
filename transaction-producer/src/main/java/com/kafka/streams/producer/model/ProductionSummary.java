package com.kafka.streams.producer.model;

import java.time.Instant;

public class ProductionSummary {

    private int totalRequested;
    private int totalProduced;
    private int transactionsOverThreshold;
    private double threshold;
    private long durationMs;
    private Instant completedAt;
    private String status;

    public ProductionSummary() {
    }

    public ProductionSummary(int totalRequested, int totalProduced, int transactionsOverThreshold,
                             double threshold, long durationMs, Instant completedAt, String status) {
        this.totalRequested = totalRequested;
        this.totalProduced = totalProduced;
        this.transactionsOverThreshold = transactionsOverThreshold;
        this.threshold = threshold;
        this.durationMs = durationMs;
        this.completedAt = completedAt;
        this.status = status;
    }

    public int getTotalRequested() {
        return totalRequested;
    }

    public void setTotalRequested(int totalRequested) {
        this.totalRequested = totalRequested;
    }

    public int getTotalProduced() {
        return totalProduced;
    }

    public void setTotalProduced(int totalProduced) {
        this.totalProduced = totalProduced;
    }

    public int getTransactionsOverThreshold() {
        return transactionsOverThreshold;
    }

    public void setTransactionsOverThreshold(int transactionsOverThreshold) {
        this.transactionsOverThreshold = transactionsOverThreshold;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
