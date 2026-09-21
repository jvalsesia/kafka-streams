package com.kafka.streams.producer.service;

import com.kafka.streams.producer.model.Transaction;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Service
public class TransactionGeneratorService {

    private final Random random = new Random();

    private final List<String> merchants = List.of(
            "Amazon.com", "Apple Store", "Walmart", "Best Buy", "Target",
            "Delta Airlines", "Hilton Hotels", "Crypto Exchange Pro",
            "Luxury Watches Inc", "Steam Games", "Uber Rides", "Costco"
    );

    private final List<String> categories = List.of(
            "RETAIL", "ELECTRONICS", "TRAVEL", "HOSPITALITY",
            "CRYPTOCURRENCY", "LUXURY_GOODS", "ENTERTAINMENT", "TRANSPORTATION"
    );

    private final List<String> locations = List.of(
            "New York, USA", "San Francisco, USA", "London, UK", "Tokyo, Japan",
            "Paris, France", "Sydney, Australia", "Berlin, Germany", "Toronto, Canada"
    );

    public Transaction generateTransaction(int index) {
        String txId = "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        // Generate account IDs between ACC-1001 and ACC-1100
        int accountNum = 1001 + random.nextInt(100);
        String accountId = "ACC-" + accountNum;

        // Roughly 30% of transactions exceed $3,000 threshold to test fraud detection
        double rawAmount;
        if (random.nextDouble() < 0.35) {
            // Fraud suspect: amount between 3000.01 and 12500.00
            rawAmount = 3000.01 + (random.nextDouble() * 9500.0);
        } else {
            // Normal: amount between 5.00 and 2999.99
            rawAmount = 5.0 + (random.nextDouble() * 2994.99);
        }

        BigDecimal amount = BigDecimal.valueOf(rawAmount).setScale(2, RoundingMode.HALF_UP);
        String merchant = merchants.get(random.nextInt(merchants.size()));
        String category = categories.get(random.nextInt(categories.size()));
        String location = locations.get(random.nextInt(locations.size()));

        return new Transaction(
                txId,
                accountId,
                amount,
                "USD",
                Instant.now(),
                merchant,
                category,
                location
        );
    }
}
