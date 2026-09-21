use crate::models::Transaction;
use chrono::Utc;
use rand::Rng;
use uuid::Uuid;

pub struct TransactionGenerator {
    merchants: Vec<&'static str>,
    categories: Vec<&'static str>,
    locations: Vec<&'static str>,
}

impl TransactionGenerator {
    pub fn new() -> Self {
        Self {
            merchants: vec![
                "Amazon.com", "Apple Store", "Walmart", "Best Buy", "Target",
                "Delta Airlines", "Hilton Hotels", "Crypto Exchange Pro",
                "Luxury Watches Inc", "Steam Games", "Uber Rides", "Costco",
                "Rolex Boutique", "Nordstrom", "Tesla Motors"
            ],
            categories: vec![
                "RETAIL", "ELECTRONICS", "TRAVEL", "HOSPITALITY",
                "CRYPTOCURRENCY", "LUXURY_GOODS", "ENTERTAINMENT",
                "TRANSPORTATION", "AUTOMOTIVE"
            ],
            locations: vec![
                "New York, USA", "San Francisco, USA", "London, UK", "Tokyo, Japan",
                "Paris, France", "Sydney, Australia", "Berlin, Germany", "Toronto, Canada",
                "Singapore", "Dubai, UAE"
            ],
        }
    }

    pub fn generate(&self) -> Transaction {
        let mut rng = rand::thread_rng();

        let tx_id = format!("TX-{}", &Uuid::new_v4().to_string()[..8].to_uppercase());
        let account_id = format!("ACC-{}", rng.gen_range(1001..=1100));

        // Roughly 35% chance of transaction exceeding $3,000 fraud threshold
        let raw_amount: f64 = if rng.gen_bool(0.35) {
            // High value transaction: $3,000.01 - $12,500.00
            rng.gen_range(3000.01..12500.0)
        } else {
            // Normal transaction: $5.00 - $2,999.99
            rng.gen_range(5.0..2999.99)
        };

        let amount = (raw_amount * 100.0).round() / 100.0;
        let merchant = self.merchants[rng.gen_range(0..self.merchants.len())];
        let category = self.categories[rng.gen_range(0..self.categories.len())];
        let location = self.locations[rng.gen_range(0..self.locations.len())];

        Transaction {
            transaction_id: tx_id,
            account_id,
            amount,
            currency: "USD".to_string(),
            timestamp: Utc::now().to_rfc3339(),
            merchant: merchant.to_string(),
            category: category.to_string(),
            location: location.to_string(),
        }
    }
}
