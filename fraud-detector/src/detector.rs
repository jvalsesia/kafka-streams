use crate::models::{FraudDetectedEvent, Transaction};
use chrono::Utc;
use uuid::Uuid;

pub struct FraudDetector {
    threshold: f64,
}

impl FraudDetector {
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }

    pub fn evaluate(&self, tx: &Transaction) -> Option<FraudDetectedEvent> {
        if tx.amount > self.threshold {
            let severity = if tx.amount > 10000.0 {
                "CRITICAL"
            } else if tx.amount > 5000.0 {
                "HIGH"
            } else {
                "MEDIUM"
            };

            Some(FraudDetectedEvent {
                fraud_id: format!("FRD-{}", &Uuid::new_v4().to_string()[..8].to_uppercase()),
                transaction_id: tx.transaction_id.clone(),
                account_id: tx.account_id.clone(),
                amount: tx.amount,
                currency: tx.currency.clone(),
                timestamp: tx.timestamp.clone(),
                merchant: tx.merchant.clone(),
                category: tx.category.clone(),
                location: tx.location.clone(),
                detected_at: Utc::now().to_rfc3339(),
                reason: format!(
                    "Transaction amount {:.2} {} exceeds threshold of {:.2} {}",
                    tx.amount, tx.currency, self.threshold, tx.currency
                ),
                severity: severity.to_string(),
            })
        } else {
            None
        }
    }
}
