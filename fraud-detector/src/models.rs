use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_id: String,
    pub account_id: String,
    pub amount: f64,
    pub currency: String,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub merchant: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FraudDetectedEvent {
    pub fraud_id: String,
    pub transaction_id: String,
    pub account_id: String,
    pub amount: f64,
    pub currency: String,
    pub timestamp: Option<String>,
    pub merchant: Option<String>,
    pub category: Option<String>,
    pub location: Option<String>,
    pub detected_at: String,
    pub reason: String,
    pub severity: String,
}
