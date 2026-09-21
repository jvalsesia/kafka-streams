use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub transaction_id: String,
    pub account_id: String,
    pub amount: f64,
    pub currency: String,
    pub timestamp: String,
    pub merchant: String,
    pub category: String,
    pub location: String,
}
