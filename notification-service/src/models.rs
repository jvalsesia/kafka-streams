use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertEvent {
    pub alert_id: String,
    pub fraud_id: String,
    #[serde(default)]
    pub transaction_id: Option<String>,
    pub account_id: String,
    pub amount: f64,
    pub currency: String,
    pub severity: String,
    #[serde(default)]
    pub alert_type: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub recommended_actions: Option<Vec<String>>,
    #[serde(default)]
    pub triggered_at: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationEvent {
    pub notification_id: String,
    pub alert_id: String,
    pub fraud_id: String,
    pub account_id: String,
    pub amount: f64,
    pub currency: String,
    pub channels: Vec<String>,
    pub subject: String,
    pub message: String,
    pub dispatched_at: String,
    pub delivery_status: String,
    pub urgency: String,
}
