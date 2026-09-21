use crate::models::{AlertEvent, NotificationEvent};
use chrono::Utc;
use uuid::Uuid;

pub struct NotificationDispatcher;

impl NotificationDispatcher {
    pub fn new() -> Self {
        Self
    }

    pub fn dispatch(&self, alert: &AlertEvent) -> NotificationEvent {
        let (channels, urgency) = match alert.severity.to_uppercase().as_str() {
            "CRITICAL" => (
                vec![
                    "SMS_PRIORITY_GATEWAY".to_string(),
                    "MOBILE_PUSH_URGENT".to_string(),
                    "EMAIL_HIGH_PRIORITY".to_string(),
                    "SECURITY_DESK_PAGER".to_string(),
                ],
                "IMMEDIATE".to_string(),
            ),
            "HIGH" => (
                vec![
                    "SMS_GATEWAY".to_string(),
                    "MOBILE_PUSH".to_string(),
                    "EMAIL".to_string(),
                ],
                "HIGH".to_string(),
            ),
            _ => (
                vec![
                    "MOBILE_PUSH".to_string(),
                    "EMAIL_NOTIFICATION".to_string(),
                ],
                "NORMAL".to_string(),
            ),
        };

        let subject = format!(
            "[{}] Security Alert: Unusual Activity Detected on Account {}",
            urgency, alert.account_id
        );

        let actions_str = alert
            .recommended_actions
            .as_ref()
            .map(|a| a.join(", "))
            .unwrap_or_else(|| "Review transaction details".to_string());

        let message = format!(
            "Dear Customer, a transaction of {:.2} {} was flagged under Alert {}. Suggested actions: {}. Please verify immediately.",
            alert.amount, alert.currency, alert.alert_id, actions_str
        );

        NotificationEvent {
            notification_id: format!("NOTIF-{}", &Uuid::new_v4().to_string()[..8].to_uppercase()),
            alert_id: alert.alert_id.clone(),
            fraud_id: alert.fraud_id.clone(),
            account_id: alert.account_id.clone(),
            amount: alert.amount,
            currency: alert.currency.clone(),
            channels,
            subject,
            message,
            dispatched_at: Utc::now().to_rfc3339(),
            delivery_status: "SENT".to_string(),
            urgency,
        }
    }
}
