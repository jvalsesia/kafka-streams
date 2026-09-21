use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub alerts_topic: String,
    pub notifications_topic: String,
    pub group_id: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            kafka_brokers: env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            alerts_topic: env::var("ALERTS_TOPIC")
                .unwrap_or_else(|_| "alerts".to_string()),
            notifications_topic: env::var("NOTIFICATIONS_TOPIC")
                .unwrap_or_else(|_| "notifications".to_string()),
            group_id: env::var("GROUP_ID")
                .unwrap_or_else(|_| "rust-notification-group".to_string()),
        }
    }
}
