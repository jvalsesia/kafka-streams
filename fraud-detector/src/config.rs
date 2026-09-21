use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub transactions_topic: String,
    pub fraud_topic: String,
    pub group_id: String,
    pub fraud_threshold: f64,
}

impl AppConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            kafka_brokers: env::var("KAFKA_BOOTSTRAP_SERVERS")
                .unwrap_or_else(|_| "localhost:9092".to_string()),
            transactions_topic: env::var("TRANSACTIONS_TOPIC")
                .unwrap_or_else(|_| "transactions".to_string()),
            fraud_topic: env::var("FRAUD_TOPIC")
                .unwrap_or_else(|_| "fraud-detected-transactions".to_string()),
            group_id: env::var("GROUP_ID")
                .unwrap_or_else(|_| "fraud-detector-group".to_string()),
            fraud_threshold: env::var("FRAUD_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(3000.0),
        }
    }
}
