mod config;
mod detector;
mod models;

use config::AppConfig;
use detector::FraudDetector;
use models::Transaction;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = AppConfig::from_env();
    info!("==================================================");
    info!("🚀 Starting Rust Fraud Detector Service");
    info!("Kafka Brokers       : {}", config.kafka_brokers);
    info!("Source Topic        : {}", config.transactions_topic);
    info!("Fraud Output Topic  : {}", config.fraud_topic);
    info!("Consumer Group ID   : {}", config.group_id);
    info!("Fraud Threshold     : > {:.2}", config.fraud_threshold);
    info!("==================================================");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("Failed to create Kafka consumer");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    consumer
        .subscribe(&[&config.transactions_topic])
        .expect("Failed to subscribe to transactions topic");

    let detector = FraudDetector::new(config.fraud_threshold);

    let mut total_transactions: u64 = 0;
    let mut total_frauds: u64 = 0;

    info!("Listening for transaction events on '{}'...", config.transactions_topic);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received. Exiting Fraud Detector...");
                break;
            }
            msg_res = consumer.recv() => {
                match msg_res {
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                warn!("Invalid UTF-8 payload: {}", e);
                                continue;
                            }
                            None => {
                                warn!("Empty payload received");
                                continue;
                            }
                        };

                        total_transactions += 1;

                        match serde_json::from_str::<Transaction>(payload) {
                            Ok(tx) => {
                                if let Some(fraud_event) = detector.evaluate(&tx) {
                                    total_frauds += 1;
                                    warn!(
                                        "🚨 [FRAUD DETECTED #{} - {}] Tx: {} | Account: {} | Amount: {:.2} {} > {:.2} | Merchant: {:?}",
                                        total_frauds,
                                        fraud_event.severity,
                                        fraud_event.transaction_id,
                                        fraud_event.account_id,
                                        fraud_event.amount,
                                        fraud_event.currency,
                                        config.fraud_threshold,
                                        fraud_event.merchant.as_deref().unwrap_or("Unknown")
                                    );

                                    match serde_json::to_string(&fraud_event) {
                                        Ok(json_payload) => {
                                            let record = FutureRecord::to(&config.fraud_topic)
                                                .key(&fraud_event.account_id)
                                                .payload(&json_payload);

                                            if let Err((e, _)) = producer.send(record, Duration::from_secs(3)).await {
                                                error!("Failed to emit fraud event to '{}': {}", config.fraud_topic, e);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to serialize fraud event: {}", e);
                                        }
                                    }
                                } else {
                                    if total_transactions % 100 == 0 {
                                        info!(
                                            "Processed {} transactions (Legit: {}, Flagged Fraud: {})",
                                            total_transactions,
                                            total_transactions - total_frauds,
                                            total_frauds
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize transaction JSON: {}. Payload: {}", e, payload);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Kafka consumer error: {}", e);
                    }
                }
            }
        }
    }

    info!(
        "Rust Fraud Detector stopped. Total processed: {}, Total fraud flagged: {}",
        total_transactions, total_frauds
    );

    Ok(())
}
