mod config;
mod dispatcher;
mod models;

use config::AppConfig;
use dispatcher::NotificationDispatcher;
use models::AlertEvent;
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
    info!("📢 Starting Rust Notification Service");
    info!("Kafka Brokers           : {}", config.kafka_brokers);
    info!("Source Alerts Topic     : {}", config.alerts_topic);
    info!("Output Notif Topic      : {}", config.notifications_topic);
    info!("Consumer Group ID       : {}", config.group_id);
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
        .subscribe(&[&config.alerts_topic])
        .expect("Failed to subscribe to alerts topic");

    let dispatcher = NotificationDispatcher::new();
    let mut total_alerts_received: u64 = 0;
    let mut total_notifications_dispatched: u64 = 0;

    info!("Listening for fraud alerts on '{}'...", config.alerts_topic);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received. Exiting Notification Service...");
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

                        total_alerts_received += 1;

                        match serde_json::from_str::<AlertEvent>(payload) {
                            Ok(alert) => {
                                let notification = dispatcher.dispatch(&alert);
                                total_notifications_dispatched += 1;

                                info!(
                                    "\n╔══════════════════════════════════════════════════════════════════════╗\n\
                                     ║ 📢 NOTIFICATION DISPATCHED [#{}]                                    ║\n\
                                     ╠══════════════════════════════════════════════════════════════════════╣\n\
                                     ║ Notification ID : {:<50} ║\n\
                                     ║ Alert Ref       : {:<50} ║\n\
                                     ║ Fraud Ref       : {:<50} ║\n\
                                     ║ Account ID      : {:<50} ║\n\
                                     ║ Amount          : {:<50} ║\n\
                                     ║ Severity        : {:<50} ║\n\
                                     ║ Urgency Level   : {:<50} ║\n\
                                     ║ Channels        : {:<50} ║\n\
                                     ║ Subject         : {:<50} ║\n\
                                     ╚══════════════════════════════════════════════════════════════════════╝",
                                    total_notifications_dispatched,
                                    notification.notification_id,
                                    notification.alert_id,
                                    notification.fraud_id,
                                    notification.account_id,
                                    format!("{:.2} {}", notification.amount, notification.currency),
                                    alert.severity,
                                    notification.urgency,
                                    notification.channels.join(", "),
                                    notification.subject
                                );

                                match serde_json::to_string(&notification) {
                                    Ok(json_payload) => {
                                        let record = FutureRecord::to(&config.notifications_topic)
                                            .key(&notification.account_id)
                                            .payload(&json_payload);

                                        if let Err((e, _)) = producer.send(record, Duration::from_secs(3)).await {
                                            error!("Failed to emit notification event to '{}': {}", config.notifications_topic, e);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to serialize notification event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize alert JSON: {}. Payload: {}", e, payload);
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
        "Notification Service stopped. Alerts processed: {}, Notifications dispatched: {}",
        total_alerts_received, total_notifications_dispatched
    );

    Ok(())
}
