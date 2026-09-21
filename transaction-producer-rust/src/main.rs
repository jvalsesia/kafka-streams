mod config;
mod generator;
mod models;

use config::CliArgs;
use generator::TransactionGenerator;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = CliArgs::parse_with_env();

    info!("==================================================");
    info!("🚀 Starting Rust Transaction Producer");
    info!("Kafka Brokers     : {}", args.brokers);
    info!("Target Topic      : {}", args.topic);
    if args.continuous {
        info!("Mode              : Continuous streaming (delay: {} ms)", args.delay_ms);
    } else {
        info!("Target Batch Size : {} events", args.count);
    }
    info!("Fraud Threshold   : > ${:.2}", args.threshold);
    info!("==================================================");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("message.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "100000")
        .set("compression.type", "none")
        .create()
        .expect("Failed to create Kafka producer");

    let generator = TransactionGenerator::new();
    let start_time = Instant::now();

    let total_sent = Arc::new(AtomicUsize::new(0));
    let fraud_candidates = Arc::new(AtomicUsize::new(0));

    let mut iteration = 0usize;

    loop {
        iteration += 1;
        let tx = generator.generate();

        let is_fraud_candidate = tx.amount > args.threshold;
        if is_fraud_candidate {
            fraud_candidates.fetch_add(1, Ordering::Relaxed);
        }

        let payload = match serde_json::to_string(&tx) {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to serialize transaction #{}: {}", iteration, e);
                continue;
            }
        };

        let record = FutureRecord::to(&args.topic)
            .key(&tx.account_id)
            .payload(&payload);

        // Asynchronously produce to Kafka
        match producer.send(record, Duration::from_secs(3)).await {
            Ok(_) => {
                let current_sent = total_sent.fetch_add(1, Ordering::Relaxed) + 1;
                if current_sent % 100 == 0 || current_sent == args.count {
                    info!(
                        "📤 Progress: Sent {}/{} transactions (Latest: {} | {} | ${:.2})",
                        current_sent,
                        if args.continuous { "∞".to_string() } else { args.count.to_string() },
                        tx.transaction_id,
                        tx.account_id,
                        tx.amount
                    );
                }
            }
            Err((e, _)) => {
                error!("Failed to deliver transaction #{}: {}", iteration, e);
            }
        }

        if args.delay_ms > 0 {
            sleep(Duration::from_millis(args.delay_ms)).await;
        }

        if !args.continuous && iteration >= args.count {
            break;
        }
    }

    let duration = start_time.elapsed();
    let sent_count = total_sent.load(Ordering::Relaxed);
    let fraud_count = fraud_candidates.load(Ordering::Relaxed);
    let throughput = if duration.as_secs_f64() > 0.0 {
        sent_count as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    info!("==================================================");
    info!("✅ Batch production completed successfully!");
    info!("Total Produced       : {} transactions", sent_count);
    info!("Suspected Fraud (> $3k): {} transactions ({:.1}%)", fraud_count, (fraud_count as f64 / sent_count as f64) * 100.0);
    info!("Total Duration       : {:.2?}", duration);
    info!("Throughput           : {:.2} events/sec", throughput);
    info!("Target Kafka Topic   : {}", args.topic);
    info!("==================================================");

    Ok(())
}
