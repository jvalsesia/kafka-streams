use clap::Parser;
use std::env;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "High-performance Rust Transaction Producer for Kafka")]
pub struct CliArgs {
    /// Kafka bootstrap servers
    #[arg(short = 'b', long, default_value = "localhost:9092")]
    pub brokers: String,

    /// Kafka target topic
    #[arg(short = 't', long, default_value = "transactions")]
    pub topic: String,

    /// Total number of transactions to produce
    #[arg(short = 'n', long, default_value_t = 1000)]
    pub count: usize,

    /// Delay in milliseconds between transactions (0 for max speed)
    #[arg(short = 'd', long, default_value_t = 0)]
    pub delay_ms: u64,

    /// Run continuously until interrupted
    #[arg(short = 'c', long, default_value_t = false)]
    pub continuous: bool,

    /// Fraud threshold for local reporting
    #[arg(long, default_value_t = 3000.0)]
    pub threshold: f64,
}

impl CliArgs {
    pub fn parse_with_env() -> Self {
        dotenvy::dotenv().ok();
        let mut args = CliArgs::parse();

        // Allow environment variables to override defaults if flags not passed
        if let Ok(env_brokers) = env::var("KAFKA_BOOTSTRAP_SERVERS") {
            if args.brokers == "localhost:9092" {
                args.brokers = env_brokers;
            }
        }

        if let Ok(env_topic) = env::var("TRANSACTIONS_TOPIC") {
            if args.topic == "transactions" {
                args.topic = env_topic;
            }
        }

        args
    }
}
