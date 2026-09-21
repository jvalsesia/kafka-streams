#!/usr/bin/env bash
set -e

KAFKA_BROKER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=("transactions" "fraud-detected-transactions" "alerts" "notifications")

echo "⏳ Waiting for Kafka broker ($KAFKA_BROKER) to be ready..."
until docker exec -i kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 2
done

echo "✅ Kafka is ready. Initializing topics..."

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic..."
    docker exec -i kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 3 \
        --replication-factor 1
done

echo "🎉 All Kafka topics created successfully:"
docker exec -i kafka kafka-topics --bootstrap-server localhost:9092 --list
