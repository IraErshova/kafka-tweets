#!/bin/bash

echo "Start Kafka tweet stream"

mkdir -p ./data

echo "Start Zookeeper and Kafka"
docker-compose up -d zookeeper kafka

# Function to check if Kafka is ready
check_kafka_ready() {
    docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1
    return $?
}

echo "Waiting for Kafka to be ready"
max_attempts=15
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if check_kafka_ready; then
        echo "Kafka is ready!"
        break
    else
        echo "Attempt $((attempt + 1))/$max_attempts - Kafka not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    fi
done

if [ $attempt -eq $max_attempts ]; then
    echo "Kafka failed to start properly after $max_attempts attempts"
    echo "Checking container status..."
    docker-compose ps
    echo "Kafka logs:"
    docker-compose logs kafka
    exit 1
fi

# Create topic
echo "Create 'tweets' topic"
docker exec kafka kafka-topics --create \
    --topic tweets \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

if [ $? -eq 0 ]; then
    echo "Topic 'tweets' created successfully"
else
    echo "Topic creation failed"
fi

echo "System started successfully"
echo ""
echo "To start streaming tweets run:"
echo "  docker-compose up tweet-producer"
echo ""
echo "To consume messages from the tweets topic run:"
echo "  docker-compose up tweet-consumer"
echo ""
echo "To stop the system:"
echo "  docker-compose down"