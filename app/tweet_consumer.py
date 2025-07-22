import time

from kafka import KafkaConsumer
import json
import os

from kafka.errors import NoBrokersAvailable

def get_kafka_consumer(max_retries=10, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            _consumer = KafkaConsumer(
                os.getenv('KAFKA_TOPIC', 'tweets'),
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                group_id='tweet-group',
                enable_auto_commit=False,  # Ensures commiting only when processing is successful
                auto_offset_reset='earliest',  # Start at the earliest offset
                fetch_min_bytes=1,  # Get messages as soon as they are available
                fetch_max_wait_ms=50,  # Wait at most 50ms for new data
                max_poll_records=10,  # Small batch size for low-latency
                session_timeout_ms=6000,  # Timeout for detecting dead consumers
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("KafkaConsumer connected successfully.")
            return _consumer
        except NoBrokersAvailable as e:
            print(f"Kafka broker not available (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error initializing KafkaConsumer (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise
            time.sleep(delay)
    return None


consumer = get_kafka_consumer()

if __name__ == "__main__":
    print("Listening for tweets...")

    for message in consumer:
        tweet = message.value
        print(f"Received tweet: {tweet['tweet_id']}")
        consumer.commit()
