import os
import json
import time
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
import kagglehub

# Retry logic for KafkaProducer initialization
from kafka.errors import NoBrokersAvailable

def get_kafka_producer(max_retries=10, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            _producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                key_serializer=str.encode,  # Serialize the message key into bytes
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for acknowledgments from all in-sync replicas
                retries=5,  # Retry up to 5 times if a send fails
                compression_type='gzip'  # Compress messages using gzip to reduce network traffic and improve throughput
            )
            print("KafkaProducer connected successfully.")
            return _producer
        except NoBrokersAvailable as e:
            print(f"Kafka broker not available (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error initializing KafkaProducer (attempt {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                raise
            time.sleep(delay)
    return None


producer = get_kafka_producer()
topic = os.getenv('KAFKA_TOPIC', 'tweets')
messages_per_second = int(os.getenv('MESSAGES_PER_SECOND', 12))

def prepare_tweet_message(tweet_row):
    return {
        'tweet_id': str(tweet_row.get('tweet_id', '')),
        'author_id': str(tweet_row.get('author_id', '')),
        'text': str(tweet_row.get('text', '')),
        'response_tweet_id': str(tweet_row.get('response_tweet_id', '')),
        'in_response_to_tweet_id': str(tweet_row.get('in_response_to_tweet_id', '')),
        'created_at': datetime.now().isoformat(),
    }

def load_tweet_data():
    try:
        path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")
        csv_file = f"{path}/twcs/twcs.csv"

        df = pd.read_csv(csv_file)
        # remove empty tweets
        df = df.dropna(subset=['text'])

        print(f"Loaded {len(df)} tweets")
        return df

    except Exception as err:
        print(f"Error loading tweet data: {err}")
        return None

def cleanup():
    # Close Kafka producer and log final stats
    if producer:
        producer.flush()
        producer.close()
        print(f"Stream completed")

def send_tweet_stream():
    if not producer:
        print("Kafka producer not initialized")
        return

    # load tweet data
    df = load_tweet_data()
    if df is None or df.empty:
        print("No tweet data available")
        return

    # calculate delay between messages
    delay_between_messages = 1.0 / messages_per_second
    messages_sent = 0
    print(f"Starting tweet stream to topic '{topic}' at {messages_per_second} messages/second")

    try:
        # get rows in sequential order (no shuffling)
        for index, tweet_row in df.iterrows():
            start_time = time.time()
            try:
                message = prepare_tweet_message(tweet_row)

                # send to Kafka
                future = producer.send(
                    topic,
                    key=message['author_id'],
                    value=message
                )

                future.get(timeout=10)
                print(f"Produced tweet: {message['tweet_id']}")

                messages_sent += 1
                if messages_sent % 10 == 0:
                    print(f"Sent {messages_sent} messages")

            except Exception as e:
                print(f"Error sending message {messages_sent}: {e}")
                continue

            elapsed = time.time() - start_time
            sleep_time = max(0, delay_between_messages - elapsed)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"Unexpected error in stream: {e}")
    finally:
        cleanup()

if __name__ == "__main__":
    send_tweet_stream()
