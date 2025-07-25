import time
import os
import json
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
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

def get_filename(_datetime):
    return os.path.join(
        output_dir,
        f"tweets_{_datetime.strftime('%d_%m_%Y_%H_%M')}.csv"
    )

consumer = get_kafka_consumer()

if __name__ == "__main__":
    print("Listening for tweets...")

    output_dir = '/app/data'
    os.makedirs(output_dir, exist_ok=True)
    current_file = None
    current_minute = None
    buffer = []
    header = ['author_id', 'created_at', 'text']

    for message in consumer:
        tweet = message.value
        try:
            created_at = tweet['created_at']
            dt = datetime.fromisoformat(created_at)
        except Exception as e:
            print(f"Error parsing created_at: {e}, skipping tweet.")
            consumer.commit()
            continue
        minute_key = dt.strftime('%d_%m_%Y_%H_%M')
        row = {
            'author_id': tweet.get('author_id', ''),
            'created_at': created_at,
            'text': tweet.get('text', '')
        }
        # if minute changes, flush buffer to file
        if current_minute is None:
            current_minute = minute_key
            current_file = get_filename(dt)
        if minute_key != current_minute:
            if buffer:
                df = pd.DataFrame(buffer)
                write_header = not os.path.exists(current_file)
                df.to_csv(current_file, mode='a', header=write_header, index=False)
                buffer = []
            current_minute = minute_key
            current_file = get_filename(dt)
        buffer.append(row)
        # clean buffer for memory reason
        if len(buffer) >= 10:
            df = pd.DataFrame(buffer)
            write_header = not os.path.exists(current_file)
            df.to_csv(current_file, mode='a', header=write_header, index=False)
            buffer = []
        print(f"Received tweet: {tweet.get('tweet_id', '')}")
        consumer.commit()

    # clean buffer on exit
    if buffer:
        df = pd.DataFrame(buffer)
        write_header = not os.path.exists(current_file)
        df.to_csv(current_file, mode='a', header=write_header, index=False)
