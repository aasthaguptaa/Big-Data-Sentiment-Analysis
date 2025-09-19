from kafka import KafkaProducer, KafkaConsumer
import json, time, os, datetime, sys

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:9092")
KAFKA_TOPIC = "reddit-sentiment-analysis"
INPUT_PATH = "filtered.jsonl"
submission_speed = 0.0001 # seconds

MAX_RECORDS_TO_SEND = 72941142

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    #batch_size=16384,
    #linger_ms=500, 
)

sent_count = 0 

with open(INPUT_PATH, 'r', encoding='utf-8') as f:
    for line in f:
        if not line.strip():
            continue
        try:
            comment = json.loads(line)
            print(f"Sending: {comment.get('id', 'N/A')}, Count: {sent_count + 1}")
            producer.send(KAFKA_TOPIC, comment)
            sent_count += 1
            
            if sent_count >= MAX_RECORDS_TO_SEND:
                print(f"Reached {MAX_RECORDS_TO_SEND} records. Stopping producer.")
                break

            time.sleep(submission_speed)
        except json.JSONDecodeError as e:
            print(f"Skipping malformed JSON line: {line.strip()[:100]}... Error: {e}", file=sys.stderr)
        except Exception as e:
            print(f"An unexpected error occurred while sending: {e}", file=sys.stderr)

producer.flush()
producer.close()
print("Kafka Producer closed.")
