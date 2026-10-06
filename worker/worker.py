import json
import os
import sys
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

from inventory import ROUTERS, get_router_config
from router_client import RouterClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TASKS = os.getenv("TOPIC_TASKS", "router-tasks")
TOPIC_RESULTS = os.getenv("TOPIC_RESULTS", "router-results")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "network-worker-group")

def create_kafka_producer(servers: str):
    retries = 10
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all"
            )
            print(f"[Worker] Connected Producer to Kafka at {servers}")
            return producer
        except Exception as e:
            print(f"[Worker] Kafka Producer connection failed ({i+1}/{retries}): {e}. Retrying in 2s...")
            time.sleep(2)
    sys.exit(1)

def create_kafka_consumer(servers: str, topic: str, group_id: str):
    retries = 10
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print(f"[Worker] Connected Consumer to Kafka at {servers}, topic='{topic}', group='{group_id}'")
            return consumer
        except Exception as e:
            print(f"[Worker] Kafka Consumer connection failed ({i+1}/{retries}): {e}. Retrying in 2s...")
            time.sleep(2)
    sys.exit(1)

def main():
    print("=" * 60)
    print(" [Network Automation Worker (Kafka + Paramiko)] Starting...")
    print(f" Target Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f" Task Topic:   {TOPIC_TASKS}")
    print(f" Result Topic: {TOPIC_RESULTS}")
    print("=" * 60)

    producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)
    consumer = create_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, TOPIC_TASKS, CONSUMER_GROUP)

    print("[Worker] Ready and listening for router tasks...")

    try:
        for message in consumer:
            task = message.value
            task_id = task.get("task_id", "unknown")
            target_router = task.get("router_id", "Router1")
            commands = task.get("commands", [])

            print(f"\n[Worker] Received Task [{task_id}] for Target: {target_router}")
            print(f"[Worker] Commands to execute: {commands}")

            # Handle targeting "all" or specific router
            target_list = list(ROUTERS.keys()) if target_router.lower() == "all" else [target_router]

            for rid in target_list:
                config = get_router_config(rid)
                if not config:
                    print(f"[Worker] Unknown router '{rid}', skipping.")
                    result_payload = {
                        "task_id": task_id,
                        "router_id": rid,
                        "status": "FAILED",
                        "output": f"Router configuration for '{rid}' not found in inventory.",
                        "error": "ROUTER_NOT_FOUND",
                        "execution_time_ms": 0,
                        "is_mock": False,
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    }
                    producer.send(TOPIC_RESULTS, value=result_payload)
                    producer.flush()
                    continue

                print(f"[Worker] Executing via Paramiko SSH on {rid} ({config['host']}:{config['port']})...")
                start_time = time.time()
                
                client = RouterClient(config)
                success, output, is_mock = client.execute_commands(commands)
                
                elapsed_ms = round((time.time() - start_time) * 1000, 2)
                status_str = "SUCCESS" if success else "FAILED"

                print(f"[Worker] Execution finished on {rid}: {status_str} in {elapsed_ms}ms (Mock={is_mock})")

                result_payload = {
                    "task_id": task_id,
                    "router_id": rid,
                    "status": status_str,
                    "output": output,
                    "is_mock": is_mock,
                    "execution_time_ms": elapsed_ms,
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }

                # Publish result back to Kafka
                producer.send(TOPIC_RESULTS, value=result_payload)
                producer.flush()
                print(f"[Worker] Published result for [{task_id}] to '{TOPIC_RESULTS}'")

    except KeyboardInterrupt:
        print("\n[Worker] Shutting down cleanly...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()
