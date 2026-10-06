#!/usr/bin/env python3
"""
Integration test for the Kafka + Paramiko pipeline.
Tests producing a task to 'router-tasks' and receiving the result from 'router-results'.
"""
import json
import time
import uuid
import sys
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_TASKS = "router-tasks"
TOPIC_RESULTS = "router-results"

def test_pipeline():
    print("==================================================")
    print(" [Integration Test] Verifying Kafka Pipeline...")
    print("==================================================")

    # 1. Connect Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000
        )
        print("✓ Kafka Producer connected successfully.")
    except Exception as e:
        print(f"✗ Failed to connect Producer: {e}")
        return False

    # 2. Connect Consumer
    group_id = f"test-verifier-{uuid.uuid4().hex[:6]}"
    try:
        consumer = KafkaConsumer(
            TOPIC_RESULTS,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="latest",
            consumer_timeout_ms=10000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        print(f"✓ Kafka Consumer subscribed to '{TOPIC_RESULTS}'.")
    except Exception as e:
        print(f"✗ Failed to connect Consumer: {e}")
        return False

    # 3. Dispatch a test task
    test_task_id = f"test-{uuid.uuid4().hex[:6]}"
    test_payload = {
        "task_id": test_task_id,
        "router_id": "Router1",
        "commands": ["show ip interface brief"],
        "timestamp": time.time()
    }

    print(f"\n[Test] Publishing test task [{test_task_id}] to '{TOPIC_TASKS}'...")
    producer.send(TOPIC_TASKS, value=test_payload)
    producer.flush()
    print("✓ Task published to Kafka.")

    # 4. Wait for worker to consume and reply on router-results
    print(f"[Test] Awaiting result from worker on '{TOPIC_RESULTS}' (timeout 10s)...")
    start_wait = time.time()
    result_received = None

    for message in consumer:
        res = message.value
        if res.get("task_id") == test_task_id:
            result_received = res
            break

    if result_received:
        elapsed = round(time.time() - start_wait, 2)
        print(f"\n✓ SUCCESS! Received result in {elapsed}s:")
        print(f"  - Router: {result_received.get('router_id')}")
        print(f"  - Status: {result_received.get('status')}")
        print(f"  - Execution Time: {result_received.get('execution_time_ms')}ms")
        print(f"  - Is Mock: {result_received.get('is_mock')}")
        print(f"\n--- Output Snippet ---")
        output = result_received.get("output", "")
        print(output[:300] + ("..." if len(output) > 300 else ""))
        print("----------------------\n")
        return True
    else:
        print("✗ TIMEOUT: Worker did not publish result within 10 seconds.")
        print("  Make sure the worker service is running (`python worker/worker.py`).")
        return False

if __name__ == "__main__":
    success = test_pipeline()
    sys.exit(0 if success else 1)
