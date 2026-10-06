import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TASKS = os.getenv("TOPIC_TASKS", "router-tasks")
TOPIC_RESULTS = os.getenv("TOPIC_RESULTS", "router-results")
CONSUMER_GROUP_BACKEND = "backend-results-consumer"
