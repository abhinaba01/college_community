from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json
import os
import time

# Wait for services
time.sleep(20)

# Neo4j Connection
uri = os.getenv('NEO4J_URI', "bolt://neo4j_db:7687")
# Note: Basic auth default for docker image is neo4j/password
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

# Kafka Consumer
print("Connecting to Kafka...")
consumer = KafkaConsumer(
    'new_listings',
    bootstrap_servers=[os.getenv('KAFKA_BROKER', 'kafka:9092')],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'
)

def update_graph(tx, user, category):
    # Create nodes and relationships
    query = (
        "MERGE (u:User {name: $user}) "
        "MERGE (c:Category {name: $category}) "
        "MERGE (u)-[:POSTED_IN]->(c)"
    )
    tx.run(query, user=user, category=category)
    print(f"Graph Updated: {user} -> {category}")

print("Python Analytics Engine Started... Listening for events.")

for message in consumer:
    data = message.value
    print(f"Received Event: {data}")
    
    try:
        with driver.session() as session:
            session.write_transaction(update_graph, data['user'], data['category'])
    except Exception as e:
        print(f"Neo4j Error: {e}")