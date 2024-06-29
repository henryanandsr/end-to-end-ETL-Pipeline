from kafka import KafkaConsumer
from google.cloud import bigquery
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'techcrunch-news',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize BigQuery client with service account credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path-to-your-service-account.json"
client = bigquery.Client()

dataset_id = 'your-dataset-id'  # Update this
table_id = 'your-table-id'  # Update this

def load_data_to_bigquery(data):
    table_ref = client.dataset(dataset_id).table(table_id)
    errors = client.insert_rows_json(table_ref, data)
    if errors:
        logging.error(f"Failed to insert rows into BigQuery: {errors}")
    else:
        logging.info(f"Successfully inserted {len(data)} rows into BigQuery")

# Main consumer loop
for message in consumer:
    try:
        message_data = message.value  # Get the message value
        logging.info(f"Processing message: {message_data}")  # Log the message data

        # Ensure message_data is a dict (it should be if deserialization is correct)
        if isinstance(message_data, dict):
            load_data_to_bigquery([message_data])
        else:
            logging.error(f"Message data is not a dict: {message_data}")
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")

# Close Kafka consumer
consumer.close()
