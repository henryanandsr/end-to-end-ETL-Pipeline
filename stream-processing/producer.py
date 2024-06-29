import logging
from kafka import KafkaProducer
import json
from datetime import datetime
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

NEWS_API_KEY = ''  # Replace with your actual NewsAPI key
NEWS_API_URL = ''  # Replace with the URL you want to fetch

def fetch_news_data(query='tesla'):
    params = {
        'q': query,
        'from': '2024-05-29',
        'sortBy': 'publishedAt',
        'apiKey': NEWS_API_KEY,
        'language': 'en',
        'pageSize': 100  # Adjust the number of articles fetched as needed
    }
    response = requests.get(NEWS_API_URL, params=params)
    if response.status_code == 200:
        news_data = response.json().get('articles', [])
        return [{
            "id": idx,
            "date": article["publishedAt"],
            "title": article["title"],
            "url": article["url"]
        } for idx, article in enumerate(news_data)]
    else:
        logging.error(f"Failed to fetch news data: {response.status_code} {response.text}")
        return []

def convert_to_timestamp(date_str):
    # Convert date string to timestamp (Unix epoch)
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return int(dt.timestamp())  # Convert datetime to timestamp and cast to int
    except ValueError as e:
        logging.error(f"Error converting date string {date_str} to timestamp: {str(e)}")
        return None


def send_to_kafka(news_data):
    for news in news_data:
        # Convert date to timestamp
        if "date" in news:
            news["date"] = convert_to_timestamp(news["date"])
            if news["date"] is None:
                continue  # Skip invalid entries

        # Convert news data to JSON and send to Kafka
        producer.send('techcrunch-news', value=news)
        logging.info(f"Sent news data to Kafka: {news}")

if __name__ == "__main__":
    news_data = fetch_news_data()
    send_to_kafka(news_data)
    producer.flush()
    logging.info("Producer execution completed")
