from kafka import KafkaProducer
import requests
import time
import DbConnect as db
import os
from dotenv import load_dotenv
import pandas as pd
database = db.DbConnect()
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_HOST") + ':' + '9092'
# Define Kafka broker address and topic name

KAFKA_TOPIC = 'stock'
# API_KEY = 'RAK70DKX2FXRM7VD'
API_KEY = 'Y2VQ0ZMPD4OS7ESD'
STOCK_NAME = 'GOOG'
DURATION = '1min'
TIMEOUT = 60000

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# Function to send data to Kafka


def send_data_to_kafka():
    try:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={STOCK_NAME}&interval={DURATION}&apikey={API_KEY}&datatype=csv'
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.text.splitlines()
            for line in data[1:]:  # Ignorer l'en-tête
                producer.send(KAFKA_TOPIC, value=line.encode('utf-8'))
            print("Data sent to Kafka successfully.")
        else:
            print(f"API request failed with status code {response.status_code}")
            database.insert_one({'error': f"API request failed with status code {response.status_code}"})
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        database.insert_one({'error': str(e)})



def main():
    try:
        while True:
            send_data_to_kafka()
            time.sleep(70)

    except KeyboardInterrupt:
        print("Script terminated by user.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        database.insert_one({'error': str(e)})
    finally:
        producer.close()


if __name__ == "__main__":
    main()
