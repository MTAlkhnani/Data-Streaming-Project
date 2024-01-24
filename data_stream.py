import pandas as pd
from confluent_kafka import Producer
import json
import time

def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
        
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def produce_csv_data(df, topic, bootstrap_servers='localhost:9092'):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    try:
        for index, row in df.iterrows():
            data = row.to_dict()
            
            producer.produce(topic, json.dumps(data))
            producer.flush()
            print(f"{index}: row have been sent to topic: {topic}")
            time.sleep(1)  

    except Exception as e:
        print(f"Error producing data to Kafka: {e}")

file_path = "/Users/mohammedalkhnani/Downloads/Questions.csv"
kafka_topic = "test4" 

data_frame = read_csv_file(file_path)

if data_frame is not None:
    produce_csv_data(data_frame, kafka_topic)
