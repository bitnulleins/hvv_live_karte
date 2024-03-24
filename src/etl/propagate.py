import json
import os
import luigi

from kafka import KafkaProducer
from dotenv import load_dotenv
from src.etl.transform import transform
from src.etl.load import load

load_dotenv()


class propagate(luigi.Task):
    """
    Push Data to Frontend via Kafka
    """

    def requires(self):
        return [
            transform(),
            load()
        ]

    def output(self):
        pass

    def run(self):
        # get FIRST input from requires
        with self.input()[0].open("r") as f:
            input_json = json.load(f)
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'], value_serializer=lambda v:json.dumps(v).encode('utf-8'))
            producer.send(os.environ['KAFKA_TOPIC'], value=input_json)
            producer.flush()
            print(f"[INFO] Push data to frontend by kafka topic: {os.environ['KAFKA_TOPIC']}")