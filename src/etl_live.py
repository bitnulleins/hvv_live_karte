import requests
import json
import time 
import os

from kafka import KafkaProducer
from src.HVVAuth import HVVAuth
from pyproj import Transformer
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

load_dotenv()

class ETLPipeline():
    def __init__(self):
        """
        Initialisiert die benötigten Variablen
        """
        try:
            self.mongo_connection = MongoClient(f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}")
            self.mongo_collection = self.mongo_connection['DAD'][os.environ['KAFKA_TOPIC']]
            print("[INFO] MongoDB connection sucessfully.")
        except:  
            raise Exception("[WARNING] Could not connect to MongoDB.")
        
        try:
            url = f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}/DAD.{os.environ['KAFKA_TOPIC']}?authSource=admin"
            
            # Connect to Spark
            self.spark = SparkSession \
                .builder \
                .appName("fahrtendb") \
                .master('local')\
                .config("spring.data.mongodb.authentication-database=admin") \
                .config("spark.mongodb.input.uri", url) \
                .config("spark.mongodb.output.uri", url) \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()
        except:
            raise Exception("[WARNING] Spark not connected.")

        self.request_url = os.environ['API_URL'] + "getVehicleMap"

    def __call__(self):
        """
        Fügt alles zusammen und ruft Extract, Transform und Load auf.
        """
        print(f"[INFO] Send request to GeoFox HVV API.")
        r = self.extract()
        print(f"[INFO] Request sucessfully get response for transform.")
        r = self.transform(r)
        print(f"[INFO] Anzahl HVV Objekte: {len(r)}")
        self.load_mongo(r)
        self.analyze()

        print(f"[INFO] Finish backend work, now push data to Kafka for Frontend.")
        self.load_kafka(r)

    def extract(self) -> requests.Response:
        """
        Lädt die JSON Werte aus der HVV API aus.
        
        """
        payload = { 
            "version":51,
            "boundingBox":{
                "lowerLeft": {
                    "x": os.environ['LOWER_LEFT_X'],
                    "y": os.environ['LOWER_LEFT_Y'], "type":"EPSG_4326"
                },
                "upperRight": {
                    "x": os.environ['UPPER_RIGHT_X'],
                    "y":os.environ['UPPER_RIGHT_Y'], "type":"EPSG_4326"
                }
            },
            "periodBegin":int(time.time()),
            "periodEnd":int(time.time()),
            "withoutCoords":False,
            "coordinateType":"EPSG_31467",
            "vehicleTypes":os.environ['VEHICLE_TYPES'].split(","),
            "realtime":True
        }
        return requests.post(self.request_url, json=payload, auth=HVVAuth(payload)).json()

    def transform(self, input):
        """
        Transform coordinates and JSON to target format
        Coordinates have to be transformed in a useable format
        """
        # define transformer
        transformer = Transformer.from_crs("epsg:31467", "epsg:4326")

        def transformCoordinates(coordinates):
            lat, lon = coordinates
            coords = str(transformer.transform(lon, lat)).split(',')
            lat = round(float(str(coords[0])[1:]), 6)
            lon = round(float(str(coords[1])[1:-1]), 6)
            return [lat, lon] # Latitude, Longitude (Rückgabe)

        def transformJSON(json_data_part):
            # mid course calculation
            course_mid = int(len(json_data_part['segments'][0]['track']['track'])/2)
            if course_mid % 2 != 0 : course_mid -= 1

            startKey = json_data_part['segments'][0]['startStationKey'].replace('Master:','')
            endKey = json_data_part['segments'][0]['endStationKey'].replace('Master:','')
            
            return {
                "_id": f"{json_data_part['line']['name']}-{startKey}-{endKey}-{json_data_part['segments'][0]['startDateTime']}",
                "line": json_data_part['line']['name'],
                "vehicleType": json_data_part['vehicleType'],
                "timestamp": int(time.time()),
                "startDateTime": json_data_part['segments'][0]['startDateTime'],
                "endDateTime": json_data_part['segments'][0]['endStationName'],
                "startStationName": json_data_part['segments'][0]['startStationName'],
                "endStationName": json_data_part['segments'][0]['endStationName'],
                "realtimeDelay": json_data_part['segments'][0]['realtimeDelay'],
                "destination": json_data_part['segments'][0]['destination'],
                "origin": json_data_part['line']['origin'],
                "tracks": {
                    "start": transformCoordinates(json_data_part['segments'][0]['track']['track'][0:2]),
                    "end": transformCoordinates(json_data_part['segments'][0]['track']['track'][-2:]),
                    "course": transformCoordinates(json_data_part['segments'][0]['track']['track'][course_mid:course_mid+2]),
                }
            }

        return [transformJSON(json_data_part) for json_data_part in input['journeys']]

    def load_kafka(self, input):
        """
        Push Data to Frontend via Kafka
        """
        producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_HOST'], value_serializer=lambda v:json.dumps(v).encode('utf-8'))
        producer.send(os.environ['KAFKA_TOPIC'], value=input)
        producer.flush()
        print(f"[INFO] Push data to frontend by kafka topic: {os.environ['KAFKA_TOPIC']}")


    def load_mongo(self, input):
        """
        Save data to MongoDB for Batch Processing
        """
        for d in input:
            self.mongo_collection.update_one({'_id':d['_id']}, {'$set': d}, True)
        print("[INFO] Finished save data to MongoDB.")

    def analyze(self):
        """
        Aggregate delays by latitude and longitute in average.
        """
        # Read database as dataframe
        df = self.spark.read\
            .format('com.mongodb.spark.sql.DefaultSource')\
            .load()
        # Aggregate and save as json
        df.select(
            F.expr("tracks.end[0]").alias("lat"),
            F.expr("tracks.end[1]").alias("lon"),
            F.expr("realtimeDelay").alias("delay")
        ).withColumn("avg_delay", F.avg("delay").over( Window.partitionBy("lon","lat") )).drop("delay").toPandas().to_json('./static/heatmap.json',orient='records')