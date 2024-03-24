import json
import time 
import os
import luigi

from pyproj import Transformer
from dotenv import load_dotenv
from pymongo import MongoClient
from src.etl.extract import extract


load_dotenv()


class transform(luigi.Task):
    """
    Transform coordinates and JSON to target format
    Coordinates have to be transformed in a useable format
    """

    force = luigi.BoolParameter(significant=False, default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before `complete()` is called
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(self.output().path)

    def requires(self):
        return extract()

    def output(self):
        return luigi.LocalTarget("./tmp/data/transformed.json")

    def run(self):
        with self.input().open("r") as f:
            input_json = json.load(f)

            try:
                self.mongo_connection = MongoClient(f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}")
                self.mongo_collection = self.mongo_connection['DAD'][os.environ['KAFKA_TOPIC']]
                print("[INFO] MongoDB connection sucessfully.")
            except:  
                raise Exception("[WARNING] Could not connect to MongoDB.")
            
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

            data = [transformJSON(json_data_part) for json_data_part in input_json['journeys']]

            with self.output().open("w") as f:
                f.write(json.dumps(data))
