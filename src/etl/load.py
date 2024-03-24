import requests
import json
import os
import luigi

from dotenv import load_dotenv
from pymongo import MongoClient
from src.etl.transform import transform


load_dotenv()


class load(luigi.Task):
    """
    Save data to MongoDB for Batch Processing
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
        return transform()

    def output(self):
        return self.input()

    def run(self):
        """
        Save data to MongoDB for Batch Processing
        """

        # Mongo connection
        try:
                mongo_connection = MongoClient(f"mongodb://{os.environ['MONGO_USER']}:{os.environ['MONGO_PASSWD']}@{os.environ['MONGO_HOST']}:{os.environ['MONGO_PORT']}")
                mongo_collection = mongo_connection['DAD'][os.environ['KAFKA_TOPIC']]
                print("[INFO] MongoDB connection sucessfully.")
        except:  
            raise Exception("[WARNING] Could not connect to MongoDB.")
        
        with self.input().open("r") as f:
            input_json = json.load(f)
            for d in input_json:
                mongo_collection.update_one({'_id':d['_id']}, {'$set': d}, True)