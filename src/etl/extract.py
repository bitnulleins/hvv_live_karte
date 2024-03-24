import requests
import json
import time 
import os
import luigi

from src.HVVAuth import HVVAuth
from dotenv import load_dotenv


load_dotenv()


class extract(luigi.Task):
    """
    Lädt die JSON Werte aus der HVV API aus.
    """

    force = luigi.BoolParameter(significant=True, default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # To force execution, we just remove all outputs before `complete()` is called
        if self.force is True:
            outputs = luigi.task.flatten(self.output())
            for out in outputs:
                if out.exists():
                    os.remove(self.output().path)

    def output(self):
        return luigi.LocalTarget("./tmp/data/response.json")

    def run(self):
        request_url = os.environ['API_URL'] + "getVehicleMap"

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

        data = requests.post(request_url, json=payload, auth=HVVAuth(payload)).json()

        with self.output().open("w") as f:
            f.write(json.dumps(data))