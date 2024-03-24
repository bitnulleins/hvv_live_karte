import hmac
import base64
import hashlib
import json
import os
import logging

from dotenv import load_dotenv
from requests.auth import AuthBase


load_dotenv()

class HVVAuth(AuthBase):

    def __init__(self, payloadFunc):
        # Get env data
        self.username = os.environ['API_USER']
        self.password = os.environ['API_KEY']

        # setup any auth-related data here
        #print(f"[INFO] Payload for HVV request {json.dumps(payloadFunc)}")
        self.sig = base64.b64encode(hmac.new(self.password.encode("UTF-8"), json.dumps(payloadFunc).encode('UTF-8'), hashlib.sha1).digest())

    def __call__(self, r):
        # modify and return the request
        r.headers['geofox-auth-signature'] = self.sig
        r.headers['geofox-auth-user'] = self.username
        return r