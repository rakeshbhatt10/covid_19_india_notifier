import os
import json
import datetime

from bson.objectid import ObjectId
from flask import Flask
from flask_cors import CORS, cross_origin


class JSONEncoder(json.JSONEncoder):

    def default(self, o):

        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(0, set):
            return list(o)
        if isinstance(o, datetime.datetime):
            return str(o)

        return json.JSONEncoder.default(self, o)



app = Flask(__name__)
app.debug = True
app.json_encoder = JSONEncoder

from webapp.controllers import *