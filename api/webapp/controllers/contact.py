import os
from flask import request, jsonify

from webapp import app
from webapp.schemas.contact import validate_contact_request
import json
import sys


from webapp.controllers.handler import register_contact




@app.route('/register_contact', methods=['POST', 'OPTION'])
def request_register_contact():

    data = validate_contact_request(request.get_json())

    print ("Data ", data)

    if data["success"]:

        data = request.get_json()
        contact_id = register_contact(data)

        return jsonify({"sucesss": True, "data": contact_id})
    else:
        return jsonify({"success": False, 'message': data.get("message"), "key": data.get("key")}), 400



