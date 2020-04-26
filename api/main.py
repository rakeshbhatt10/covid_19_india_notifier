import os
import sys
import requests
from flask import jsonify, request, make_response

# from webapp.controllers import helper

from webapp import app
# from flask import Flask


# helper.load_saved_model()
# helper.load_vocabulary()
# helper.load_stop_words()

@app.route('/')
def index():
    print ("Inside index html")
    f = open('html/register.html', 'r')
    return f.read()

@app.route('/states.json')
def states():
    print ("Inside index html")
    f = open('html/states.json', 'r')
    return f.read()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5500)

