#!/usr/bin/python

# boot strap libs
import sys, os
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

# setup
pathname = os.path.abspath( os.path.dirname(sys.argv[0]) )
LIB_PATH =  pathname + '/../lib/'
sys.path.append( LIB_PATH )
ETC_PATH = pathname + '/../etc/'

from flask import Flask, request
from flask import jsonify
from flask.ext.pymongo import PyMongo

app = Flask('assets')
mongo = PyMongo(app)

import json
from bson import Binary, Code
from bson.json_util import dumps

from re import compile, IGNORECASE

import logging

# old
from slac_ci.output import report


@app.route('/')
def root():
  return app.send_static_file('index.html')

@app.route('/<path:path>')
def static_proxy(path):
    return app.send_static_file(path)

def jsonise( data ):
    return json.loads( dumps(data) )

@app.route('/ci/<collection_name>/<key>/<value>')
def ci( collection_name, key, value, case_insensitive=True ):
    collection = getattr( mongo.db, collection_name )
    if case_insensitive:
        v = compile( value, IGNORECASE )
    else:
        v = value
    data = collection.find({key: v})
    return jsonify( { 'data': jsonise(data) } )

@app.route('/<key>/<value>')
def item( key, value, collection_name='final', case_insensitive=True  ):
    collection = getattr( mongo.db, collection_name )
    if case_insensitive:
        v = compile( value, IGNORECASE )
    else:
        v = value
    data = collection.find({key: v})
    return jsonify( { 'data': jsonise(data) } )

if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0')
