from app import app
from flask import request
import json
import requests
from state import State
import logging
import sys
import _thread

global state
@app.before_first_request
def build_state():
    global state
    state = State()

@app.route('/recon', methods=['GET'])
def recon():
    array = []
    state.cl.moveHead()
    
    for _ in range(state.cl.getLength()):
        array.append(state.cl.getCursorData())
        state.cl.moveNext()
    return json.dumps({"linked list data":array, "finger table":state.finger_table})

@app.route('/kvs/keys/<key>', methods=['GET'])
def get(key):
    id = state.hash_key(key)
    address = state.maps_to(key)

    return json.dumps({"here's the key's hashed id":id, "here's the address that the key maps to in this node's ft":address}), 200
    