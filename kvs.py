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

@app.route('/kvs/keys/<key>', methods=['PUT'])
def add(key):
    data = request.get_json()
    if "value" not in data: return json.dumps({"error":"Value is missing","message":"Error in PUT"}), 400
    if len(key) > 50 : return json.dumps({"error":"Key is too long","message":"Error in PUT"}), 400
    global state
    address = state.maps_to(key)
    if address == state.address:
        replace = key in state.storage
        message = "Updated successfully" if replace else "Added successfully"
        status_code = 200 if replace else 201
        state.storage[key] = data["value"]
        return json.dumps({"message":message,"replaced":replace}), status_code
    else:
        response = requests.put(f'http://{address}/kvs/keys/{key}', json = request.get_json(), timeout=6, headers = {"Content-Type": "application/json"})
        proxy_response = response.json()
        proxy_response['address'] = address
        return proxy_response, response.status_code

@app.route('/recon', methods=['GET'])
def recon():
    array = []
    state.cl.moveHead()
    
    for _ in range(state.cl.getLength()):
        array.append(state.cl.getCursorData())
        state.cl.moveNext()
    return json.dumps({"linked list data":array, "finger table":state.finger_table, "local store":state.storage, "map":state.map})

@app.route('/kvs/keys/<key>', methods=['GET'])
def get(key):
    id = state.hash_key(key)
    address = state.maps_to(key)

    return json.dumps({"here's the key's hashed id":id, "here's the address that the key maps to in this node's ft":address}), 200
    