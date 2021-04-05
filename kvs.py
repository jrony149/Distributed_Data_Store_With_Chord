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

def sender(location, extension, key, payload):
    resp = response = requests.put(f'http://{location}/{extension}/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
    resp_data = resp.json()
    return resp_data, resp.status_code

@app.route('/kvs/keys/<key>', methods=['PUT'])
def add(key):
    
    data = request.get_json()
    if "value" not in data: return json.dumps({"error":"Value is missing","message":"Error in PUT"}), 400
    if len(key) > 50 : return json.dumps({"error":"Key is too long","message":"Error in PUT"}), 400

    global state 
    key_hash_id,payload               = state.hash_key(key),{}
    payload["value"],address_present  = data["value"],"address" in data
    payload["address"]                = data["address"] if address_present else state.address 
    if key_hash_id in state.set_of_local_ids: return sender(state.address, "store_key", key, payload)
    if key_hash_id < state.finger_table[0][0]:
        if key_hash_id < state.lowest_hash_id: return sender(state.predecessor[1], "kvs/keys", key, payload)
        else: return sender(state.finger_table[0][1], "store_key", key, payload)
    if key_hash_id > state.finger_table[-1][0]: return sender(state.finger_table[-1][1], "kvs/keys", key, payload)
    bounds = state.maps_to(key_hash_id)
    if bounds["upper bound"][2] == 1:
        pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
        if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]:
            return sender(bounds["upper bound"][1], "store_key", key, payload)
        else:
            return sender(bounds["lower bound"][1], "kvs/keys", key, payload)
    else:
        return sender(bounds["lower bound"][1], "kvs/keys", key, payload)
  
    return json.dumps({"message":"Server is down!"}), 400     

@app.route('/recon', methods=['GET'])
def recon():
    array = []
    state.cl.moveHead()
    
    for _ in range(state.cl.getLength()):
        array.append(state.cl.getCursorData())
        state.cl.moveNext()
    return json.dumps({"linked list data":array,"length of linked list":state.cl.getLength(), "finger table":state.finger_table, "local store":state.storage, "map":state.map})

@app.route('/kvs/keys/<key>', methods=['GET'])
def get(key):
    id = state.hash_key(key)
    address = state.maps_to(id)

    return json.dumps({"lowest_hash_id":state.lowest_hash_id,"lowest hash id's predecessor":state.predecessor,"here's the key's hashed id":id, "here's the address that the key maps to in this node's ft":address}), 200


@app.route('/store_key/<key>', methods = ['PUT'])
def store(key):

    global state
    data               = request.get_json()

    replace            = key in state.storage
    message            = "Updated successfully" if replace else "Added successfully"
    status_code        = 200 if replace else 201
    state.storage[key] = data["value"]
    if data["address"] == state.address:#if the address featured in the request data is the same as the
                                        #local address, then we don't return the address as part of the response.
        return json.dumps({"message":message,"replace":replace}), status_code
    else:#if the address featured in the request data is NOT the same as the local
         #address, then we DO return the address as part of the response.
        return json.dumps({"message":message,"replace":replace,"address":state.address}), status_code
