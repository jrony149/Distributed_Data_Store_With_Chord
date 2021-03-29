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
    key_hash_id = state.hash_key(key)
    payload     = {}
    if key_hash_id in state.set_of_local_ids:
        #send the key-value pair to be stored at the local address (make sure to set the address to the local address
        # within the json data of the request)
        payload["value"] = data["value"]
        if "address" not in data: payload["address"] = state.address 
        response = requests.put(f'http://{state.address}/store_key/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        #pass
    bounds = state.maps_to(key_hash_id)
    if bounds["lower bound"] != None and bounds["upper bound"] == None:
        #Forward the request to the lower bound
        pass
    if bounds["lower bound"] == None and bounds["upper bound"] != None:
        #Send the key-value pair to be stored at the upper bound's address.
        pass
    if bounds["lower bound"][1] == state.address:
        #send the key-value pair to be stored at the upper bound
        pass
    upper_bound = bounds["upper bound"][0]
    if bounds["upper bound"][2] == 1:
        pred_of_first_finger = state.immediate_pred(upper_bound)
        if key_hash_id > pred_of_first_finger and key_hash_id <= upper_bound:
            #send key-value pair to be stored at address associated with upper bound
            pass

    return json.dumps({"bounds":bounds, "immediate pred": pred_of_first_finger}), 200     

# @app.route('/kvs/keys/<key>', methods=['PUT'])
# def add(key):
#     data = request.get_json()
#     if "value" not in data: return json.dumps({"error":"Value is missing","message":"Error in PUT"}), 400
#     if len(key) > 50 : return json.dumps({"error":"Key is too long","message":"Error in PUT"}), 400
#     global state
#     address = state.maps_to(key)
#     if address == state.address:
#         replace = key in state.storage
#         message = "Updated successfully" if replace else "Added successfully"
#         status_code = 200 if replace else 201
#         state.storage[key] = data["value"]
#         return json.dumps({"message":message,"replaced":replace}), status_code
#     else:
#         response = requests.put(f'http://{address}/kvs/keys/{key}', json = request.get_json(), timeout=6, headers = {"Content-Type": "application/json"})
#         proxy_response = response.json()
#         proxy_response['address'] = address
#         return proxy_response, response.status_code

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
    address = state.maps_to(id)

    return json.dumps({"here's the key's hashed id":id, "here's the address that the key maps to in this node's ft":address}), 200


@app.route('/store_key/<key>', methods = ['PUT'])
def store(key):

    global state
    data = request.get_json()

    app.logger.info("Here's the key-value pair: " + str(data["value"]))

    state.storage[key] = data["value"]

    return json.dumps({"store":state.storage, "list of local ids":state.list_of_local_ids }), 200