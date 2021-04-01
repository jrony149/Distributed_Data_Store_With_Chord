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
    first_receipt = False 
    key_hash_id   = state.hash_key(key)
    payload       = {}
    if "address" not in data: 
        payload["address"],first_receipt = state.address,True
    if "address" in data: 
        payload["address"],first_receipt = data["address"],False
    payload["value"]                         = data["value"]
    if key_hash_id in state.set_of_local_ids:
        app.logger.info("HELLO FROM KEY_HASH IN SET OF LOCAL IDS")

        #send the key-value pair to be stored at the local address (make sure to set the address to the local address
        # within the json data of the request) 
        response = requests.put(f'http://{state.address}/store_key/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        response_data = response.json()
        return response_data, response.status_code
         #pass
    bounds = state.maps_to(key_hash_id)
    if bounds["lower bound"] != None and bounds["upper bound"] == None:
        app.logger.info("HELLO FROM LOWER BOUND != NONE AND UPPER BOUND == NONE")
        #Forward the request to the lower bound
        lower_bound_addr = bounds["lower bound"][1]
        response = requests.put(f'http://{lower_bound_addr}/kvs/keys/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        response_data = response.json()
        return response_data, response.status_code
        #pass
    if bounds["lower bound"] == None and bounds["upper bound"] != None:
        app.logger.info("HELLO FROM LOWER BOUND == NONE AND UPPER BOUND != NONE")
        #Send the key-value pair to be stored at the upper bound's address.

        ext = "kvs/keys" if first_receipt else "store_key"
        app.logger.info("HERE'S ext : " + str(ext))
        upper_bound_addr = bounds["upper bound"][1]
        response = requests.put(f'http://{upper_bound_addr}/{ext}/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        response_data = response.json()
        return response_data, response.status_code
        #pass
    if bounds["lower bound"][1] == state.address:
        #send the key-value pair to be stored at the upper bound
        app.logger.info("HELLO FROM LOWER BOUND == STATE.ADDRESS")
        upper_bound_addr = bounds["upper bound"][1]
        response = requests.put(f'http://{upper_bound_addr}/store_key/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        response_data = response.json()
        return response_data, response.status_code
        #pass
    if bounds["upper bound"][2] == 1:
        upper_bound = bounds["upper bound"][0]
        pred_of_first_finger = state.immediate_pred(upper_bound)
        if key_hash_id > pred_of_first_finger and key_hash_id <= upper_bound:
            app.logger.info("HELLO FROM UPPER BOUND IS FIRST FINGER")
            #send key-value pair to be stored at address associated with upper bound
            upper_bound_addr = bounds["upper bound"][1]
            response = requests.put(f'http://{upper_bound_addr}/store_key/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
            response_data = response.json()
            return response_data, response.status_code
            #pass
        else:
            #forward key-value pair to lower bound
            app.logger.info("HELLO FROM ELSE CASE OF UPPER BOUND IS FIRST FINGER")
            lower_bound_addr = bounds["lower bound"][1]
            response = requests.put(f'http://{lower_bound_addr}/kvs/keys/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
            response_data = response.json()
            return response_data, response.status_code
            #pass
    else: #forward the request to the lower bound
        app.logger.info("HELLO FROM DEFAULT")
        lower_bound_addr = bounds["lower bound"][1]
        response = requests.put(f'http://{lower_bound_addr}/kvs/keys/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
        response_data = response.json()
        return response_data, response.status_code

    #You forgot to account for your potentially most probable case; the key's hash id falls between two fingers, neither of which 
    #is a first finger.  Account for that case here.    
    return json.dumps({"message":"Server is down!"}), 400     

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

    return json.dumps({"here's state.lowest_hash_id":state.lowest_hash_id,"here's the key's hashed id":id, "here's the address that the key maps to in this node's ft":address}), 200


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
