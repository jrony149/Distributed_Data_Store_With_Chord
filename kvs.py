from app import app
from flask import request
import json
import requests
from state import State
import logging
import sys
import _thread
import threading 
import copy
import concurrent.futures

global state
@app.before_first_request
def build_state():
    global state
    state = State()

def sender(location, extension, key, request_type, payload):
    resp = ""
    if request_type == "p":
        resp = requests.put(f'http://{location}/{extension}/{key}', json=payload, timeout=6, headers = {"Content-Type": "application/json"})
    elif request_type == "g":
        resp = requests.get(f'http://{location}/{extension}/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
    elif request_type == "d":
        resp = requests.delete(f'http://{location}/{extension}/{key}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
    elif request_type == "v":
        resp = requests.put(f'http://{location}/{extension}', json = payload, timeout=6, headers = {"Content-Type": "application/json"})
    elif request_type == "s":
        resp = requests.get(f'http://{location}/{extension}',timeout=6, headers = {"Content-Type": "application/json"})
    resp_data = resp.json()
    return resp_data, resp.status_code

################################################### Main Endpoints #######################################################################

@app.route('/kvs/view-change', methods=['PUT'])
def view_change():

    view_str = request.get_json()["view"]
    view_list = sorted(view_str.split(','))

    payload = {"view":view_list}

    threads = []
    for address in range(len(state.view)):
        threads.append(threading.Thread(target=sender, args=(state.view[address],"view-change-action",None,"v",payload)))
        threads[-1].start()
    for thread in threads:
        thread.join()
    threads.clear()

    shards = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(sender,view_list[x],"kvs/key-count",None,"s",None) for x in range(len(view_list))]
    result_collection = [f.result() for f in futures]
    for x in range(len(result_collection)):
        shards.append({"address":view_list[x],"key-count":result_collection[x][0]["key-count"]})
        app.logger.info("here's result " + str(x) + " " + str(result_collection[x][0]["key-count"]))

    return json.dumps({"message":"View change successful","shards":shards}), 200



    #Here is where you need to poll each node in the new view, gather their key counts,
    #and return this information to the client.


@app.route('/view-change-action', methods=['PUT'])
def view_change_action():
    new_view = request.get_json()["view"]
    if new_view == state.view:
        return json.dumps({"view change successful":"success!"}),200

    previous_ids_and_preds = copy.deepcopy(state.list_of_local_ids_and_preds)

    state.gen_finger_table(new_view)

    app.logger.info("Here's the finger table after view change: " + str(state.finger_table))

    app.logger.info("\n")

    app.logger.info("previous ids and preds: " + str(previous_ids_and_preds) + "\n")
    app.logger.info("current ids and preds: " + str(state.list_of_local_ids_and_preds))


    #here is where you need to perform your necessary view change actions - 
    #including checking to see if the local node needs to have its keys re-sent
    #and if it does, re-sending them.  In the /kvs/view-change function is where the
    #nodes in the new view will be polled and the key-count of each shard will be returned to the client 
    



@app.route('/kvs/keys/<key>', methods=['PUT'])
def handle_put(key):

    data = request.get_json()
    if "value" not in data: return json.dumps({"error":"Value is missing","message":"Error in PUT"}), 400
    if len(key) > 50 : return json.dumps({"error":"Key is too long","message":"Error in PUT"}), 400

    global state 
    key_hash_id,payload               = state.hash_key(key),{}
    payload["value"],address_present  = data["value"],"address" in data
    payload["address"]                = data["address"] if address_present else state.address 
    if key_hash_id in state.set_of_local_ids: return sender(state.address,"store_key",key,"p",payload)
    if key_hash_id < state.finger_table[0][0]:
        if (key_hash_id < state.lowest_hash_id) and (key_hash_id < state.predecessor[0]) and (state.predecessor[0] > state.lowest_hash_id):
            return sender(state.address,"store_key",key,"p",payload)
        elif key_hash_id < state.lowest_hash_id: return sender(state.predecessor[1],"kvs/keys",key,"p",payload)
        else: return sender(state.finger_table[0][1],"store_key",key,"p",payload)
    if key_hash_id > state.finger_table[-1][0]: return sender(state.finger_table[-1][1],"kvs/keys",key,"p",payload)
    if len(state.finger_table) > 1:
        bounds = state.maps_to(key_hash_id)
        if bounds["upper bound"][2] == 1:
            pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
            if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]: return sender(bounds["upper bound"][1],"store_key",key,"p",payload)
            else: return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)
        else: return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)
    else: return sender(state.finger_table[0][1],"kvs/keys",key,"p",payload)
  
    return json.dumps({"message":"Server is down!"}), 400   

@app.route('/kvs/keys/<key>', methods=['GET'])
def handle_get(key):
    
    global state 
    key_hash_id,payload,data,json_present = state.hash_key(key),{},{},True

    try: data                           = request.get_json()
    except Exception as e: json_present = False

    if not json_present: payload["address"] = state.address
    if json_present: payload["address"]     = data["address"] 
    
    if key_hash_id in state.set_of_local_ids: return sender(state.address,"store_key",key,"g",payload)
    if key_hash_id < state.finger_table[0][0]:
        if (key_hash_id < state.lowest_hash_id) and (key_hash_id < state.predecessor[0]) and (state.predecessor[0] > state.lowest_hash_id):
            return sender(state.address,"store_key",key,"g",payload)
        elif key_hash_id < state.lowest_hash_id: return sender(state.predecessor[1],"kvs/keys",key,"g",payload)
        else: return sender(state.finger_table[0][1],"store_key",key,"g",payload)
    if key_hash_id > state.finger_table[-1][0]: return sender(state.finger_table[-1][1],"kvs/keys",key,"g",payload)
    if len(state.finger_table) > 1:
        bounds = state.maps_to(key_hash_id)
        if bounds["upper bound"][2] == 1:
            pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
            if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]: return sender(bounds["upper bound"][1],"store_key",key,"g",payload)
            else: return sender(bounds["lower bound"][1],"kvs/keys",key,"g",payload)
        else: return sender(bounds["lower bound"][1],"kvs/keys",key,"g",payload)
    else: return sender(state.finger_table[0][1],"kvs/keys",key,"g",payload)
  
    return json.dumps({"message":"Server is down!"}), 400

@app.route('/kvs/keys/<key>', methods=['DELETE'])
def handle_delete(key):

    global state 
    key_hash_id,payload,data,json_present = state.hash_key(key),{},{},True

    try: data = request.get_json()
    except Exception as e: json_present = False

    if not json_present: payload["address"] = state.address
    if json_present: payload["address"]     = data["address"] 
    
    if key_hash_id in state.set_of_local_ids: return sender(state.address,"store_key",key,"d",payload)
    if key_hash_id < state.finger_table[0][0]:
        if (key_hash_id < state.lowest_hash_id) and (key_hash_id < state.predecessor[0]) and (state.predecessor[0] > state.lowest_hash_id):
            return sender(state.address,"store_key",key,"d",payload)
        elif key_hash_id < state.lowest_hash_id: return sender(state.predecessor[1],"kvs/keys",key,"d",payload)
        else: return sender(state.finger_table[0][1],"store_key",key,"d",payload)
    if key_hash_id > state.finger_table[-1][0]: return sender(state.finger_table[-1][1],"kvs/keys",key,"d",payload)
    if len(state.finger_table) > 1:
        bounds = state.maps_to(key_hash_id)
        if bounds["upper bound"][2] == 1:
            pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
            if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]: return sender(bounds["upper bound"][1],"store_key",key,"d",payload)
            else: return sender(bounds["lower bound"][1],"kvs/keys",key,"d",payload)
        else: return sender(bounds["lower bound"][1],"kvs/keys",key,"d",payload)
    else: return sender(state.finger_table[0][1],"kvs/keys",key,"d",payload)
  
    return json.dumps({"message":"Server is down!"}), 400


######################### Storage Management Endpoints (final destinations muahahaha) ##################################


@app.route('/store_key/<key>', methods=['PUT'])
def store(key):
    global state
    data               = request.get_json()
    replace            = key in state.storage
    message            = "Updated successfully" if replace else "Added successfully"
    status_code        = 200 if replace else 201
    state.storage[key] = data["value"]
    if data["address"] == state.address: return json.dumps({"message":message,"replace":replace}), status_code
    else: return json.dumps({"message":message,"replace":replace,"address":state.address}), status_code

@app.route('/store_key/<key>', methods=['GET'])
def retrieve(key):
    global state
    data = request.get_json()
    data_present = key in state.storage
    if data_present: 
        if data["address"] == state.address:
            return json.dumps({"doesExist":True, "message":"Retrieved successfully", "value": state.storage[key]}), 200
        return json.dumps({"doesExist":True,"message":"Retrieved successfully","value":state.storage[key],"address":state.address}) 
    return json.dumps({"doesExist":False,"error":"Key does not exist","message":"Error in GET"}), 404

@app.route('/store_key/<key>', methods=['DELETE'])
def delete(key):
    global state
    data = request.get_json()
    data_present = key in state.storage
    if data_present: 
        del state.storage[key]
        if data["address"] == state.address:
            return json.dumps({"doesExist":True, "message":"Deleted successfully"}), 200
        return json.dumps({"doesExist":True,"message":"Deleted successfully","address":state.address}) 
    return json.dumps({"doesExist":False,"error":"Key does not exist","message":"Error in DELETE"}), 404


######################################## Administrative Endpoints ##############################################


@app.route('/recon', methods=['GET'])
def recon():
    array = []
    state.cl.moveHead()
    
    for _ in range(state.cl.getLength()):
        array.append(state.cl.getCursorData())
        state.cl.moveNext()
    return json.dumps({"linked list data":array,"length of linked list":state.cl.getLength(), "finger table":state.finger_table, "local store":state.storage, "map":state.map})

@app.route('/data_request/<key>', methods=['GET'])
def get(key):
    id = state.hash_key(key)
    #address = state.maps_to(id)

    return json.dumps({"lowest_hash_id":state.lowest_hash_id,"lowest hash id's predecessor":state.predecessor,"here's the key_local_ids_and_preds":state.list_of_local_ids_and_preds}), 200

@app.route('/view_request', methods=["GET"])
def return_view():
    global state
    return json.dumps({"state.view":state.view}), 200
    
@app.route('/kvs/key-count', methods=['GET'])
def count():
    global state
    return json.dumps({"message":"Key count retrieved successfully","key-count":len(state.storage.keys())}), 200   
