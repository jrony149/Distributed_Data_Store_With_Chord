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

################################################ Helper Functions ###############################################################

def sender(location, extension, key, request_type, payload):
    resp = ""
    if request_type == "p":
        resp = requests.put(f'http://{location}/{extension}/{key}', json=payload, timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "g":
        resp = requests.get(f'http://{location}/{extension}/{key}', json = payload, timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "d":
        resp = requests.delete(f'http://{location}/{extension}/{key}', json = payload, timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "v":
        resp = requests.put(f'http://{location}/{extension}', json = payload, timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "s":
        resp = requests.get(f'http://{location}/{extension}',timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "dd":
        resp = requests.put(f'http://{location}/{extension}',timeout=120, headers = {"Content-Type": "application/json"})
    elif request_type == "r":
        resp = requests.get(f'http://{location}/{extension}',timeout=120, headers = {"Content-Type": "application/json"})

    resp_data = resp.json()
    return resp_data, resp.status_code

def optimized_send(key, payload):
    key_hash_id = state.maps_to(key)
    if len(state.finger_table) == 1:
        return sender(state.finger_table[0][1],"kvs/keys",key,"p")
    if len(state.finger_table) > 1:
        bounds = state.maps_to(key_hash_id)
        if bounds["upper bound"][2] == 1:
            pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
            if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]: return sender(bounds["upper bound"][1],"store_key",key,"p",payload)
            else: return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)
        else: return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)

def full_send(new_view, route):
    global state
    fail_flag = False
    delete_dict = copy.deepcopy(state.storage)
    app.logger.info("Here's new_view from full_send(): " + str(new_view))
    for key in delete_dict:
        payload = {"value":state.storage[key],"address":state.address}
        del state.storage[key]
        response = sender(new_view[0],route,key,"p",payload)
        if response[1] != 200 and response[1] != 201: fail_flag = True
    if fail_flag: return json.dumps({"message":"redistribution of data unsuccessful."}), 500            
    return json.dumps({"message":"redistribution of data successful."}), 200
    
################################################### View Change Endpoints #######################################################################

@app.route('/kvs/view-change', methods=['PUT'])
def view_change():

    global state
    view_str = request.get_json()["view"]
    view_list = sorted(view_str.split(','))
    payload = {"view":view_list}

    state_view_set = set(state.view)
    new_view_set   = set(view_list)
    broadcast_set  = new_view_set.union(state_view_set)

    app.logger.info("Here's broadcast_set from view_change: " + str(broadcast_set))

    # broadcasting the view
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(sender,address,"view-change-action",None,"v",payload) for address in broadcast_set]
    result_collection = [f.result() for f in futures]
    for x in range(len(result_collection)):
        if result_collection[x][1] != 200:
            return json.dumps({"message":"View change unsuccessful."}), 500
    # polling the servers to display the finger tables of the nodes
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(sender,address,"recon",None,"r",{}) for address in broadcast_set]
    result_collection = [f.result() for f in futures]
    for x in range(len(result_collection)):
        app.logger.info(str(result_collection[x][0]) + "\n")
    # triggering the redistribution of data
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(sender,address,"distribute_data",None,"dd",{}) for address in broadcast_set]
    result_collection = [f.result() for f in futures]
    for x in range(len(result_collection)):
        if result_collection[x][1] != 200:
            return json.dumps({"message":"View change unsuccessful."}), 500
    #polling the shards and gathering the key-counts
    shards = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(sender,view_list[x],"kvs/key-count",None,"s",None) for x in range(len(view_list))]
    result_collection = [f.result() for f in futures]
    for x in range(len(result_collection)):
        shards.append({"address":view_list[x],"key-count":result_collection[x][0]["key-count"]})
        #app.logger.info("here's result " + str(x) + " " + str(result_collection[x]))
    return json.dumps({"message":"View change successful","shards":shards}), 200

@app.route('/view-change-action', methods=['PUT'])
def view_change_action():
    global state
    new_view = request.get_json()["view"]
    state.previous_view = copy.deepcopy(state.view)
    state.view = copy.deepcopy(new_view)
    state.finger_table.clear()
    if state.address in state.view: state.gen_finger_table(new_view)
    return json.dumps({"message":"View received and finger table generated."}), 200

@app.route('/distribute_data', methods=['PUT'])
def data_distribute():

    global state
    return full_send(state.view, "kvs/keys")
    # set_of_ids_and_preds,fail_flag = [],False
    # if state.view == state.previous_view:
    #     return json.dumps({"message":"View change successful."}), 200
    # if len(state.view) == 1:
    #     return full_send(state.view,"store_key")
    # if state.address not in state.view:
    #     app.logger.info("state.view from state.address not in state.view: " + str(state.view)) 
    #     return full_send(state.view,"kvs/keys")
    # if state.address in state.view:
    #     app.logger.info("Hello from state.address in state.view")
    #     for x in range(len(state.list_of_local_ids_and_preds)):
    #          if state.list_of_local_ids_and_preds[x] in state.previous_local_ids_and_preds:
    #              set_of_ids_and_preds.append(state.list_of_local_ids_and_preds[x])
    #     if len(set_of_ids_and_preds) == 0:
    #         delete_dict = copy.deepcopy(state.storage)
    #         for key in delete_dict:
    #             payload = {"value":state.storage[key]}
    #             #optimized_send(key,payload)
    #             response = sender(state.address,"kvs/keys",key,"p",payload)
    #             del state.storage[key]
    #             if response[1] != 200 and response[1] != 201: fail_flag = True
    #     if len(set_of_ids_and_preds) >= 1:
    #         delete_dict = copy.deepcopy(state.storage)
    #         for key in delete_dict:
    #             key_hash_id = state.hash_key(key)
    #             result = state.find_range(key_hash_id, set_of_ids_and_preds)
    #             if not result:
    #                 payload = {"value":state.storage[key]}
    #                 del state.storage[key]
    #                 #response = optimized_send(key, payload)
    #                 response = sender(state.address,"kvs/keys",key,"p",payload)
    #                 if response[1] != 200 and response[1] != 201: fail_flag = True
    #    if fail_flag: return json.dumps({"message":"Redistribution of data unsuccessful."}), 500
    #    return json.dumps({"message":"redistribution of data successful."}), 200 




 ########################################### Key Value Store Endpoints ######################################################

@app.route('/kvs/keys/<key>', methods=['PUT'])
def handle_put(key):
    app.logger.info("HEre is key from handle_put: " + key)

    data = request.get_json()
    if "value" not in data: return json.dumps({"error":"Value is missing","message":"Error in PUT"}), 400
    if len(key) > 50 : return json.dumps({"error":"Key is too long","message":"Error in PUT"}), 400

    global state 
    key_hash_id,payload               = state.hash_key(key),{}
    payload["value"],address_present  = data["value"],"address" in data
    payload["address"]                = data["address"] if address_present else state.address
    if state.single_node_view_address != None:
        app.logger.info("Hello from 1") 
        return sender(state.single_node_view_address,"store_key",key,"p",payload)
    if key_hash_id in state.set_of_local_ids:
        app.logger.info("Hello from 2") 
        return sender(state.address,"store_key",key,"p",payload) 
    if key_hash_id < state.finger_table[0][0]:
        app.logger.info("Hello from 3")
        if (key_hash_id < state.lowest_hash_id) and (key_hash_id < state.predecessor[0]) and (state.predecessor[0] > state.lowest_hash_id):
            app.logger.info("Hello from 3_1")
            return sender(state.address,"store_key",key,"p",payload)
        elif key_hash_id < state.lowest_hash_id:
            app.logger.info("Hello from 3_2") 
            return sender(state.predecessor[1],"kvs/keys",key,"p",payload)
        else:
            app.logger.info("Hello from 3_3") 
            return sender(state.finger_table[0][1],"store_key",key,"p",payload)
    if key_hash_id > state.finger_table[-1][0]:
        app.logger.info("Hello from 3_4") 

        if state.finger_table[-1][0] == state.max_address[0]: 
            app.logger.info("Hello from 3_4_1")
            return sender(state.min_address[1],"store_key",key,"p",payload)
        else:
            app.logger.info("Hello from 3_4_2")
            return sender(state.finger_table[-1][1],"kvs/keys",key,"p",payload)

    if len(state.finger_table) > 1:
        app.logger.info("hello from 4")
        bounds = state.maps_to(key_hash_id)
        if bounds["upper bound"][2] == 1:
            app.logger.info("Hello from 4_1")
            pred_of_first_finger = state.immediate_pred(bounds["upper bound"][0])
            if key_hash_id > pred_of_first_finger and key_hash_id <= bounds["upper bound"][0]:
                app.logger.info("Hello from 4_1_1")
                return sender(bounds["upper bound"][1],"store_key",key,"p",payload)
            elif key_hash_id > pred_of_first_finger and bounds["upper bound"][0] < bounds["lower bound"][0]:
                app.logger.info("Hello from 4_1_2")
                return sender(bounds["upper bound"][1],"store_key",key,"p",payload)
            else:
                app.logger.info("Hello from 4_1_3") 
                return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)
        else:
            app.logger.info("Hello from 4_2") 
            return sender(bounds["lower bound"][1],"kvs/keys",key,"p",payload)
    else:
        app.logger.info("Hello from 5") 
        return sender(state.finger_table[0][1],"kvs/keys",key,"p",payload)
    return json.dumps({"message":"Server is down!"}), 400   

@app.route('/kvs/keys/<key>', methods=['GET'])
def handle_get(key):
    
    global state 
    key_hash_id,payload,data = state.hash_key(key),{},request.get_json()

    address_present = "address" in data
    payload["address"] = data["address"] if address_present else state.address

    if state.single_node_view_address != None: return sender(state.single_node_view_address,"store_key",key,"g",payload)
    if key_hash_id in state.set_of_local_ids: return sender(state.address,"store_key",key,"g",payload)
    #if key_hash_id > state.lowest_hash_id and state.finger_table[0][0] < state.lowest_hash_id:
    #    return sender(state.finger_table[0][1],"store_key",key,"g",payload)
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
    key_hash_id,payload,data = state.hash_key(key),{},request.get_json()

    address_present = "address" in data
    payload["address"] = data["address"] if address_present else state.address

    if state.single_node_view_address != None: return sender(state.single_node_view_address,"store_key",key,"d",payload)

    if key_hash_id in state.set_of_local_ids: return sender(state.address,"store_key",key,"d",payload)
    #if key_hash_id > state.lowest_hash_id and state.finger_table[0][0] < state.lowest_hash_id:
    #    return sender(state.finger_table[0][1],"store_key",key,"d",payload)
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
    if data["address"] == state.address: return json.dumps({"message":message,"replaced":replace}), status_code
    else: return json.dumps({"message":message,"replaced":replace,"address":state.address}), status_code

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
    global state
    array = []
    state.cl.moveHead()
    
    for _ in range(state.cl.getLength()):
        array.append(state.cl.getCursorData())
        state.cl.moveNext()
    return json.dumps({"local address":state.address,"linked list data":array,"length of linked list":state.cl.getLength(),"state.view":state.view, "finger table":state.finger_table,"length of finger table":len(state.finger_table),"map":state.map,"state.storage":state.storage})

@app.route('/data_request/<key>', methods=['GET'])
def get(key):
    id = state.hash_key(key)
    #address = state.maps_to(id)

    return json.dumps({"key hash id":id,"lowest_hash_id":state.lowest_hash_id,"lowest hash id's predecessor":state.predecessor,"here's the key_local_ids_and_preds":state.list_of_local_ids_and_preds}), 200

@app.route('/view_request', methods=["GET"])
def return_view():
    global state
    return json.dumps({"state.view":state.view}), 200
    
@app.route('/kvs/key-count', methods=['GET'])
def count():
    global state
    return json.dumps({"message":"Key count retrieved successfully","key-count":len(state.storage.keys())}), 200 

@app.route('/all_hash_ids', methods=["GET"])
def hash_ids():
    global state
    hash_id_dict = {}
    for key in state.storage:
        hash_id = state.hash_key(key)
        hash_id_dict[key] = hash_id
    return json.dumps({"local node address: ":state.address,"all hash ids present in this node's storage: ":hash_id_dict})


# @app.route('/view-change-action', methods=['PUT'])
# def view_change_action():
#     global state
#     set_of_ids_and_preds,fail_flag= [],False
#     new_view,incoming_address = request.get_json()["view"],request.get_json()["address"]
#     if new_view == state.view: #If the new view == old view, no work necessary.  We're done.
#         return json.dumps({"view change successful":"success!"}),200
#     if len(new_view) == 1: #If the new view only consists of a single node, just send everything there to be stored.
#         return full_send(new_view,"store_key")
#     if state.address not in new_view: #If local node not in new view, no finger table generation necessary.  
#                                       #Just send everything to be ping pong'd starting at first node of new view.
#         return full_send(new_view,"kvs/keys")
#     if state.address in new_view: #Now the work comes in.
#         if incoming_address != state.address: state.view = new_view
#         previous_ids_and_preds = copy.deepcopy(state.list_of_local_ids_and_preds)
#         state.finger_table.clear()
#         state.gen_finger_table(new_view)
#         for x in range(len(state.list_of_local_ids_and_preds)):
#             if state.list_of_local_ids_and_preds[x] in previous_ids_and_preds:
#                 set_of_ids_and_preds.append(state.list_of_local_ids_and_preds[x])
#         if len(set_of_ids_and_preds) == 0:#None of the intervals that existed in the old view's ring exist in the new view's ring.
#                                           #Use the finger table as best you can and send it all.
#             for key in state.storage:
#                 payload = {"value":state.storage[key]}
#                 #optimized_send(key,payload)
#                 response = sender(state.address,"kvs/keys",keys,"p",payload)
#                 if response[1] != 200 and response[1] != 201: fail_flag = True
#         if len(set_of_ids_and_preds) >= 1:#At least one interval has been carried over from the old view.  We must check to see 
#                                           #what we can keep and what we must send out.
#             delete_dict = copy.deepcopy(state.storage)
#             for key in delete_dict:
#                 key_hash_id = state.hash_key(key)
#                 result = state.find_range(key_hash_id, set_of_ids_and_preds)
#                 if not result:
#                     payload = {"value":state.storage[key]}
#                     del state.storage[key]
#                     #response = optimized_send(key, payload)
#                     response = sender(state.address,"kvs/keys",key,"p",payload)
#                     if response[1] != 200 and response[1] != 201: fail_flag = True
#         if fail_flag: return json.dumps({"message":"Redistribution of data unsuccessful."}), 500
#         return json.dumps({"message":"redistribution of data successful."}), 200 
