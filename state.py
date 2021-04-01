from app import app
from hashlib import sha1
import os
import requests
import random
import threading
from CircularList import CreateList
import math

class State():
    def __init__(self):
        # self.cl circular linked list for use in generating finger table
        self.cl = CreateList()
        # self.view is the latest view. List of sorted addresses.
        self.view = sorted(os.environ.get('VIEW').split(','))
        # self.num_of_fingers_and_virtual_nodes - variable that determines the number of entries the local node's
        # finger table will have.
        self.num_of_fingers_and_virtual_nodes = 0  #int(math.log(len(self.view), 2))
        # self.address is address of the current node.
        self.address = os.environ.get('ADDRESS')
        # self.map stores the hash value to address mapping.
        self.map = {}
        # self.finger_table stores "fingers" of the local node
        self.finger_table = []
        # list_of_local_ids a list of all the ids that correspond with the local node's address
        self.list_of_local_ids = []
        self.set_of_local_ids = set()
        self.lowest_hash_id = ""
        # The number of node replica for one address.
        #self.node_replica = self.num_of_fingers_and_virtual_nodes
        # the total number of keys in the map
        self.indices = []
        # The primary kv store.
        self.storage = {}

        self.gen_finger_table(self.view)
        #self.data_structure_clear()
    
    def gen_finger_table(self, view):
        self.num_of_fingers_and_virtual_nodes = int(math.log(len(view), 2))
        for address in view:
            self.hash_and_store_address(address)
        self.indices = sorted(self.map.keys())
        for x in range(len(self.indices)):#your circular linked list is populated
            self.cl.add([self.indices[x], self.map[self.indices[x]], 0]) #the third element is a marker to help determine where
                                                                         #where the first finger of each v-node points to.
        self.list_of_local_ids = sorted([key for key in self.map if self.map[key] == self.address])
        self.set_of_local_ids  = set(self.list_of_local_ids)
        for x in range(len(self.list_of_local_ids)):
            self.cl.findID(self.list_of_local_ids[x])
            for y in range(self.num_of_fingers_and_virtual_nodes):
                for z in range(2**y):
                    self.cl.moveNext()
                element_to_append = self.cl.getCursorData()
                if y == 0: element_to_append[2] = 1
                self.finger_table.append(element_to_append)
                self.cl.findID(self.list_of_local_ids[x])

        self.finger_table = sorted(self.finger_table)#sorting the finger table to prep it for binary search.

    def maps_to(self, key):
        return_dict = {}
        #app.logger.info("Here's key: " + str(key))
        if key < self.finger_table[0][0]: #In this case, send it to the store of self.finger_table[0](meaning
            #send it to the upper bound)
            return_dict["upper bound"] = self.finger_table[0]
            return_dict["lower bound"] = None
            return return_dict
        if key > self.finger_table[-1][0]: #In this case, send it to the GET or PUT addr of self.finger_table[-1]
            return_dict["lower bound"] = self.finger_table[-1]
            return_dict["upper bound"] = None 
            return return_dict

        low,mid,high = 0,0,(len(self.finger_table) - 1)
        
        while low <= high:
            mid = (high + low) // 2
            if self.finger_table[mid][0] <= key and self.finger_table[mid+1][0] >= key:
                return_dict["lower bound"] = self.finger_table[mid]
                return_dict["upper bound"] = self.finger_table[mid+1]
                return return_dict
            elif self.finger_table[mid][0] < key:
                low = mid + 1
            elif self.finger_table[mid][0] > key:
                high = mid - 1
        # This should be unreachable
        return_dict["upper bound"] = None
        return_dict["lower bound"] = None
        return return_dict

    def immediate_pred(self, first_finger):

        if first_finger >= self.list_of_local_ids[-1]:
            return self.list_of_local_ids[-1]

        low,mid,high = 0,0,(len(self.list_of_local_ids) - 1)
        while low <= high:
            mid = (high + low) // 2
            if self.list_of_local_ids[mid] <= first_finger and self.list_of_local_ids[mid+1] >= first_finger:
                return self.list_of_local_ids[mid] 
            elif self.list_of_local_ids[mid] < first_finger:
                low = mid + 1
            elif self.list_of_local_ids[mid] > first_finger:
                high = mid - 1
        #this should be unreachable
        return -1
            



    def data_structure_clear(self):
        self.cl.deleteAll()
        self.map.clear()
        self.list_of_local_ids.clear()
        self.indices.clear()

    def hash_and_store_address(self, address): #create "self.num_of_fingers_and_virtual_nodes" number of
                                               #v-nodes, unless the [floor of log(base 2) of the number of
                                               #actual up and running nodes] is 1.  In that case, just hash the address once
                                               # and add it to the map. 
        hash = State.hash_key(address)
        if address == self.address: self.lowest_hash_id = hash
        if(self.num_of_fingers_and_virtual_nodes > 1):
            for _ in range((self.num_of_fingers_and_virtual_nodes) - 1):
                self.map[hash] = address
                hash = State.hash_key(hash)
                if hash < self.lowest_hash_id: self.lowest_hash_id = hash
                self.map[hash] = address
        else:
            self.map[hash] = address


    

    @staticmethod
    def hash_key(key):
        return sha1(key.encode('utf-8')).hexdigest()