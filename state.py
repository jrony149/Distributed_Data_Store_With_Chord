from app import app
from hashlib import sha1
import os
import requests
import random
import threading
from CircularList import CreateList
import math
import copy

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
        # will be assigned to the address of the single node in the view if view only consists of single node.
        self.single_node_view_address = None
        # self.map stores the hash value to address mapping.
        self.map = {}
        # self.finger_table stores "fingers" of the local node
        self.finger_table = []
        # list_of_local_ids a list of all the ids that correspond with the local node's address
        self.list_of_local_ids = []
        #creating a set from the list of local ids to be used with the 'in' keyword for improved time complexity
        self.set_of_local_ids = set()
        #will be set by gen_finger_table() - this is the lowest hash id for the local node
        self.lowest_hash_id = ""
        # the total number of keys in the map
        self.indices = []
        # The primary kv store.
        self.storage = {}
        # the previous view to be compared to the current view by the distribute data function.
        self.previous_view = []
        # The previous list of local hash ids and their predecessors for use in determining if a key needs to be re-sent or not.
        self.previous_local_ids_and_preds = []
        # The current list of local hash ids and their predecessors for use in determining if a key needs to be re-sent or not.
        self.list_of_local_ids_and_preds = []
        # The predecessor hash id of the position on the ring just before the lowest hash id of the local node.
        self.predecessor = None
        # lowest index position on ring.
        self.min_address = None
        # greatest index position on ring.
        self.max_address = None
        # Generating the first finger table of the node upon startup.
        self.gen_finger_table(self.view)

    def gather_ids_and_preds(self):
        self.previous_local_ids_and_preds = copy.deepcopy(self.list_of_local_ids_and_preds)
        return_list = []
        for x in range(len(self.list_of_local_ids)):
            self.cl.findID(self.list_of_local_ids[x])
            self.cl.movePrevious()
            if self.list_of_local_ids[x] == self.lowest_hash_id:
                self.predecessor = self.cl.getCursorData()
            return_list.append([self.list_of_local_ids[x], self.cl.getCursorData()])
        return return_list
        
    def gen_finger_table(self, view):
        if len(view) == 1: 
            self.single_node_view_address = view[0]
            return 0
        self.single_node_view_address = None
        self.cl.deleteAll()
        self.num_of_fingers_and_virtual_nodes = int(math.log(len(view), 2))
        for address in view: self.hash_and_store_address(address)
        self.indices = sorted(self.map.keys())
        min_index,max_index = min(self.indices),max(self.indices)
        self.min_address,self.max_address = [min_index,self.map[min_index]],[max_index,self.map[max_index]]
        for x in range(len(self.indices)):#your circular linked list is populated
            self.cl.add([self.indices[x], self.map[self.indices[x]], 0]) #the third element is a marker to help determine where
        self.list_of_local_ids = sorted([key for key in self.map if self.map[key] == self.address])
        self.set_of_local_ids  = set(self.list_of_local_ids)
        self.list_of_local_ids_and_preds = self.gather_ids_and_preds()
        for x in range(len(self.list_of_local_ids)):
            self.cl.findID(self.list_of_local_ids[x])
            for y in range(self.num_of_fingers_and_virtual_nodes):
                for z in range(2**y):
                    self.cl.moveNext()
                element_to_append = self.cl.getCursorData()
                if y == 0: element_to_append[2] = 1
                self.finger_table.append(element_to_append)
                self.cl.findID(self.list_of_local_ids[x])
        temp_list = []
        [temp_list.append(x) for x in self.finger_table if x not in temp_list]
        self.finger_table = sorted(temp_list)
        self.data_structure_clear()
        return 0

    def maps_to(self, key):
        return_dict = {}
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
        if first_finger >= self.list_of_local_ids[-1]: return self.list_of_local_ids[-1]
        low,mid,high = 0,0,(len(self.list_of_local_ids) - 1)
        while low <= high:
            mid = (high + low) // 2
            if self.list_of_local_ids[mid] <= first_finger and self.list_of_local_ids[mid+1] >= first_finger: return self.list_of_local_ids[mid] 
            elif self.list_of_local_ids[mid] < first_finger: low = mid + 1
            elif self.list_of_local_ids[mid] > first_finger: high = mid - 1
        #this should be unreachable
        return -1

    def find_range(self, key_hash_id, list_of_ranges):

        if len(list_of_ranges) == 1:
            lower_bound,upper_bound = list_of_ranges[0][1][0],list_of_ranges[0][0]
            if key_hash_id > lower_bound and key_hash_id < upper_bound: return True
            elif key_hash_id > lower_bound and lower_bound > upper_bound: return True
            else:return False

        low,mid,high = 0,0,(len(list_of_ranges) - 1)
        while low <= high:
            mid,lower_bound,upper_bound = ((high + low) // 2),list_of_ranges[mid][1][0],list_of_ranges[mid][0]
            if key_hash_id > lower_bound and key_hash_id < upper_bound: return True
            elif key_hash_id > lower_bound and lower_bound > upper_bound: return True
            elif key_hash_id == list_of_ranges[mid][1][0]: return True
            elif key_hash_id < lower_bound:high = mid - 1
            elif key_hash_id > upper_bound:low = mid + 1
        return False

            
    def data_structure_clear(self):
        #self.cl.deleteAll()
        self.map.clear()
        #self.list_of_local_ids.clear()
        self.indices.clear()

    def hash_and_store_address(self, address):
        hash = State.hash_key(address)
        if address == self.address: self.lowest_hash_id = hash
        if(self.num_of_fingers_and_virtual_nodes > 1):
            for _ in range((self.num_of_fingers_and_virtual_nodes) - 1):
                self.map[hash] = address
                hash = State.hash_key(hash)
                if ((hash < self.lowest_hash_id) and (address == self.address)): self.lowest_hash_id = hash
                self.map[hash] = address
        else:
            self.map[hash] = address
    
    @staticmethod
    def hash_key(key):
        return sha1(key.encode('utf-8')).hexdigest()