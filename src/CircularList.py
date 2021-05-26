#Represents the node of list.    
class Node:    
    def __init__(self,data):    
        self.data = data    
        self.next = None
        self.previous = None    
     
class CreateList:    
    #Declaring head and tail pointer as null.    
    def __init__(self):    
        self.head = Node(None)    
        self.tail = Node(None)    
        self.head.next = self.tail    
        self.tail.next = self.head 
        self.tail.previous = self.head
        self.head.previous = self.tail 
        self.cursor = -1
        self.length = 0
    
    def moveHead(self):
        self.cursor = self.head 
    
    def moveTail(self):
        self.cursor = self.tail
    
    def moveNext(self):
        self.cursor = self.cursor.next 
    
    def movePrevious(self):
        self.cursor = self.cursor.previous
    
    def getCursorData(self):
        if self.cursor != -1:
            return self.cursor.data

    def getLength(self):
        return self.length
        
    #This function will add the new node at the end of the list.    
    def add(self,data):    
        newNode = Node(data)    
        #Checks if the list is empty.    
        if self.head.data is None:    
            #If list is empty, both head and tail would point to new node.    
            self.head = newNode    
            self.tail = newNode    
            self.length += 1    
        else:    
            #tail will point to new node.    
            self.tail.next = newNode 
            newNode.previous = self.tail 
            #New node will become new tail.    
            self.tail = newNode  
            #Since, it is circular linked list tail will point to head.    
            self.tail.next = self.head
            self.head.previous = self.tail
            self.length += 1  
   
     
  #Displays all the nodes in the list    
    def display(self):    
        current = self.head    
        if self.head is None:    
            print("List is empty")    
            return    
        else:    
            print("Nodes of the circular linked list: ")    
            #Prints each node by incrementing pointer.    
            print(current.data),    
            while(current.next != self.head):    
                current = current.next    
                print(current.data),

    def deleteAll(self):
        self.head = Node(None)    
        self.tail = Node(None)    
        self.head.next = self.tail    
        self.tail.next = self.head 
        self.tail.previous = self.head
        self.head.previous = self.tail 
        self.cursor = -1
        self.length = 0
        # temp = self.head    
        # if temp is None:
        #     print("\n Not possible to delete empty list.")
        # while temp:
        #     self.head = temp.next
        #     temp.data = None
        #     temp = None
        #     temp = self.head
        #     if temp == self.tail:
        #         temp.data = None
        #         temp = None
        #         break

    def findID(self, searchID):

        self.cursor = self.head
        while(self.cursor.data[0] != searchID):
            self.cursor = self.cursor.next

    def findAllIdsWithSearchAddr(self, searchAddr):

        returnList = []
        self.cursor = self.head
        for x in range(self.length):
            if self.cursor.data[1] == searchAddr:
                returnList.append(self.cursor.data[0])
            self.cursor = self.cursor.next
        return returnList
        

     
     
# class CircularLinkedList:    
#   cl = CreateList()    
#   #Adds data to the list    
#   cl.add(1)    
#   cl.add(2)    
#   cl.add(3)    
#   cl.add(4)  
#   #Displays all the nodes present in the list    
#   cl.display()
#   cl.deleteAll()
#   cl.display()
