from requests.exceptions import Timeout
import requests
import asyncio
import json
import random
import string
import time
import sys

#list of relevant ports
listOfPorts = ["13801", "13802", "13803"]

#list of relevant url's
url         = "http://localhost:13801/kvs/view-change"
url1        = "http://localhost:13802/kvs/view-change"
url2        = "http://localhost:13803/kvs/view-change"

url3        = "http://localhost:13801/kvs/key-count"
url4        = "http://localhost:13802/kvs/key-count"
url5        = "http://localhost:13803/kvs/key-count"

url6        = "http://localhost:13801/kvs/keys/"
url7        = "http://localhost:13802/kvs/keys/"
url8        = "http://localhost:13803/kvs/keys/"

def get_random_string(length):
    letters    = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def tallyKeys(portList):

    headers   = {'content-type':'application/json'}
    sumOfKeys = 0

    for x in range(len(portList)):

        try:
            response  = requests.get(f'http://localhost:{portList[x]}/kvs/key-count', timeout=2, headers=headers)
            keyData   = response.json()
            keyCount  = keyData["key-count"]
            sumOfKeys = sumOfKeys + keyCount
            print("This is portList[x]:")
            print(str(portList[x]))
            print("This is response:")
            print(response)
            print("This is keyCount " + str(keyCount))
        except requests.exceptions.Timeout:
            print("server at local host " + portList[x] + " timed out.")

    return sumOfKeys

def sendViewChange(view, address):

    headers   = {'content-type':'application/json'}
    payload   = json.dumps(view)

    try:
        requests.put(address, headers=headers, data=payload)
    except requests.exceptions.ConnectionError as e:
        print("server at local host " + adress + " timed out.")

async def sendKeys(numOfKeys, address):

    headers   = {'content-type':'application/json'}

    for x in range(numOfKeys):
        key   = get_random_string(8)
        value = get_random_string(8)

        target  = address + key
        payload = json.dumps({"value":value})

        try:
            requests.put(target, data=payload, headers=headers, timeout=2)
        except requests.exceptions.Timeout:
            print("server at local host " + target + " timed out.")

        await asyncio.sleep(0.01)

async def tests():

    sumOfAllKeys        = 0
    sumOfAllKeys2       = 0
    numOfKeysOnDeadNode = 0

    #throwing data at servers
    if( int(sys.argv[1]) == 1):
        await asyncio.wait([ sendKeys(300, url6), sendKeys(300, url7), sendKeys(300, url8) ])
    if( int(sys.argv[1]) == 2):
        await asyncio.wait([ sendKeys(300, url6), sendKeys(300, url6), sendKeys(300, url6) ])
    if( int(sys.argv[1]) == 3):
        await asyncio.wait([ sendKeys(300, url6), sendKeys(300, url6), sendKeys(300, url6) ])
        
    #summing all the keys in the system before view change request
    if( int(sys.argv[1]) == 1):
        sumOfAllKeys = tallyKeys(listOfPorts)
    if( int(sys.argv[1]) == 2):
        sumOfAllKeys = tallyKeys([listOfPorts[0]])
    if( int(sys.argv[1]) == 3):
        sumOfAllKeys = tallyKeys([listOfPorts[0]])

    #issuing view change
    if ( int(sys.argv[1]) == 1):
        sendViewChange({"view":"10.10.0.4:13800"}, url)
        time.sleep(20)
    if ( int(sys.argv[1]) == 2):
        sendViewChange({"view":"10.10.0.5:13800"}, url)
        time.sleep(20)
    if ( int(sys.argv[1]) == 3):
        sendViewChange({"view":"10.10.0.5:13800,10.10.0.6:13800"}, url)
        time.sleep(20)

    #summing all the keys in the system after view change request
    if ( int(sys.argv[1]) == 1):
        sumOfAllKeys2 = tallyKeys([listOfPorts[0]])
        numOfKeysOnDeadNode = tallyKeys([ listOfPorts[1], listOfPorts[2] ])
    if ( int(sys.argv[1]) == 2):
        sumOfAllKeys2 = tallyKeys([listOfPorts[1]])
        numOfKeysOnDeadNode = tallyKeys( [listOfPorts[0]] )
    if ( int(sys.argv[1]) == 3):
        sumOfAllKeys2 = tallyKeys([ listOfPorts[1], listOfPorts[2] ])
        numOfKeysOnDeadNode = tallyKeys( [listOfPorts[0]] )
    
    if( int(sys.argv[1]) == 1):
        if (sumOfAllKeys != sumOfAllKeys2):
            print("###########################################################################################")
            print("{x, y, z} -> {x} test failed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys collectively stored on the dead nodes y and z is: " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")
        else:
            print("###########################################################################################")
            print("{x, y, z} -> {x} test passed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys stored collectively on the dead nodes y and z is: " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")

    if( int(sys.argv[1]) == 2):

        if (sumOfAllKeys != sumOfAllKeys2):
            print("###########################################################################################")
            print("{x} -> {y} test failed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys stored on the dead node x is: " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")
        else:
            print("###########################################################################################")
            print("{x} -> {y} test passed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys stored on the dead node x is: " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")

    if( int(sys.argv[1]) == 3):

        if (sumOfAllKeys != sumOfAllKeys2):
            print("###########################################################################################")
            print("{x} -> {y, z} test failed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys stored on the dead node x is " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")
        else:
            print("###########################################################################################")
            print("{x} -> {y, z} test passed.")
            print("The sum of keys stored on the system before view change request was: " + str(sumOfAllKeys))
            print("The sum of keys stored on the system after view change request is: " + str(sumOfAllKeys2))
            print("The number of keys stored on the dead node x is: " + str(numOfKeysOnDeadNode))
            print("###########################################################################################")

async def main():

    await tests()


loop =asyncio.get_event_loop()
loop.run_until_complete(main())
    





    




