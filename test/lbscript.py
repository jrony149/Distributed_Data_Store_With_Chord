from requests.exceptions import Timeout
import requests
import asyncio
import json
import random
import string
import time
import sys
from termcolor import colored
import time
import subprocess

#list of relevant ports
listOfPorts  = ["13801", "13802", "13803", "13804", "13805"]
portToVarMap = {"13801":"v","13802":"w","13803":"x","13804":"y","13805":"z"}

cExcellent   = colored('Excellent.', 'green')
cGood        = colored('Good.', 'yellow')
cFine        = colored('Fine.', 'magenta')
cImbalanced  = colored('Imbalanced.', 'red')   
#list of relevant url's

#view change url's
url         = "http://localhost:13801/kvs/view-change"
url1        = "http://localhost:13802/kvs/view-change"
url2        = "http://localhost:13803/kvs/view-change"
url3        = "http://localhost:13804/kvs/view-change"
url4        = "http://localhost:13805/kvs/view-change"

#key count url's
url5        = "http://localhost:13801/kvs/key-count"
url6        = "http://localhost:13802/kvs/key-count"
url7        = "http://localhost:13803/kvs/key-count"
url8        = "http://localhost:13804/kvs/key-count"
url9        = "http://localhost:13805/kvs/key-count"

#data op url's
url10       = "http://localhost:13801/kvs/keys/"
url11       = "http://localhost:13802/kvs/keys/"
url12       = "http://localhost:13803/kvs/keys/"
url13       = "http://localhost:13804/kvs/keys/"
url14       = "http://localhost:13805/kvs/keys/"

def get_random_string(length):
    letters    = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def keyCountAtNode(addr):

    headers  = {'content-type':'application/json'}
    response = requests.get(f'http://localhost:{addr}/kvs/key-count', timeout=2, headers=headers)                
    return response

def balanceQuality(portList):

    totalNumOfKeysInSystem = 100
    x                      = totalNumOfKeysInSystem/len(portList)
    returnString           = ""
    
    for port in range(len(portList)):

        try:
            #response  = requests.get(f'http://localhost:{portList[port]}/kvs/key-count', timeout=2, headers=headers)
            response  = keyCountAtNode(portList[port])
            keyData   = response.json()
            keyCount  = keyData["key-count"]
            if( (keyCount >= 0.9*x) and (keyCount <= 1.1*x) ):
                returnString = ""
                returnString = returnString + cExcellent
            if( ((keyCount > 0.8*x) and (keyCount < 0.9*x)) or ((keyCount > 1.1*x) and (keyCount < 1.2*x)) ):
                returnString = ""
                returnString = returnString + cGood
            if( ((keyCount > 0.6*x) and (keyCount < 0.8*x)) or ((keyCount > 1.2*x) and (keyCount < 1.4*x)) ):
                returnString = ""
                returnString = returnString + cFine
            if( (keyCount < 0.6*x) or (keyCount > 1.4*x) ):
                returnString = ""
                returnString = returnString + cImbalanced
            print("This is keyCount from server " + portToVarMap[portList[port]] + ": " + str(keyCount))
            
        except requests.exceptions.Timeout:
            print("server at local host " + portList[x] + " timed out.")

    return returnString

def sendViewChange(view, address):

    headers   = {'content-type':'application/json'}
    payload   = json.dumps(view)

    try:
        requests.put(address, headers=headers, data=payload)
    except requests.exceptions.ConnectionError:
        print("server at local host " + address + " timed out.")

async def sendKeys(numOfKeys, address):

    headers = {'content-type':'application/json'}

    for x in range(numOfKeys):
        key     = get_random_string(8)
        value   = get_random_string(8)

        target  = address + key
        payload = json.dumps({"value":value})

        try:
            requests.put(target, data=payload, headers=headers, timeout=2)
        except requests.exceptions.Timeout:
            print("server at local host " + target + " timed out.")

        await asyncio.sleep(0.01)

async def tests():

    if( int(sys.argv[1]) == 1):

        #throwing 100 keys at the system
        await asyncio.wait([ sendKeys(20, url10),
        sendKeys(20, url11), sendKeys(20, url12), 
        sendKeys(20, url13), sendKeys(20, url14)])

        time.sleep(6)

        #doing our view changes and distribution quality checks
        print("#################################################################")
        print("100 key dispersal across 5 nodes {v, w, x, y, z}")
        print("The balance quality is " + balanceQuality(listOfPorts))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.4:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800"}, url)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered.  {v, w, x, y, z} -> {v, x, y, z}")
        print("The balance quality is " + balanceQuality([listOfPorts[0], listOfPorts[2], listOfPorts[3], listOfPorts[4]]))
        keyCountAtW = str(keyCountAtNode( listOfPorts[1]).json()["key-count"])
        if(keyCountAtW == "0"):
            print("The number of keys stored at deprecated node w is: " + colored(keyCountAtW, "green"))
        if(keyCountAtW != "0"):
            print("The number of keys stored at deprecated node w is: " + colored(keyCountAtW, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.4:13800,10.10.0.7:13800,10.10.0.8:13800"}, url4)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered. {v, x, y, z} -> {v, y, z}")
        print("The balance quality is " + balanceQuality([listOfPorts[0], listOfPorts[3], listOfPorts[4]]))
        keyCountAtX = str(keyCountAtNode( listOfPorts[2] ).json()["key-count"])
        if(keyCountAtX == "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "green"))
        if(keyCountAtX != "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.7:13800,10.10.0.8:13800"}, url4)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered. {v, y, z} -> {y, z}")
        print("The balance quality is " + balanceQuality([listOfPorts[3], listOfPorts[4]]))
        keyCountAtV = str(keyCountAtNode( listOfPorts[2] ).json()["key-count"])
        if(keyCountAtV == "0"):
            print("The number of keys stored at deprecated node v is: " + colored(keyCountAtV, "green"))
        if(keyCountAtV != "0"):
            print("The number of keys stored at deprecated node v is: " + colored(keyCountAtV, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800"}, url4)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered. {y, z} -> {v, w, x, y, z}")
        print("The balance quality is " + balanceQuality([listOfPorts[0], listOfPorts[1], listOfPorts[2], listOfPorts[3], listOfPorts[4]]))
        print("There are no dead nodes as a result of this view change.")
        print("#################################################################\n")
        subprocess.call("./recon.sh")
    
        sendViewChange({"view":"10.10.0.4:13800,10.10.0.7:13800,10.10.0.8:13800"}, url4)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered:")  
        print("{v, w, x, y, z} -> {v, y, z}")
        print("The balance quality is " + balanceQuality([listOfPorts[0], listOfPorts[3], listOfPorts[4]]))
        keyCountAtW = str(keyCountAtNode( listOfPorts[1] ).json()["key-count"])
        keyCountAtX = str(keyCountAtNode( listOfPorts[2] ).json()["key-count"])
        if(keyCountAtW == "0"):
            print("The number of keys stored at deprecated node w is: " + colored(keyCountAtW, "green"))
        if(keyCountAtW != "0"):
            print("The number of keys stored at deprecated node w is: " + colored(keyCountAtW, "red"))
        if(keyCountAtX == "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "green"))
        if(keyCountAtX != "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.6:13800"}, url4)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered: {v, y, z} -> {x}")
        print("The balance quality is: " + balanceQuality( [listOfPorts[2]] ))
        keyCountAtV = str(keyCountAtNode( listOfPorts[0] ).json()["key-count"])
        keyCountAtY = str(keyCountAtNode( listOfPorts[3] ).json()["key-count"])
        keyCountAtZ = str(keyCountAtNode( listOfPorts[4] ).json()["key-count"])
        if(keyCountAtV == "0"):
            print("The number of keys stored at deprecated node v is: " + colored(keyCountAtV, "green"))
        if(keyCountAtV != "0"):
            print("The number of keys stored at deprecated node v is: " + colored(keyCountAtV, "red"))
        if(keyCountAtY == "0"):
            print("The number of keys stored at deprecated node y is: " + colored(keyCountAtY, "green"))
        if(keyCountAtY != "0"):
            print("The number of keys stored at deprecated node y is: " + colored(keyCountAtY, "red"))
        if(keyCountAtZ == "0"):
            print("The number of keys stored at deprecated node z is: " + colored(keyCountAtZ, "green"))
        if(keyCountAtZ != "0"):
            print("The number of keys stored at deprecated node z is: " + colored(keyCountAtZ, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

        sendViewChange({"view":"10.10.0.7:13800"}, url2)
        time.sleep(6)
        print("#################################################################")
        print("View change triggered: {x} -> {y}")
        print("The balance quality is: " + balanceQuality( [listOfPorts[3]] ))
        keyCountAtX = str(keyCountAtNode( listOfPorts[2] ).json()["key-count"])
        if(keyCountAtX == "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "green"))
        if(keyCountAtX != "0"):
            print("The number of keys stored at deprecated node x is: " + colored(keyCountAtX, "red"))
        print("#################################################################\n")
        subprocess.call("./view_hashes.sh")

async def main():

    await tests()


loop =asyncio.get_event_loop()
loop.run_until_complete(main())