#!/bin/bash



#building the image
docker build -t kvs .

#downloading termcolor for colorcoding
pip3 install termcolor

#creating the network
docker network create --subnet=10.10.0.0/16 kv_subnet

#run 5 nodes to test load balance

docker run -d -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

docker run -d -p 13803:13800 --net=kv_subnet --ip=10.10.0.6 --name="node3" -e ADDRESS="10.10.0.6:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

docker run -d -p 13804:13800 --net=kv_subnet --ip=10.10.0.7 --name="node4" -e ADDRESS="10.10.0.7:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

docker run -d -p 13805:13800 --net=kv_subnet --ip=10.10.0.8 --name="node5" -e ADDRESS="10.10.0.8:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

sleep 5

python3 test/lbscript.py 1

docker kill node1 node2 node3 node4 node5
docker rm node1 node2 node3 node4 node5
