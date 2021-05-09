#!/bin/bash

#building the image
docker build -t kvs .

#creating the network
docker network create --subnet=10.10.0.0/16 kv_subnet

#run x, y, and z for {x,y,z} -> {x} test
#x, y and z all know of each other

docker run -d -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800" kvs 

docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800" kvs

docker run -d -p 13803:13800 --net=kv_subnet --ip=10.10.0.6 --name="node3" -e ADDRESS="10.10.0.6:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800" kvs

sleep 5

python3 test/ecscript.py 1

docker kill node1 node2 node3
docker rm node1 node2 node3


#run x and y for {x} -> {y} test
#x only knows of itself and y only knows of itself
docker run -d -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800" kvs
#docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.5:13800" kvs
docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.4:13800" kvs

sleep 5

python3 test/ecscript.py 2

docker kill node1 node2
docker rm node1 node2

#run x and a and b for {x} -> {a, b} test

#x only knows of itself, but it will receive a view change with {a, b} only included in the incoming view.

docker run -d -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800" kvs

#a and b (below) know about each other, but they don't know about x (because they don't need to)

docker run -d -p 13802:13800 --net=kv_subnet --ip=10.10.0.5 --name="node2" -e ADDRESS="10.10.0.5:13800" -e VIEW="10.10.0.5:13800,10.10.0.6:13800" kvs

docker run -d -p 13803:13800 --net=kv_subnet --ip=10.10.0.6 --name="node3" -e ADDRESS="10.10.0.6:13800" -e VIEW="10.10.0.5:13800,10.10.0.6:13800" kvs


sleep 5

python3 test/ecscript.py 3

docker kill node1 node2 node3
docker rm node1 node2 node3
