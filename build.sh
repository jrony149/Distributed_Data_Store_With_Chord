
#!/bin/bash

docker network create --subnet=10.10.0.0/16 kv_subnet 

docker build -t kvs .
