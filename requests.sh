#!/bin/bash

#curl --request   PUT \
# --header    "Content-Type: application/json" \
# --write-out "%{http_code}\n" \
# --data '{"value": "127"}' \
# http://localhost:13801/kvs/keys/x

curl --request   PUT \
 --header    "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 --data '{"value": "127"}' \
 http://localhost:13807/kvs/keys/conglomerate

#echo "---------------------------------------------------------------------------------------------------------------------------------"

#curl --request   GET \
# --header    "Content-Type: application/json" \
# --write-out "%{http_code}\n" \
# --data '{"value": "127"}' \
#http://localhost:13801/kvs/keys/v

#echo "----------------------------------------------------------------------------------------------------------------------------------"


#curl --request   PUT \
# --header    "Content-Type: application/json" \
# --write-out "%{http_code}\n" \
# --data '{"value": "127"}' \
# http://localhost:13801/store_key/v

#echo "-----------------------------------------------------------------------------------------------------------------------------------"


#curl --request GET \
# --header "Content-Type: application/json" \
# --write-out "%{http_code}\n" \
# http://localhost:13801/recon

