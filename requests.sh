#!/bin/bash

#curl --request   PUT \
# --header    "Content-Type: application/json" \
# --write-out "%{http_code}\n" \
# --data '{"value": "127"}' \
# http://localhost:13801/kvs/keys/x


curl --request   GET \
 --header    "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 --data '{"value": "127"}' \
 http://localhost:13801/kvs/keys/xylophone

echo "----------------------------------------------------------------------------"


curl --request GET \
 --header "Content-Type: application/json" \
  --write-out "%{http_code}\n" \
  http://localhost:13801/recon

