#!/bin/bash

curl --request GET \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 http://localhost:13801/kvs/keys/v

echo "--------------------------------------------------------------------------------------------"

curl --request GET \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 http://localhost:13802/kvs/keys/v

echo "--------------------------------------------------------------------------------------------"

curl --request GET \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 http://localhost:13803/kvs/keys/v

echo "--------------------------------------------------------------------------------------------"

curl --request GET \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 http://localhost:13804/kvs/keys/v

echo "--------------------------------------------------------------------------------------------"

curl --request GET \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 http://localhost:13805/kvs/keys/v

echo "--------------------------------------------------------------------------------------------"


