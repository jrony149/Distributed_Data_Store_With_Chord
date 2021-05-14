#!/bin/bash

curl --request PUT \
 --header "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 --data '{"value":"127"}' \
 http://localhost:13801/kvs/keys/b
