#!/bin/bash


 curl --request PUT \
 --header    "Content-Type: application/json" \
 --write-out "%{http_code}\n" \
 --data '{"view":"10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800,10.10.0.9:13800,10.10.0.10:13800,10.10.0.11:13800"}' \
 http://localhost:13801/kvs/view-change
