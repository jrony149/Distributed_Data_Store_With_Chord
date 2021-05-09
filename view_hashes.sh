#!/bin/bash

curl --request   GET \
	 --header    "Content-Type: application/json" \
	  --write-out "%{http_code}\n" \
	  http://localhost:13801/all_hash_ids

echo "----------------------------------------------------------------------------------------------------------------"

curl --request   GET \
	 --header    "Content-Type: application/json" \
	   --write-out "%{http_code}\n" \
	    http://localhost:13802/all_hash_ids

echo "----------------------------------------------------------------------------------------------------------------"

curl --request   GET \
	 --header    "Content-Type: application/json" \
	  --write-out "%{http_code}\n" \
	   http://localhost:13803/all_hash_ids

echo "----------------------------------------------------------------------------------------------------------------"

curl --request   GET \
	 --header    "Content-Type: application/json" \
	  --write-out "%{http_code}\n" \
	   http://localhost:13804/all_hash_ids

echo "----------------------------------------------------------------------------------------------------------------"

curl --request   GET \
	 --header    "Content-Type: application/json" \
	  --write-out "%{http_code}\n" \
	   http://localhost:13805/all_hash_ids

echo "----------------------------------------------------------------------------------------------------------------"

