#!/bin/bash
jobUUId=$1
echo "Starting digital ocean manager to create droplets"
java -jar ../lib/digital-ocean.jar ../json/create_droplet.json ../conf/config.properties $jobUUId
echo "Droplets creation is initiated successfully."
