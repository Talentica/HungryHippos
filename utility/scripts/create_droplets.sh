#!/bin/bash
echo "Starting digital ocean manager to create droplets"
java -jar ../lib/digital-ocean.jar ../json/create_droplet.json ../conf/config.properties
echo "Droplets creation is initiated successfully."
