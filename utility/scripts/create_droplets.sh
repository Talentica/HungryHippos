#!/bin/bash
echo "Starting digital ocean manager to create droplets"
java -jar /root/hungryhippos/digital-ocean-manager/digital-ocean.jar /root/hungryhippos/tmp/create_droplet.json /root/hungryhippos/tmp/config.properties
echo "Droplets creation is initiated successfully."
