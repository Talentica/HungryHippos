#!/bin/bash
jobuuid=$1
echo "#####Delete active droplets#####"
java -jar ../lib/digital-ocean.jar ../json/delete_droplet.json ../conf/config.properties $jobuuid
