#!/bin/bash
echo "#####Delete active droplets#####"
java -jar ../lib/digital-ocean.jar ../json/delete_droplet.json ../conf/config.properties

