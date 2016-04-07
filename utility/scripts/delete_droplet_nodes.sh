#!/bin/bash
echo "#####Delete active droplets#####"
java -jar /root/hungryhippos/digital-ocean-manager/digital-ocean.jar /root/hungryhippos/tmp/delete_droplet.json /root/hungryhippos/tmp/config.properties

