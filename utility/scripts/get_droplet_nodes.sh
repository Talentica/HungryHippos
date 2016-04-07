#!/bin/bash
echo "#####Getting active droplets#####"
java -jar /root/hungryhippos/digital-ocean-manager/digital-ocean.jar /root/hungryhippos/tmp/list-droplets.json /root/hungryhippos/tmp/config.properties
