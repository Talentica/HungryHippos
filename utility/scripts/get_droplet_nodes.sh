#!/bin/bash
echo "#####Getting active droplets#####"
java -jar ../lib/digital-ocean.jar ../json/list-droplets.json ../conf/config.properties
