#!/bin/bash
echo "Initiating to destroy the droplets"
java -cp ../lib/digital-ocean.jar com.talentica.hungryHippos.droplet.main.DeleteDropletsMain dummy $1
echo "Droplets are destroyed"