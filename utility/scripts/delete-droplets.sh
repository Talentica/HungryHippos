#!/bin/bash
echo "Initiating to destroy the droplets"
java -cp ../lib/digital-ocean.jar com.talentica.hungryHippos.droplet.main.DeleteDropletsMain
echo "Droplets are destroyed"