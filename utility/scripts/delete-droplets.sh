#!/bin/bash
jobUuid=$1
echo "Initiating to destroy the droplets"
java -cp ../lib/digital-ocean.jar com.talentica.hungryHippos.droplet.main.DeleteDropletsMain $jobUuid &
echo "Droplets are destroyed"