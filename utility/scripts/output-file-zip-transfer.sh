#!/bin/bash
jobUuid=$1
echo "Initiate the communication for output file transfer"
java -cp ../lib/digital-ocean.jar com.talentica.hungryHippos.droplet.main.DownloadOutputMain $jobUuid &
echo "Done."