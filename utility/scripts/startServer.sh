#!/bin/bash
# first argument is {job_matrix}, second argument is {jobuuid}
jobMatrixClassName=$1
jobUuid=$2

echo '################          Create droplets      ################'
sh create_droplets.sh
echo '################          Droplet creation is initiated      ################'

echo '################          Removing older host key      ################'
ssh-keygen -R 127.0.0.1
echo '################          Done      ################'

echo '################          Copying master ip file      ################'
sh copy-files-to-all-nodes.sh
echo '################          Master ip file is copied      ################'

echo '################          Sharding setup started      ################'
sh setup-sharding.sh
echo '################          Sharding setup completed      ################'

echo '################          Data publisher setup started      ################'
sh setup-data-publisher.sh
echo '################          Data publisher setup completed      ################'

echo '################          Job manager setup started      ################'
sh setup-job-manager.sh
echo '################          Job manager setup completed      ################'

echo '################          Nodes setup started      ################'
sh setup-nodes.sh
echo '################          Nodes setup completed      ################'

echo '################          START SHARDING,DATA PUBLISHING AND JOB MATRIX SEQUENCIALLY     ################'
sh start-sharding-and-datapublishing-and-jobmatrix.sh $jobMatrixClassName $jobUuid
echo '################          PROCESS INITIATED      ################'

echo '################          starting kazoo server   ################'
sh start-kazoo-server.sh $jobUuid
echo '################          kazoo server started.  ################'

echo '################          Start to destroy the droplets   ################'
sh delete-droplets.sh
echo '################          Destroy of the droplets are initiated   ################'
