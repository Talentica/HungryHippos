#!/bin/bash
# first argument is {job_matrix}, second argument is {jobuuid}
jobMatrixClassName=$1
jobUuid=$2

echo '################          Create droplets      ################'
sh create_droplets.sh $jobUuid
echo '################          Droplet creation is initiated      ################'

echo '################          Removing older host key      ################'
ssh-keygen -R 127.0.0.1
echo '################          Done      ################'

echo '################          Copying master ip file      ################'
sh copy-files-to-all-nodes.sh $jobUuid
echo '################          Master ip file is copied      ################'

echo '################          Sharding setup started      ################'
sh setup-sharding.sh
echo '################          Sharding setup completed      ################'

echo '################          Data publisher setup started      ################'
sh setup-data-publisher.sh $jobUuid
echo '################          Data publisher setup completed      ################'

echo '################          Job manager setup started      ################'
sh setup-job-manager.sh $jobUuid
echo '################          Job manager setup completed      ################'

echo '################          Nodes setup started      ################'
sh setup-nodes.sh $jobUuid
echo '################          Nodes setup completed      ################'

echo '################          START SHARDING,DATA PUBLISHING AND JOB MATRIX SEQUENCIALLY     ################'
sh start-sharding-and-datapublishing-and-jobmatrix.sh $jobMatrixClassName $jobUuid
echo '################          PROCESS INITIATED      ################'

echo '################          starting kazoo server   ################'
sh start-kazoo-server.sh $jobUuid
echo '################          kazoo server started.  ################'

echo '################          Start to destroy the droplets   ################'
sh delete-droplets.sh $jobUuid
echo '################          Destroy of the droplets are initiated   ################'
