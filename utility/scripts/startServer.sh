#!/bin/bash
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
sh start-sharding-and-datapublishing-and-jobmatrix.sh $1
echo '################          PROCESS INITIATED      ################'

echo '################          starting kazoo server   ################'
sh start-kazoo-server.sh $1
echo '################          kazoo server started.  ################'
