#!/bin/bash
#*******************************************************************************
# Copyright [2017] [Talentica Software Pvt. Ltd.]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*******************************************************************************
# first argument is {job_matrix}, second argument is {jobuuid}, third argument is dataparser class
jobUuid=$1
jobMatrixClassName=$2
dataparserclass=$3
mysqlIp=$4

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
sh setup-sharding.sh $jobUuid
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

echo '################ 			Start scheduler to for clean up of inactive jobs ################'
hours=6
sh scheduler-to-cleanup.sh $jobUuid $mysqlIp $hours
echo '################  		Scheduler started   	 ################'

echo "Starting zookeeper sever on master node."
sh start-zk-server.sh $jobUuid
echo "Done."

echo '################          START SHARDING,DATA PUBLISHING AND JOB MATRIX SEQUENCIALLY     ################'
sh start-sharding-and-datapublishing-and-jobmatrix.sh $jobMatrixClassName $jobUuid $dataparserclass
echo '################          PROCESS INITIATED      ################'

echo '################          Start communication for the output file transfer      ################'
sh output-file-zip-transfer.sh $jobUuid
echo '################          Done      ################'

echo '################          Start to destroy the droplets   ################'
sh delete-droplets.sh $jobUuid
echo '################          Destroy of the droplets are initiated   ################'
