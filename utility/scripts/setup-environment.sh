#*******************************************************************************
# Copyright 2017 Talentica Software Pvt. Ltd.
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
sh start-sharding-and-datapublishing-and-jobmatrix.sh
echo '################          PROCESS INITIATED      ################'
