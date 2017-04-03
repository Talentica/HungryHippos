#!/bin/bash
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
jobUuid=$1
output_server_ip=`cat ../$jobUuid/output_ip_file`

#webserver_ip=`cat ../$jobUuid/webserver_ip_file`

webserver_ip=$2

ssh-keygen -R 127.0.0.1

for node in `echo $output_server_ip`
do
   echo "Starting kazoo server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd /root/hungryhippos/scripts/python_scripts;/usr/bin/python start-kazoo-server.py $jobUuid $webserver_ip;"
done
