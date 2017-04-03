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

node=$1
job_uuid=$2

pattern="server."$node
path='/root/hungryhippos/'$job_uuid

node_ip=`cat $path/serverConfigFile.properties |grep -i $pattern|awk -F":" '{print $2}'`

ssh -o StrictHostKeyChecking=no root@$node_ip "test -e /root/hungryhippos/node/outputFile"
if [ $? -eq 0 ]; then
scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/outputFile /root/hungryhippos/download-output/outputFile_$node

fi

