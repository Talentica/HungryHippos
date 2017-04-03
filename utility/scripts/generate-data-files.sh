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
sh shut-down-data-publisher.sh
cat ../tmp/master_ip_file > ../$jobUuid/data_publisher_node_ips.txt
for node in `cat ../$jobUuid/data_publisher_node_ips.txt`
do
   echo "Generating data file on node $node"
   ssh -o StrictHostKeyChecking=no root@$node "echo 1 > /proc/sys/vm/drop_caches;cd hungryhippos/data-publisher;rm ../input/sampledata_new.txt;java -XX:+HeapDumpOnOutOfMemoryError -Xmx1500m -XX:HeapDumpPath=./ -cp data-publisher.jar com.talentica.hungryHippos.utility.ConfigurableDataGenerator $1 ../input/sampledata_new.txt C:1 C:1 C:1 C:3 C:3 C:3 N:3 N:2 C:5> ./system.out 2>./system.err &"
done
