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

download_zookeeper()
{

        #create dir if not exist
        mkdir -p ../chef/src/cookbooks/download_zookeeper/files/default/
       
        #remove zookeeper previously downloaded.
        rm -f zookeeper-3.5.1-alpha.tar.gz

        #Download zookeeper     
        wget  "http://www-us.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz"

        #move zookeeper to required position
        mv zookeeper-3.5.1-alpha.tar.gz ../chef/src/cookbooks/download_zookeeper/files/default/

}


append_to_zoo_conf(){
#Creating file to append it to zoo.conf
zookeeper_flag=1

#get total count of zookeeper
total_zookeepers=$(wc -l ip_file_zookeeper.txt | tr -d 'a-z/_/.')

for zookeeper_ip in "${zookeeper_ips[@]}"
do

        echo "server.$zookeeper_flag=$zookeeper_ip:2888:3888" >> append_to_zoo_cfg
        zookeeper_flag=`expr $zookeeper_flag + 1`
        
done
}
