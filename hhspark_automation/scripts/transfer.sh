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

source assign-global-variables.sh
source ../distr_original/sbin/utility.sh
source ../distr_original/sbin/run-hh-scripts-on-cluster.sh

#add ssh key to local machine to access nodes
add_ssh_key
#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

#retrieve all zookeeper Ips
zookeeper_ips=($(awk -F ':' '{print $1}' ip_file_zookeeper.txt))

create_zookeeperip_string zookeeper_ips[@]


start_process(){
tmp=0
for ip in "${ips[@]}"
do

        if [ $tmp -eq 0 ]
        then
                run_script_on_random_node $zookeeperip_string $ip
        fi
	#Run scripts on all node
        run_all_scripts $zookeeperip_string $ip &

	tmp=`expr $tmp + 1`

done

}

perform_copy(){
    ip=$1
    rsync -av ../distr root@$ip:/home/hhuser/
    ssh root@$ip "chown hhuser:hungryhippos /home/hhuser/ -R"
    ssh root@$ip "chown root /home/hhuser/distr/bin/clear_unix_cache"
    ssh root@$ip "chmod 7755 /home/hhuser/distr/bin/clear_unix_cache"
}
copy_folder(){

for ip in "${ips[@]}"
do
        perform_copy $ip &

done

}

echo 1. to start process on all nodes
echo 2. to copy distr folders on all nodes
read val

if [ $val == 1 ];
then
 echo request to start process sent to all nodes
 start_process
 echo started process on all nodes

elif [ $val == 2 ];
then 
 echo request to copy folders sent
 copy_folder
 echo transfered distr folder
fi  

echo "Waiting for tranfer process to complete"
wait
echo "Transfer process completed"


