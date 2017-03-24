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
#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add $PRIVATE_KEY_PATH

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

kill_process(){
for ip in "${ips[@]}"
do
	ssh hhuser@$ip pkill -f "talentica"	
done
}

remove_dir(){
for ip in "${ips[@]}"
do
        ssh -o StrictHostKeyChecking=no  hhuser@$ip rm -rf /home/hhuser/distr/lib
        ssh hhuser@$ip rm -rf /home/hhuser/hh
done

}

echo enter 1. to kill process from all nodes
echo enter 2. to remove folders /home/hhuser/hh and  /home/hhuser/distr/lib from all nodes
read val
if [ $val == 1 ];
then

   echo kill command sent
   kill_process 
   echo finished killing process from all nodes

elif [ $val == 2 ];
then
   echo remove folders command sent 
   remove_dir
   echo removed folders from all nodes

fi


