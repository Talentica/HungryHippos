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

folder=$1
download_dir=$2

mkdir -p $download_dir
#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

date=$date

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

for ip in "${ips[@]}"
do

	ssh root@$ip "cat $folder >  $ip.txt"
	scp root@$ip:$ip.txt $download_dir
	ssh root@$ip "rm $ip.txt" 
done

