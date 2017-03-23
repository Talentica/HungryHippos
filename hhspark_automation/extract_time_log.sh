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

job_id=$1
output_file_path=$2

eval `ssh-agent -s`
ssh-add hhuser_id_rsa

#Sharding
sharding_time=$(awk -F '-'  '/seconds of time to do sharding./{ print $4 }'  distr/logs/sharding.out | tr -dc '0-9')

echo "Sharding time log in seconds" >> time_log.txt
echo "$sharding_time" >> time_log.txt

echo "" >> time_log.txt


#data_publishing
publishing_time=$(awk -F '-'  '/seconds of time to for publishing./{ print $4 }'  distr/logs/datapub_6.out | tr -dc '0-9')

echo "Data publishing time log in seconds" >> time_log.txt
echo "$publishing_time" >> time_log.txt

echo "" >> time_log.txt


#Data_synchronisation

sync_time=$(awk -F '-'  '/Data synchronized time in ms/{ print $5 }'  distr/logs/job-man_2.out )

echo "Data synchronisation time log in seconds" >> time_log.txt
echo "$sync_time" >> time_log.txt

echo "" >> time_log.txt

#Execution
echo "Data Execution time log in seconds from all nodes" >> time_log.txt

#Retrieve all IP
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

for ip in "${ips[@]}"
        do
	
	Execution_time=$(ssh hhuser@$ip "awk -F '-'  '/seconds of time to execute all jobs/{ print $4 }'  /home/hhuser/hh/JobJars/$job_id/logs/com.talentica.hungryhippos.node.jobexecutor.out" | awk -F ' - '  '/seconds of time to execute all jobs/{ print $2 }' | tr -dc '0-9')

	echo "${ip}	${Execution_time}" >> time_log.txt


        done

echo "" >> time_log.txt

#getting output file size from all nodes
echo "Output file size in MB  on all nodes" >> time_log.txt

for ip in "${ips[@]}"
        do

	output_file_size=$(ssh hhuser@$ip "stat -c%s $output_file_path")
	
	NUM=$output_file_size
        output_file_size=$(echo "scale=2;$NUM/1048576" | bc)

        echo "${ip}     ${output_file_size}" >> time_log.txt


        done

