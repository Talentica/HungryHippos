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
source vagrant_init_functions.sh


#get no of jobs
no_of_jobs=$(cat hadoop_conf.json  |jq '.number_of_jobs')
no_of_jobs=$(echo "$no_of_jobs" | tr -d '"')

export MASTER_IP
export file_size_input
export file_size_output
export process_id
export process_instance_id
export process_instance_detail_id
export job_id

export jar_file_path
export input_file_path
export class_name
export output_file_name
export desired_input_file_location_master
export desired_output_file_location_master
export desired_job_file_location_master
export desired_input_file_location_hdfs
export desired_output_file_location_hdfs
export expected_result_file_path
export desired_expected_result_location_master
export jar_name
export file_name
export delete_hdfs_file_name
export mysql_server
export mysql_username
export mysql_password


get_master_ip


for (( i=0; $i <$no_of_jobs; ++i ))
do

	#Read all values in of jason file
        read_json $i

	
	#get start time for job submit
	time_submit=$(date +'%Y:%m:%d %H:%M:%S')

	#insertion of time submit in job table.
	mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job (status,date_time_submitted,user_id,file_system) VALUES ('submitted', '$time_submit','1','Hadoop');"

	#get job id of current job
	job_id=$(mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -se "select job_id from job where date_time_submitted='$time_submit';")
	

	#start timer for whole script
	start=$(date +%s.%N)


	#perform below block if expected result in available with user
	if [ "$expected_result_file_path" != "" ]
	then

	expected_result_file_path_operations

	fi

	#setup master node for hadoop operations (Includes creating folders as per given in json file )
	master_setup_for_job_execution

	#get start time for job 
        time_started=$(date +'%Y:%m:%d %H:%M:%S')
        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update job set  status='started', date_time_started='$time_started' where job_id='$job_id';"


	
	start_upload=$(date +%s.%N)

	#upload data from master node to hdfs
	data_publishing

	time_upload=$(echo "$(date +%s.%N) - $start_upload" | bc)
	echo -e "\nExecution time for uploading file to hdfs: $time_upload seconds" 

	#show uploaded data (Only for verification)
	show_data

	#execute submitted job
	job_execution

	#Copy result file from hdfs
	transfer_from_hdfs
	
	#Sort copied result file
	sort_compare_output

	#Get file size of input	
	get_hdfs_file_size_input $desired_input_file_location_hdfs/${file_name}
	#filesize=$?
	
	#update tabale job_input
	job_input_dbwrite $job_id $desired_input_file_location_hdfs/${file_name} $file_size_input

	get_hdfs_file_size_output $desired_output_file_location_hdfs/$output_file_name
	#filesize=$?

	#update table job_output
	job_output_dbwrite $job_id $desired_output_file_location_hdfs/${output_file_name} $file_size_output

	#Delete hdfs file if file name provided
	if [ "$delete_hdfs_file_name" != "" ]
        then

		#Delete hdfs file
		delete_hdfs_file	
	fi

	
	#stop timer for whole script
        time_script=$(echo "$(date +%s.%N) - $start" | bc)
        printf "Execution time for running whole script for Job `expr $i + 1`: $time_script seconds"


	#get end time for job 
        time_finished=$(date +'%Y:%m:%d %H:%M:%S')
        mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update job set status='finished', date_time_finished='$time_finished' where job_id='$job_id';"

	echo -e "\n-------------------Job `expr $i + 1` completed-------------------------"


done
