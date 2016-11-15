#!/bin/bash

source vagrant_init_functions.sh

#get no of jobs
no_of_jobs=$(cat hungryhippos_operations_conf.json  |jq '.number_of_jobs')
no_of_jobs=$(echo "$no_of_jobs" | tr -d '"')

export lib_client
export process_id
export process_instance_id
export process_instance_detail_id
export job_id
export file_size_input
export file_size_output
export sharding_dimension_string

export file_size_output
export sharding_class
export filepath_client_config
export folderpath_config
export filepath_sharding_output_log
export filepath_sharding_error_log
export filepath_datapublisher_jar
export datapublisher_class
export input_filepath
export sampling_filepath
export distributed_input_filepath
export filepath_datapublisher_output_log
export filepath_datapublisher_error_log
export joborchestrator_class
export job_filepath
export job_class
export distributed_output_filepath
export filepath_orchestrator_output_log
export filepath_orchestrator_error_log
export data_type_configuration
export local_path_to_copy_file_from_hhfs
export delete_file
export file_to_copy_to_local
export mysql_server
export mysql_username
export mysql_password

for (( i=0; $i < $no_of_jobs; ++i ))
do

read_json_hungryhippos $i

#Creating temp file of distributed_input_filepath for checking in next job in next job execution. 
#To avoid publishing file if same file is required in further jobs.
#echo $distributed_input_filepath >> distributed_input_filepath.txt
touch distributed_input_filepath.txt

#get start time for job submit
time_submit=$(date +'%Y:%m:%d %H:%M:%S')

#insertion of time submit in job table.
mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "INSERT INTO job (status,date_time_submitted,user_id,file_system) VALUES ('submitted', '$time_submit','1','HungryHippos');"

#get job id of current job
job_id=$(mysql -h $mysql_server hungryhippos_tester -u$mysql_username -p$mysql_password -se "select job_id from job where date_time_submitted='$time_submit';")

#read_json_hungryhippos $i

update_sharding-client-config

get_hhfs_file_size_input

#get_hhfs_file_size_output

get_data_type_configuration

get_sharding_dimension

hh_job_input_dbwrite $job_id $distributed_input_filepath $file_size_input $job_class $sharding_dimension_string $data_type_configuration

#hh_job_output_dbwrite $job_id $distributed_output_filepath $file_size_output


#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

#read_json_hungryhippos $i


#get start time for job 
time_started=$(date +'%Y:%m:%d %H:%M:%S')
mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update job set  status='started', date_time_started='$time_started' where job_id='$job_id';"

#Check if required file is already available on HH file system
if grep -Fxq "$distributed_input_filepath" distributed_input_filepath.txt
then
	file_exist=1
else
	file_exist=0
	
   	#Add distributed_input_filepath to text file for checking its availability on HHFS for next jobs execution
	echo $distributed_input_filepath >> distributed_input_filepath.txt

fi

if [ $file_exist -eq 0 ]
then

	start_sampling
	start_sharding
	start_publishing

fi

start_execution

get_hhfs_file_size_output
hh_job_output_dbwrite $job_id $distributed_output_filepath $file_size_output


#copy file from hhfs to local
if [ "$file_to_copy_to_local" != ""  ] && [ "$local_path_to_copy_file_from_hhfs" != ""  ]
then
        copy_to_local_from_hhfs
fi




#Delete file from hhfs
if [ "$delete_file" != "" ]
then
echo "test"
	delete_hhfs_file
fi



#get end time for job 
time_finished=$(date +'%Y:%m:%d %H:%M:%S')
mysql -h $mysql_server -D hungryhippos_tester -u$mysql_username -p$mysql_password -e "update job set status='finished', date_time_finished='$time_finished' where job_id='$job_id';"

echo -e "\n-------------------Job `expr $i + 1` completed-------------------------"


done
