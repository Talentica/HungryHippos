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

for (( i=0; $i < $no_of_jobs; ++i ))
do

#get start time for job submit
time_submit=$(date +'%Y:%m:%d %H:%M:%S')

#insertion of time submit in job table.
mysql -D hungryhippos_tester -uroot -proot -e "INSERT INTO job (status,date_time_submitted,user_id,file_system) VALUES ('submitted', '$time_submit','1','HungryHippos');"

#get job id of current job
job_id=$(mysql hungryhippos_tester -uroot -proot -se "select job_id from job where date_time_submitted='$time_submit';")

read_json_hungryhippos $i

update_sharding-client-config

get_hhfs_file_size_input

#get_hhfs_file_size_output

get_data_type_configuration

get_sharding_dimension

hh_job_input_dbwrite $job_id $distributed_input_filepath $file_size_input $job_class $sharding_dimension_string $data_type_configuration

hh_job_output_dbwrite $job_id $distributed_output_filepath $file_size_output



#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

#read_json_hungryhippos $i


#get start time for job 
time_started=$(date +'%Y:%m:%d %H:%M:%S')
mysql -D hungryhippos_tester -uroot -proot -e "update job set  status='started', date_time_started='$time_started' where job_id='$job_id';"


start_sampling

start_sharding

start_publishing

start_execution


#get end time for job 
time_finished=$(date +'%Y:%m:%d %H:%M:%S')
mysql -D hungryhippos_tester -uroot -proot -e "update job set status='finished', date_time_finished='$time_finished' where job_id='$job_id';"

echo -e "\n-------------------Job `expr $i + 1` completed-------------------------"


done
