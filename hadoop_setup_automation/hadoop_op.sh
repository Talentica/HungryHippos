#!/bin/bash

source vagrant_init_functions.sh

#get no of jobs
no_of_jobs=$(cat hadoop_conf.json  |jq '.number_of_jobs')
no_of_jobs=$(echo "$no_of_jobs" | tr -d '"')

export MASTER_IP
get_master_ip


for (( i=0; i <$no_of_jobs; ++i ))
do

#read values from json file
jar_file_path=$(cat hadoop_conf.json  |jq --arg job_no "$i"  '.jobs['$i'].jar_file_path')
input_file_path=$(cat hadoop_conf.json  |jq --arg job_no "$i" '.jobs['$i'].input_file_path')
class_name=$(cat hadoop_conf.json  |jq '.jobs['$i'].class_name')
output_file_name=$(cat hadoop_conf.json  |jq '.jobs['$i'].output_file_name')
desired_input_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_input_file_location_master')
desired_output_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_output_file_location_master')
desired_job_file_location_master=$(cat hadoop_conf.json  |jq '.jobs['$i'].desired_job_file_location_master')

#remove " from variable
jar_file_path=$(echo "$jar_file_path" | tr -d '"')
input_file_path=$(echo "$input_file_path" | tr -d '"')
class_name=$(echo "$class_name" | tr -d '"')
output_file_name=$(echo "$output_file_name" | tr -d '"')
desired_input_file_location_master=$(echo "$desired_input_file_location_master" | tr -d '"')
desired_output_file_location_master=$(echo "$desired_output_file_location_master" | tr -d '"')
desired_job_file_location_master=$(echo "$desired_job_file_location_master" | tr -d '"')


#export MASTER_IP

#get jar file name from path
jar_name=`basename $jar_file_path`

#get file name from path
file_name=`basename $input_file_path`

#get_master_ip

#create folders on master
ssh root@$MASTER_IP "mkdir -p $desired_input_file_location_master"
ssh root@$MASTER_IP "mkdir -p $desired_output_file_location_master"
ssh root@$MASTER_IP "mkdir -p $desired_job_file_location_master"

#change ownership of created folder
ssh root@$MASTER_IP "chown hduser:hadoop $desired_job_file_location_master"
ssh root@$MASTER_IP "chown hduser:hadoop $desired_input_file_location_master"
ssh root@$MASTER_IP "chown hduser:hadoop $desired_output_file_location_master"

#copy jar file to master machine
scp $jar_file_path root@$MASTER_IP:$desired_job_file_location_master/$jar_name

#copy test file to master machine
scp $input_file_path root@$MASTER_IP:$desired_input_file_location_master/$file_name

upload_to_hdfs $desired_input_file_location_master/$file_name /${file_name}

view_data_of_hdfs

run_job $desired_job_file_location_master/$jar_name $class_name /${file_name} /$output_file_name

download_from_hdfs /$output_file_name $desired_output_file_location_master/$output_file_name

echo "-------------------Job `expr $i + 1` completed-------------------------"

done

