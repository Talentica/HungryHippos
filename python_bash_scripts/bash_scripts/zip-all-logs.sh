#!/bin/bash

job_uuid=$1
state=$2

path_nginx='/root/hungryhippos/scripts/conf'
path_output_server='/root/hungryhippos/'$job_uuid'/output_ip_file'
path_sharding='/root/hungryhippos/sharding/'
path_datapublisher='/root/hungryhippos/data-publisher'
path_jobmanager='/root/hungryhippos/job-manager'
path_jobexecution='/root/hungryhippos/node'
master_ip_file='/root/hungryhippos/'$job_uuid'/master_ip_file'
logs_folder='/root/hungryhippos/'$job_uuid'/logs'
serverConfig_path='/root/hungryhippos/'$job_uuid'/serverConfigFile.properties'

nginx_ip=`cat $path_nginx/nginx.txt|head -1`
output_server_ip=`cat $path_output_server`

ssh-keygen -f "/root/.ssh/known_hosts" -R 127.0.0.1

## Creating logs folder under job_uuid folder for temp storage of all logs before being sent to nginx server
mkdir /root/hungryhippos/$job_uuid/logs

## Copying Sharding,Data_Publisher and Job_Manager logs to "logs" folder
cp $path_sharding/*.out $logs_folder
cp $path_sharding/*.err $logs_folder
cp $path_datapublisher/*.out $logs_folder
cp $path_datapublisher/*.err $logs_folder
cp $path_jobmanager/*.out $logs_folder
cp $path_jobmanager/*.err $logs_folder

## Copying log files from each data node
for i in `cat $serverConfig_path`
do
	node_id=`echo $i| cut -d'.' -f2| cut -d':' -f1`
	node_ip=`echo $i| cut -d':' -f2`

	scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/system_data_receiver.out $logs_folder/system_data_receiver_node$node_id.out
	scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/system_data_receiver.err $logs_folder/system_data_receiver_node$node_id.err
	scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/system.out $logs_folder/system_node$node_id.out
	scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/system.err $logs_folder/system_node$node_id.err

done

## Zipping log files in log folders
zip -j $logs_folder/all_logs $logs_folder/*

## Creating job_uuid folder under logs dir on Nginx server
ssh -o StrictHostKeyChecking=no root@$nginx_ip 'mkdir /root/hungryhippos/job/logs/success/'$job_uuid
ssh -o StrictHostKeyChecking=no root@$nginx_ip 'mkdir /root/hungryhippos/job/logs/error/'$job_uuid

scp -o StrictHostKeyChecking=no ~/.ssh/id_rsa.pub root@$nginx_ip:~/.ssh
scp -o StrictHostKeyChecking=no ~/.ssh/id_rsa root@$nginx_ip:~/.ssh

## Transferring zipped logs to nginx server
if [ $state = 'success' ]
then
	scp -o StrictHostKeyChecking=no $logs_folder/all_logs.zip root@$nginx_ip:/root/hungryhippos/job/logs/success/$job_uuid/all_logs.zip

elif [ $state = 'failure' ]
then
	scp -o StrictHostKeyChecking=no $logs_folder/all_logs.zip root@$nginx_ip:/root/hungryhippos/job/logs/error/$job_uuid/all_logs.zip

else
	echo "Incorrect parameter passed. Please check!!"
fi
