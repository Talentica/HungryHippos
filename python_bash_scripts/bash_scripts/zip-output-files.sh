#!/bin/bash

job_uuid=$1
mysql_server_ip=$2

path='/root/hungryhippos/scripts/conf'
path_output_server='/root/hungryhippos/'$job_uuid'/output_ip_file'

nginx_ip=`cat $path/nginx.txt|head -1`
nginx_zip_folder=`cat $path/nginx.txt|tail -1`
output_server_ip=`cat $path_output_server`

#ssh-keygen -R 127.0.0.1

## Copying public and private keys of Output server to nginx
#scp ~/.ssh/id_rsa.pub root@$nginx_ip:~/.ssh
#scp ~/.ssh/id_rsa root@$nginx_ip:~/.ssh

ssh-keygen -f "/root/.ssh/known_hosts" -R 127.0.0.1

## Zipping output files present on Output server
ssh -o StrictHostKeyChecking=no root@$output_server_ip 'zip -j /root/hungryhippos/download-output/output /root/hungryhippos/download-output/outputFile*'

#ssh -o StrictHostKeyChecking=no root@$output_server_ip 'size_in_bytes=`ls -lrt /root/hungryhippos/download-output/output.zip | awk -F" " '{print $5}'`'

size_in_bytes=`ssh -o StrictHostKeyChecking=no root@$output_server_ip 'ls -lrt /root/hungryhippos/download-output/output.zip | cut -d" " -f5'`
size_in_kb=`echo print $size_in_bytes/1024. |python`

ssh -o StrictHostKeyChecking=no root@$nginx_ip 'mkdir /root/hungryhippos/job/output/'$job_uuid
scp -o StrictHostKeyChecking=no root@$output_server_ip:/root/hungryhippos/download-output/output.zip root@$nginx_ip:/root/hungryhippos/job/output/$job_uuid/output.zip

data_location='http://'$nginx_ip'/output/'$job_uuid'/output.zip'


mysql -umysql_admin -ppassword123 -h$mysql_server_ip << EOF
use hungryhippos_tester;

insert into job_output(job_id,data_location,data_size_in_kbs)
values
((select job_id from job where job_uuid=('$job_uuid')),
('$data_location'),
($size_in_kb));
