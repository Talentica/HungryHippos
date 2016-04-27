#!/bin/bash

job_uuid=$1
mysql_server_ip=$2

path='/root/hungryhippos/scripts/conf'

nginx_ip=`cat $path/nginx.txt|head -1`
nginx_zip_folder=`cat $path/nginx.txt|tail -1`

zip /root/hungryhippos/download-output/output outputFile*

size_in_bytes=`ls -lrt /root/hungryhippos/output.zip | awk -F" " '{print $5}'`
size_in_kb=`echo print $size_in_bytes/1024. |python`

ssh -o StrictHostKeyChecking=no root@$nginx_ip 'mkdir $nginx_zip_folder/$job_uuid'
scp -o StrictHostKeyChecking=no /root/hungryhippos/output.zip root@$nginx_ip:$nginx_zip_folder/$job_uuid/output.zip

data_location='http://'$nginx_ip'/output/'$job_uuid'/output.zip'


mysql -umysql_admin -ppassword123 -h$mysql_server_ip << EOF
use hungryhippos_tester;

insert into job_output(job_id,data_location,data_size)
values
((select job_id from job where job_uuid=('$job_uuid')),
('$data_location'),
($size_in_kb));
