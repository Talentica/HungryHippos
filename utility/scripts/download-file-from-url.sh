#!/bin/bash

url_link=$1
job_uuid=$2
mysql_server_ip=$3

path_master='/root/hungryhippos/tmp/'
node_ip=`cat $pathmaster/master_ip_file|head -1`

in_progress() {
mysql -umysql_admin -ppassword123 -h$mysql_server_ip << EOF
use hungryhippos_tester;

insert into process_instance(process_id,job_id)
values
((select process_id from process where name='FILE_DOWNLOAD'),
         (select job_id from job where job_uuid= ('$job_uuid')));

insert into process_instance_detail(process_instance_id,node_ip,status,execution_start_time)
values
((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= ('$job_uuid')
          and b.name='FILE_DOWNLOAD')
        ,('$node_ip'),"In-Progress",now())

EOF
}

completed_status() {
mysql -umysql_admin -ppassword123 -h$mysql_server_ip << EOF
use hungryhippos_tester;

update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= ('$job_uuid'))
    and b.process_id=(select process_id from process where name='FILE_DOWNLOAD')
EOF
}

failed_status() {
mysql -umysql_admin -ppassword123 -h$mysql_server_ip << EOF
use hungryhippos_tester;

 update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Download of the output file has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= ('$job_uuid'))
    and b.process_id=(select process_id from process where name='FILE_DOWNLOAD')

EOF
}

in_progress

curl $url_link -o input.txt -s
if [ $? = 0 ]
then
        completed_status
	python /root/hungryhippos/scripts/python_scripts/input-download-complete.py
else
        failed_status
fi

