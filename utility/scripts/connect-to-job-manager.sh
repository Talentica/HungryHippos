#!/bin/bash
job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`

ssh -o StrictHostKeyChecking=no root@$job_manager_ip
