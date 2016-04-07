#!/bin/bash
job_manager_ip=`cat /root/hungryhippos/tmp/master_ip_file'`

ssh -o StrictHostKeyChecking=no root@$job_manager_ip
