#!/bin/bash
echo 'Shutting down all java processes on manager'
job_runner_ip=`cat ./node_pwd_file.txt|grep "job_runner_ip"|awk -F":" '{print $2}'`
job_runner_ip=`cat ./node_pwd_file.txt|grep "job_runner_ip"|awk -F":" '{print $2}'`
job_runner_ip=`cat ./node_pwd_file.txt|grep "job_runner_ip"|awk -F":" '{print $2}'`


for node in `echo $manager_node_ip`
do
   echo "Stopping HungryHippos manager $node"
   ssh -o StrictHostKeyChecking=no root@$node "killall java"
done