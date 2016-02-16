#!/bin/bash

job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sh start-zk-server.sh
for node in `echo $job_manager_ip`-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false 
do
   echo "Starting job manager on $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/job-manager;java -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp job-manager.jar:test-jobs.jar com.talentica.hungryHippos.job.main.JobManagerStarter $1 > ./system.out 2>./system.err &"
done
