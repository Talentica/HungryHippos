#!/bin/bash
jobUuid=$1
output_server_ip=`cat ../$jobUuid/output_ip_file`

#webserver_ip=`cat ../$jobUuid/webserver_ip_file`

webserver_ip=$2

ssh-keygen -R 127.0.0.1

for node in `echo $output_server_ip`
do
   echo "Starting kazoo server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd /root/hungryhippos/scripts/python_scripts;/usr/bin/python start-kazoo-server.py $jobUuid $webserver_ip;"
done
