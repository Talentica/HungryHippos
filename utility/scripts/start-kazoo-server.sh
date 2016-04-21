#!/bin/bash

output_server_ip=`cat ../tmp/output_ip_file`
webserver_ip=`cat ../tmp/webserver_ip_file`

for node in `echo $output_server_ip`
do
   echo "Starting kazoo server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd /root/hungryhippos/scripts/python_scripts;/usr/bin/python start-kazoo-server.py $1 $webserver_ip;"
done
