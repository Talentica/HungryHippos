#!/bin/bash
node_to_connect_ip=`cat /root/hungryhippos/tmp/master_ip_file`
ssh -o StrictHostKeyChecking=no root@$node_to_connect_ip
