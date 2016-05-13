#!/bin/bash
sharding_node_ip=`cat ../tmp/master_ip_file`

ssh -o StrictHostKeyChecking=no root@$sharding_node_ip
