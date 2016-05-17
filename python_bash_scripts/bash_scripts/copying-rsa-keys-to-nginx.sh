#!/bin/bash

nginx_ip=`cat /root/hungryhippos/scripts/conf/nginx.txt|head -1`

scp -o StrictHostKeyChecking=no ~/.ssh/id_rsa.pub root@$nginx_ip:~/.ssh
scp -o StrictHostKeyChecking=no ~/.ssh/id_rsa root@$nginx_ip:~/.ssh
