#!/bin/bash

for i in `cat node_list.txt`
do

ip=`echo $i|head -1| awk -F"," '{print $1}'`
initial_pwd=`echo $i|head -1| awk -F"," '{print $2}'`

sed -e "s/node_ip/$ip/g" -e "s/node_initial_pwd/$initial_pwd/g" initial-connect-to-server.sh > initial-connect-to-$ip.sh
chmod 755 initial-connect-to-$ip.sh
./initial-connect-to-$ip.sh
done
