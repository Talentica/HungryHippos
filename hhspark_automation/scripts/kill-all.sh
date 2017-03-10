#!/bin/bash

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add ../hhuser_id_rsa

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

for ip in "${ips[@]}"
do
 	ssh -o StrictHostKeyChecking=no  hhuser@$ip rm -rf /home/hhuser/distr/lib
        ssh hhuser@$ip rm -rf /home/hhuser/hh
	ssh hhuser@$ip pkill -f "talentica"	
done
