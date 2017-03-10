#!/bin/bash

folder=$1
download_dir=$2

mkdir -p $download_dir
#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add hhuser_id_rsa

date=$date

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

for ip in "${ips[@]}"
do

	ssh root@$ip "cat $folder >  $ip.txt"
	scp root@$ip:$ip.txt $download_dir
	ssh root@$ip "rm $ip.txt" 
done

