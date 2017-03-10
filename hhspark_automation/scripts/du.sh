#!/bin/bash

ips=($(awk -F ':' '{print $1}' ip_file.txt))

for ip in "${ips[@]}"
do
    echo $ip >> data.txt
    ssh hhuser@$ip "du -sh /home/hhuser/hh/filesystem/dir/input/data_" >> data.txt
done
