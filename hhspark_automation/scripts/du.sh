#!/bin/bash

ips=($(awk -F ':' '{print $1}' ip_file.txt))

print_used_space(){

rm -f data.txt
echo "enter cluster input dir"
read cluster_input_dir
for ip in "${ips[@]}"
do
    echo $ip >> data.txt
    ssh hhuser@$ip "du -sh /home/hhuser/hh/filesystem/$cluster_input_dir/data_" >> data.txt
done

echo "nodes disk usage for $cluster_input_dir details are stored in data.txt"
}




