#!/bin/bash

#add ssh key to local machine to access nodes
eval `ssh-agent -s`
ssh-add ../hhuser_id_rsa

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file.txt))

kill_process(){
for ip in "${ips[@]}"
do
	ssh hhuser@$ip pkill -f "talentica"	
done
}

remove_dir(){
for ip in "${ips[@]}"
do
        ssh -o StrictHostKeyChecking=no  hhuser@$ip rm -rf /home/hhuser/distr/lib
        ssh hhuser@$ip rm -rf /home/hhuser/hh
done

}

echo enter 1. to kill process from all nodes
echo enter 2. to remove folders /home/hhuser/hh and  /home/hhuser/distr/lib from all nodes
read val
if [ $val == 1 ];
then

   echo kill command sent
   kill_process 
   echo finished killing process from all nodes

elif [ $val == 2 ];
then
   echo remove folders command sent 
   remove_dir
   echo removed folders from all nodes

fi


