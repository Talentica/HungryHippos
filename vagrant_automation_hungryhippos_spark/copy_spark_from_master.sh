#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp1.txt))
for ip in "${ips[@]}"
do
#Add all the nodes to known host of this machine
scp spark-2.0.2-bin-hadoop2.7/conf/slaves hhuser@$ip:/home/hhuser/spark-2.0.2-bin-hadoop2.7/conf
ssh hhuser@$ip;iptables-save
logout
done
