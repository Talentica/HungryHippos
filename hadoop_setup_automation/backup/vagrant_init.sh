#!/bin/bash 

#Start vagrant file
vagrant up --provider=digital_ocean
sleep 10

#Copy original file to tmp file to perform file operatiob
cp ip_file.txt ip_file_tmp.txt


#replace tab with colon in tmp file
sed -i 's/	/:/g' ip_file_tmp.txt

#Sort file  as we want HadoopMaster to come at first line
sort  -t':' -k2 ip_file_tmp.txt -o ip_file_tmp.txt


#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

#flag
j=0
master_ip=""

for i in "${ips[@]}"
do
	echo $i

	j=`expr $j + 1`


	chmod 777 ip_file.txt

	#for adding new ip addresses to known hosts
	#ssh -o StrictHostKeyChecking=no root@$i
	ssh-keyscan $i >> ~/.ssh/known_hosts
	sleep 1

	#copy ip file to every node
	cat ip_file.txt | ssh root@$i "cat >> /etc/hosts"
	sleep 1
	#scp ip_file.txt  root@$i:/etc/ip_file.txt
	#vagrant ssh hadoop-$j -c '/etc/ip_file.txt >> /etc/hosts'

	if [ $j -eq 1  ]
	then
		master_ip=$i
		#Copying pub key of chef-solo/this machine to master nodes hduser's authorised key. 
		#Here autorised key of master nodes are same as chef-solo server
		ssh root@$i "cat /root/.ssh/authorized_keys >> /home/hduser/.ssh/authorized_keys"
		sleep 1
	fi

done


rm ip_file_tmp.txt

#format namenode
ssh hduser@$master_ip "/usr/local/hadoop/bin/hdfs namenode -format"
sleep 5
echo "namenode formatted"


#adding all slave nodes to known host of master
j=0
for ip in "${ips[@]}"
do
	j=`expr $j + 1`
	if [ $j -eq 1  ]
	then
		echo "MASTER IP IS $master_ip"
		ssh hduser@$master_ip "ssh -o StrictHostKeyChecking=no hduser@hadoopMaster "exit""
		sleep 1
	else
		slave_no=`expr $j - 1`
		ssh hduser@$master_ip "ssh -o StrictHostKeyChecking=no hduser@hadoopSlave$slave_no "exit""
		sleep 1
	fi

#below line is to solve warning - "Hadoop “Unable to load native-hadoop library for your platform” warning" on every node
#ssh hduser@$ip "export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/local/hadoop/lib/native""
done

#adding secondary name node to known host of master
ssh hduser@$master_ip "ssh -o StrictHostKeyChecking=no hduser@0.0.0.0 "exit""
sleep 1

#start-dfs
ssh hduser@$master_ip "/usr/local/hadoop/sbin/start-dfs.sh"
sleep 1

#start-yarn
ssh hduser@$master_ip "/usr/local/hadoop/sbin/start-yarn.sh"
sleep 1

#copy test file from local machine to hdfs
ssh hduser@$master_ip "/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /usr/local/hadoop/test.txt /test"

#List data uploaded to HDFS
echo "data uploaded to HDFS is"
ssh hduser@$master_ip "/usr/local/hadoop/bin/hdfs dfs -ls /"

#Run jar file of wordcount 
echo "Running hadoop wordcount job"
ssh hduser@$master_ip "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/Hadoop-WordCount/wordcount.jar WordCount /test /test_result"

#Check hadoop result
echo "Displaying result of hadoop wordcount job"
sleep 4
ssh hduser@$master_ip "/usr/local/hadoop/bin/hdfs  dfs -cat  /test_result/*"

#copy file from hdfs system to local
ssh hduser@$master_ip "/usr/local/hadoop/bin/hdfs  dfs -getmerge /test_result /usr/local/hadoop/test_result.txt"

#Compare expected result file with actual result
diff_in_result=$(ssh hduser@$master_ip "diff /usr/local/hadoop/expected_test_result.txt /usr/local/hadoop/test_result.txt")

if [ ! $diff_in_result ]; then
	echo "No difference in actual file and expected file"
	echo "Hadoop configuration with testing sample wordcount example completed successfully !!!"
	sleep 5
else
	echo "There is difference in actual file and expected file, please check it manually."
fi




