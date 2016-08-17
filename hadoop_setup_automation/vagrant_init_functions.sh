#!/bin/bash 

start_vagrantfile()
{

no_of_nodes=$1

#Start vagrant file
NODENUM=$no_of_nodes vagrant up --provider=digital_ocean
sleep 10
}


file_processing_to_getIP()
{
#Copy original file to tmp file to perform file operatiob
cp ip_file.txt ip_file_tmp.txt


#replace tab with colon in tmp file
sed -i 's/	/:/g' ip_file_tmp.txt

#Sort file  as we want HadoopMaster to come at first line
sort  -t':' -k2 ip_file_tmp.txt -o ip_file_tmp.txt

}

copy_ips_to_remote_host()
{
ips=("${!1}")
echo "${ips[@]}"

j=0
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
                MASTER_IP=$i
                #Copying pub key of chef-solo/this machine to master nodes hduser's authorised key. 
                #Here autorised key of master nodes are same as chef-solo server
                ssh root@$i "cat /root/.ssh/authorized_keys >> /home/hduser/.ssh/authorized_keys"
                sleep 1 
        fi

done
    

}

get_master_ip()
{

#retrieve all Ips 
ips=($(awk -F ':' '{print $1}' ip_file_tmp.txt))

j=0
for i in "${ips[@]}"
do
	j=`expr $j + 1`

	if [ $j -eq 1  ]
        then
                MASTER_IP=$i
	       
		break
	fi

done

}


adding_slave_nodes_to_knownhost_master()
{

ips=("${!1}")
#echo "${ips[@]}"


#adding all slave nodes to known host of master
j=0
for ip in "${ips[@]}"
do
        j=`expr $j + 1`
        if [ $j -eq 1  ]
        then
                echo "MASTER IP IS $MASTER_IP "
                ssh hduser@$MASTER_IP "ssh -o StrictHostKeyChecking=no hduser@hadoopMaster "exit""
                sleep 1
        else
                slave_no=`expr $j - 1`
                ssh hduser@$MASTER_IP "ssh -o StrictHostKeyChecking=no hduser@hadoopSlave$slave_no "exit""
                sleep 1
        fi

#below line is to solve warning - "Hadoop “Unable to load native-hadoop library for your platform” warning" on every node
#ssh hduser@$ip "export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=/usr/local/hadoop/lib/native""
done

#adding secondary name node to known host of master
ssh hduser@$MASTER_IP "ssh -o StrictHostKeyChecking=no hduser@0.0.0.0 "exit""
sleep 1


}

format_namenode()
{
#format namenode
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs namenode -format"
sleep 5
echo "namenode formatted"
}



start_dfs(){
#start-dfs
ssh hduser@$MASTER_IP "/usr/local/hadoop/sbin/start-dfs.sh"
sleep 1
}


start_yarn(){
#start-yarn
ssh hduser@$MASTER_IP "/usr/local/hadoop/sbin/start-yarn.sh"
sleep 1
}


upload_to_hdfs(){
#copy test file from local machine to hdfs
#ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /usr/local/hadoop/test.txt /test"
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -copyFromLocal $1 $2"
}


view_data_of_hdfs(){
#List data uploaded to HDFS
echo "data uploaded to HDFS is"
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs dfs -ls /"
}

run_job(){
#Run jar file of hadoop job
echo "Running hadoop wordcount job"
#ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/Hadoop-WordCount/wordcount.jar WordCount /test /test_result"
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hadoop jar $1 $2 $3 $4"


}



cat_hadoop_result(){
#Check hadoop result
echo "Displaying result of hadoop wordcount job"
sleep 4
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs  dfs -cat  /test_result/*"
}



download_from_hdfs(){
#copy file from hdfs system to local
ssh hduser@$MASTER_IP "/usr/local/hadoop/bin/hdfs  dfs -getmerge $1 $2"
}



compare_result(){
#Compare expected result file with actual result
diff_in_result=$(ssh hduser@$MASTER_IP "diff /usr/local/hadoop/expected_test_result.txt /usr/local/hadoop/test_result.txt")

if [ ! $diff_in_result ]; then
	echo "No difference in actual file and expected file"
	echo "Hadoop configuration with testing sample wordcount example completed successfully !!!"
	sleep 5
else
	echo "There is difference in actual file and expected file, please check it manually."
fi

}


