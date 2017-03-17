#!/bin/bash

file_processing_to_get_ip()
{
        #Sort file  as we want HadoopMaster to come at first line
        sort  -t':' -k2 -V  ip_file.txt -o ip_file.txt

}


file_processing_to_get_zookeeper_ip(){

        #Copy original file to tmp file to perform file operatiob
        cp ip_file.txt ip_file_zookeeper.txt

        sed -i '/Zookeeper/!d' ip_file_zookeeper.txt

        #Sort file  as we want HadoopMaster to come at first line
        sort  -t':' -k2 ip_file_zookeeper.txt -o ip_file_zookeeper.txt


}

create_zookeeperip_string(){


        i=0
        for zookeeper_ip in "${zookeeper_ips[@]}"
        do
                if [ $i -eq "0" ]
                then
                echo   zookeeper_ip= $zookeeper_ip
                zookeeperip_string="$zookeeper_ip:2181"
                else
                zookeeperip_string="$zookeeperip_string,$zookeeper_ip:2181"
                fi

                i=`expr $i + 1`

        done
        
	echo zookeeperip_stringinfunc=$zookeeperip_string


}

update_client-config(){

        zookeeperip_string_config="\<tns\:servers\>$zookeeperip_string\<\/tns\:servers\>"


        #get line no having <tns:servers> in file
        line_no=$(grep -n  '<tns:servers>' ../distr/config/client-config.xml | awk -F  ":" '{print $1}' )

        #delete existing line containing zookeeper string
        sed -i "${line_no}d" ../distr/config/client-config.xml

        #add new zookeeper string
        sed -i "${line_no}i $zookeeperip_string_config" ../distr/config/client-config.xml
}

update_cluster-config(){

        i=0
        for ip in "${ips[@]}"
        do
                echo "  <tns:node>" >> tmp_cluster-config.xml
                echo "          <tns:identifier>$i</tns:identifier>" >> tmp_cluster-config.xml
                echo "          <tns:name>node$i</tns:name>" >> tmp_cluster-config.xml
                echo "          <tns:ip>$ip</tns:ip>" >> tmp_cluster-config.xml
                echo "          <tns:port>2324</tns:port>" >> tmp_cluster-config.xml
                echo "  </tns:node>" >> tmp_cluster-config.xml

                i=`expr $i + 1`
        done

        #adding tmp file to original file
        sed -i -e '/xsi\:schemaLocation/r tmp_cluster-config.xml' ../distr/config/cluster-config.xml

        rm tmp_cluster-config.xml

}

remove_known_hosts()
{
 rm -f ~/.ssh/known_hosts
}

update_limits_conf(){
 ssh root@$ip 'echo "* soft nofile 500000" >> /etc/security/limits.conf'
 ssh root@$ip 'echo "* hard nofile 500000" >> /etc/security/limits.conf'
}

add_ssh_key(){
	eval `ssh-agent -s`
	ssh-add $PRIVATE_KEY_PATH
}

check_file_exists(){
file=$1
if [ -f "$file" ]
then
	echo "true"
else
	echo "false"
fi

}

add_nodes_to_known_hosts(){
  for ip in "${ips[@]}"
  do
#Add all the nodes to known host of this machine
    ssh -o StrictHostKeyChecking=no hhuser@$ip "exit"
  done
}

