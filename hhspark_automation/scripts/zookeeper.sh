#!/bin/bash

download_zookeeper()
{

        #create dir if not exist
        mkdir -p ../chef/src/cookbooks/download_zookeeper/files/default/
       
        #remove zookeeper previously downloaded.
        rm -f zookeeper-3.5.1-alpha.tar.gz

        #Download zookeeper     
        wget  "http://www-us.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz"

        #move zookeeper to required position
        mv zookeeper-3.5.1-alpha.tar.gz ../chef/src/cookbooks/download_zookeeper/files/default/

}


append_to_zoo_conf(){
#Creating file to append it to zoo.conf
zookeeper_flag=1

#get total count of zookeeper
total_zookeepers=$(wc -l ip_file_zookeeper.txt | tr -d 'a-z/_/.')

for zookeeper_ip in "${zookeeper_ips[@]}"
do

        echo "server.$zookeeper_flag=$zookeeper_ip:2888:3888" >> append_to_zoo_cfg
        zookeeper_flag=`expr $zookeeper_flag + 1`
        
done
}
