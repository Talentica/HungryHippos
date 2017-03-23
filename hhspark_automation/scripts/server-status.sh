#!/bin/bash

source utility.sh

/home/sudarshans/Downloads/zookeeper-3.5.1-alpha/bin/zkCli.sh -server 67.205.129.112:2181 ls -s /hungryhippo/hosts | sed -e '1,28d;30,40d' | tr -d '[]' | tr ',' '\n' | sed -e 's/  */ /g' -e 's/^ *\(.*\) *$/\1/' | sort  > running-server.txt

check_flg=$(check_file_exists ip.txt)

count=0
if [ $check_flg != "true" ];
then
 ips=($(awk -F ':' '{print $1}' ip_file.txt))
 for i in "${ips[@]}"
 do
   echo $i >> ip_temp.txt
   count=`expr $count + 1`
 done
 sort ip_temp.txt > ip.txt
 rm ip_temp.txt
fi


echo "Number of servers Running :$count"
rs=($(awk '{print $1}' running-server.txt))
tmp_count=0
for i in "${rs[@]}"
do
 echo $i
 tmp_count=`expr $tmp_count + 1`
done

if [ $count != $tmp_count ];
then
 echo "Number of servers failed : $tmp_count "
 diff running-server.txt ip.txt | sed -e '/>/!d' | tr '>' " "
fi

rm running-server.txt
rm ip.txt
