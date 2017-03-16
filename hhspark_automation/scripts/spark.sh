#!/bin/bash

source assign-spark-variables.sh
download_spark()
{

        #create dir if not exist
        mkdir -p ../chef/src/cookbooks/download_spark/files/default/

        #remove if spark is already downloaded
        rm -f spark-2.0.2-bin-hadoop2.7.tgz

        #Download spark
        wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz

        #move zookeeper to required position
        mv  spark-2.0.2-bin-hadoop2.7.tgz ../chef/src/cookbooks/download_spark/files/default/

}

add_spark_ip_port_on_spark_env()
{
        ips=("${!1}")
        echo "${ips[@]}"
        rm -f ip.txt
        j=0
        for i in "${ips[@]}"
        do
                echo $i >> ip.txt
        done

        for i in "${ips[@]}"
        do
                echo $i

                j=`expr $j + 1`

                chmod 777 ip.txt
                ssh hhuser@$i "rm -f  /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/slaves"
                ssh hhuser@$i "rm -f  /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                #copy ip file to every node
                cat ip.txt | ssh hhuser@$i "cat >>/home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/slaves"
                sleep 1
                #scp ip_file.txt  root@$i:/etc/ip_file.txt
                #vagrant ssh hadoop-$j -c '/etc/ip_file.txt >> /etc/hosts'

                if [ $j -eq 1  ]
                then
                        SPARK_MASTER_HOST=$i
                        ssh root@$i "mkdir -p /mnt/spark_history_server"
                        ssh hhuser@$i "echo "SPARK_MASTER_HOST="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                        ssh hhuser@$i "echo "SPARK_LOCAL_IP="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                        ssh hhuser@$i "echo "SPARK_WORKER_PORT="$SPARK_WORKER_PORT >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                        ssh hhuser@$i "echo "SPARK_MASTER_PORT="$SPARK_MASTER_PORT >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                        ssh hhuser@$i "echo spark.eventLog.enabled   $SPARK_EVENT_LOG_ENABLED >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
                        ssh hhuser@$i "echo spark.eventLog.dir       $SPARK_EVENT_LOG_DIR  >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
                        ssh hhuser@$i "echo spark.eventLog.compress  $SPARK_EVENT_LOG_COMPRESS >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
                        ssh hhuser@$i "echo spark.history.fs.logDirectory  $SPARK_HISTORY_FS_LOG_DIR >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"

                        echo  $i > ../../utility/ip.txt

			sleep 15
                
                       
                else
                        ssh hhuser@$i "echo "SPARK_LOCAL_IP="$i >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"
                        ssh hhuser@$i "echo "SPARK_WORKER_PORT="9090 >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-env.sh"

                fi
       done

       rm -f ip.txt
}

start_spark_all()
{
        ips=("${!1}")
        ssh hhuser@${ips[0]} "sh /home/hhuser/spark-2.0.2-bin-hadoop2.7/sbin/start-all.sh"
}
