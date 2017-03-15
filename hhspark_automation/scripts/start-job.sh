#!/bin/bash

class_name
 ./spark-submit --class com.talentica.hungryHippos.rdd.main.SumJobWithShuffle --master spark://192.241.154.239:9091 --jars /home/hhuser/distr/lib_client/sharding-0.7.0.jar /home/hhuser/distr/lib_client/spark-0.7.0.jar spark://192.241.154.239:9091 HHJobSumWithShuffle /dir/input3 /home/hhuser/distr/config/client-config.xml /dir/sumout3 >../logs/spark_sum3.out 2>../logs/spark_sum3.err &
