#!/bin/bash

source assign-spark-server-variables.sh

attach_storage() {
token=$TOKEN
echo "region of the node is set nyc1, as digital ocean supports mounted capability on nyc1"
region="nyc1"
#name=volume-nyc1-03
name=$STORAGE_NAME

echo name = $name
#droplet_ip="67.205.180.236"
droplet_ip=$(echo ip.txt)
echo droplet_ip $droplet_ip

curl -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $token" "https://api.digitalocean.com/v2/volumes?name=$name&region=$region" >> result.txt

assigned_id=$(cat result.txt | jq .volumes[].droplet_ids | jq '.[]')

if [ -z $assigned_id ]; then
  droplet_id=$(ssh root@$droplet_ip "curl -s http://169.254.169.254/metadata/v1/id")

  curl -X POST -H "Content-Type: application/json" -H "Authorization: Bearer $token" -d '{"type": "attach", "volume_name": '\"$name\"', "region": '\"$region\"', "droplet_id": '\"$droplet_id\"'}' "https://api.digitalocean.com/v2/volumes/actions"

 echo block storage is assigned to the $droplet_id

  ssh root@$i "mount -o discard,defaults /dev/disk/by-id/scsi-0DO_Volume_$name /mnt/spark_history_server; echo /dev/disk/by-id/scsi-0DO_Volume_$name /mnt/spark_history_server ext4 defaults,nofail,discard 0 0 | sudo tee -a /etc/fstab"
   sleep 1

  ssh root@$i "chown hhuser:hungryhippos /mnt/spark_history_server -R"

else
 echo block storage is already assigned to the $assigned_id
fi

 ssh hhuser@$droplet_ip "echo spark.eventLog.enabled   $SPARK_EVENT_LOG_ENABLED >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
 ssh hhuser@$droplet_ip "echo spark.eventLog.dir       $SPARK_EVENT_LOG_DIR  >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
 ssh hhuser@$droplet_ip "echo spark.eventLog.compress  $SPARK_EVENT_LOG_COMPRESS >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"
 ssh hhuser@$droplet_ip "echo spark.history.fs.logDirectory  $SPARK_HISTORY_FS_LOG_DIR >> /home/hhuser/spark-2.0.2-bin-hadoop2.7/conf/spark-defaults.conf"

rm -rf result.txt
}
