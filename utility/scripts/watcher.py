#!/usr/bin/env python
import subprocess
import sys
import os
import time
import fileinput
import MySQLdb
from kazoo.client import KazooClient

path=sys.argv
print "path args:",path
job_uuid=path[2]
print "uuid:",job_uuid
mysql_ip=path[3]

## Read Zookeeper's IP from the file
f=open('/root/hungryhippos/tmp/master_ip_file')

for line in f:
    hostname=line.strip()

## Connect Kazoo Client to Zookeeper
zk = KazooClient(hosts=hostname)
zk.start()

print "path:",path
print "hostname:",hostname

## Find current node where job execution is happening
node=str(path[1]).split("/")[-1]
print "node:",node
node_id=int(node.split("_node")[-1])
print "node_id:",node_id

## Read IP for the node_id from serverConfigFile.properties file
pattern='server.'+str(node_id)

with open('/root/hungryhippos/tmp/serverConfigFile.properties') as f1:
    for line in f1:
        if pattern in line:
            l=line.strip()

node_ip=l.split(":")[1]
print "node_ip:",node_ip

## Watcher on Zookeeper 
child=set()

@zk.ChildrenWatch(path[1])
def watch1(c):
    for i in c:
        child.add(i)


## Function to insert "In-Progress" status into DB
def in_progress():
    cur = db.cursor()

    sql1= """
    insert into
        process_instance_detail(process_instance_id,node_id,node_ip,status,execution_start_time)
    values
        ((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= %s
          and b.name='OUTPUT_TRANSFER')
        ,%s,%s,"In-Progress",now())"""

    cur.execute(sql1, (job_uuid,node_id,node_ip))
    db.commit()
    #db.close()

## Function to insert "Completed" status into DB
def completed_status():
    cur = db.cursor()
    sql_update= """
    update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and a.node_id= %s
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='OUTPUT_TRANSFER')"""

    cur.execute(sql_update, (node_id,job_uuid, ))
    db.commit()

## Function to insert "Failed" status into DB
def failed_status():
    cur = db.cursor()
    sql_failed= """
    update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Download of the output file has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and a.node_id= %s
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='OUTPUT_TRANSFER')"""

    cur.execute(sql_failed, (node_id,job_uuid, ))
    db.commit()

## Connection to MySql Database 
db = MySQLdb.connect(host=mysql_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")
in_progress()

count=0
while True:
    if 'FINISH_JOB_MATRIX' in child:
        print "finish found"
        cmd="sh /root/hungryhippos/scripts/bash_scripts/copy-output-file.sh"+" "+str(node_id)
        print cmd
        rc=os.system(cmd)
        if (rc==0):
            completed_status()
            download_znode=path[1]+"/"+"DOWNLOAD_FINISHED"
            zk.create(download_znode)
        else:
            failed_status()
        break
    else:
        time.sleep(5)
        count=count+1
        print "count:",count
