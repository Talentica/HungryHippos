#!/usr/bin/env python
import os
import sys
import subprocess
import time
import fileinput
import MySQLdb
from kazoo.client import KazooClient

args=sys.argv
uuid=args[1]
mysql_server_ip=args[2]

print "uuid:",uuid
master_ip_file_path="/root/hungryhippos/"+uuid+"/master_ip_file"
nginx_server_path="/root/hungryhippos/scripts/conf/nginx.txt"

f=open(master_ip_file_path)

for line in f:
    hostname=line.strip()

zk = KazooClient(hosts=hostname)
zk.start()

f1=open(nginx_server_path)

for l in f1:
    nginx_ip=line.strip()
    break


##########################################	SHARDING	##########################################

def in_progress_sharding():
    cur = db.cursor()
    sql= """
    insert into
        process_instance(process_id,job_id)
    values
        ((select process_id from process where name='SHARDING'),
         (select job_id from job where job_uuid= %s))"""

    sql1= """
    insert into
        process_instance_detail(process_instance_id,node_ip,status,execution_start_time)
    values
        ((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= %s
          and b.name='SHARDING')
          ,%s,"IN_PROGRESS",now())"""

    cur.execute(sql, (uuid, ))
    db.commit()
    cur.execute(sql1, (uuid,hostname))
    db.commit()


def completed_status_sharding():
    cur = db.cursor()
    sql_update= """
    update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='SHARDING')"""

    cur.execute(sql_update, (uuid, ))
    db.commit()


def failed_status_sharding():
    cur = db.cursor()
    sql_failed= """
    update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Sharding of the Input file has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='SHARDING')"""

    cur.execute(sql_failed, (uuid, ))
    db.commit()

##########################################      DATA PUBLISHING		##########################################


def in_progress_data_publishing():
    cur = db.cursor()
    sql= """
    insert into
        process_instance(process_id,job_id)
    values
        ((select process_id from process where name='DATA_PUBLISHING'),
         (select job_id from job where job_uuid= %s))"""
    
    sql1= """
    insert into
        process_instance_detail(process_instance_id,node_ip,status,execution_start_time)
    values
        ((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= %s
          and b.name='DATA_PUBLISHING')
          ,%s,"IN_PROGRESS",now())"""

    cur.execute(sql, (uuid, ))
    db.commit()
    cur.execute(sql1, (uuid,hostname))
    db.commit()

def completed_status_data_publishing():
    cur = db.cursor()
    sql_update= """
    update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='DATA_PUBLISHING')"""

    cur.execute(sql_update, (uuid, ))
    db.commit()

def failed_status_data_publishing():
    cur = db.cursor()
    sql_failed= """
    update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Download of the output file has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='DATA_PUBLISHING')"""

    cur.execute(sql_failed, (uuid, ))
    db.commit()


##########################################      JOB EXECUTION      ##########################################

def in_progress_job_execution():
    cur = db.cursor()
    sql= """
    insert into
        process_instance(process_id,job_id)
    values
        ((select process_id from process where name='JOB_EXECUTION'),
         (select job_id from job where job_uuid= %s))"""

    cur.execute(sql, (uuid, ))
    db.commit()

##########################################	Update JOB table	##########################################

def update_job_table():
    cur = db.cursor()
    sql= """
    update job a
    set status='COMPLETED',
        date_time_finished=now()
    where a.job_uuid= %s"""

    cur.execute(sql, (uuid, ))
    db.commit()

##########################################	Zip OUTPUT_TRANSFER		##########################################

def zip_output_in_progress():
    cur = db.cursor()

    sql1= """
    insert into
        process_instance_detail(process_instance_id,node_ip,status,execution_start_time)
    values
        ((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= %s
          and b.name='OUTPUT_TRANSFER')
        ,%s,"IN_PROGRESS",now())"""

    cur.execute(sql1, (uuid,nginx_ip))
    db.commit()


def zip_output_completed_status():
    cur = db.cursor()
    sql_update= """
    update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and a.node_ip= %s
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='OUTPUT_TRANSFER')"""

    cur.execute(sql_update, (nginx_ip,uuid, ))
    db.commit()

def zip_output_failed_status():
    cur = db.cursor()
    sql_failed= """
    update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Zipping of the output files has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and a.node_ip= %s
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='OUTPUT_TRANSFER')"""

    cur.execute(sql_failed, (nginx_ip,uuid, ))
    db.commit()


########################################################################################################

path_alert="/rootnode/alertsnode"

## Watcher on Zookeeper for above path
child=set()

@zk.ChildrenWatch(path_alert)
def watch_alert(c):
    for i in c:
        child.add(i)

path_hosts="/rootnode/hostsnode"

## Watcher on Zookeeper  for above path
child_hosts=set()

@zk.ChildrenWatch(path_hosts)
def watch_hosts(c):
    for i in c:
        child_hosts.add(i)


## Connection to MySql Database 
db = MySQLdb.connect(host=mysql_server_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")

count=0
############## Sharding entries ##############

while True:
    if 'SAMPLING_COMPLETED' in child:
        print "Sampling Finished. Making entry in DB for starting Sharding!!"
	in_progress_sharding()	
        break
    else:
        time.sleep(5)
        count=count+1
        print "count:",count

while True:
    if 'SHARDING_COMPLETED' in child:
        print "Sharding Finished. Making entry in DB for Completed Status!!"
	completed_status_sharding()
	in_progress_data_publishing()
        break

    elif 'SHARDING_FAILED' in child:
        print "Sharding Failed. Making entry in DB for Failed Status!!"
	failed_status_sharding()
        break

    else:
        time.sleep(5)
        count=count+1
        print "count:",count

############## Data Publishing entries ##############

while True:
    if 'DATA_PUBLISHING_COMPLETED' in child:
        print "Data Publishing Finished. Making entry in DB for Completed Status!!"
        completed_status_data_publishing()
	in_progress_job_execution()	
        break

    elif 'DATA_PUBLISHING_FAILED' in child:
        print "Data Publishing Failed. Making entry in DB for Failed Status!!"
        failed_status_data_publishing()
        break

    else:
        time.sleep(5)
        count=count+1
        print "count:",count

############## Job Execution entries ############## 

while True:
    if 'START_JOB_MATRIX' in child_hosts:
        print "Job Execution started. Making entry in DB for IN_PROGRESS Status!!"
	in_progress_job_execution()
        break

    else:
        time.sleep(5)
        count=count+1
        print "count:",count

############## Update Job table with end_time now ############## 

while True:
    if 'END_JOB_MATRIX' in child_hosts:
        print "Job Execution sucessfully ended on all nodes. Making entry in DB!!"
	update_job_table()
        break

    else:
        time.sleep(10)
        count=count+1
        print "count:",count


############## Zipping all output files and sending to nginx server ############## 

while True:
    if 'ALL_OUTPUT_FILES_DOWNLOADED' in child_hosts:
        print "Output files ready to be Zipped!!"
	zip_output_in_progress()
        cmd="sh /root/hungryhippos/scripts/bash_scripts/zip-output-files.sh"+" "+uuid+" "+mysql_server_ip
        rc=os.system(cmd)
        if (rc==0): 
	    zip_output_completed_status()
            zipped_znode=path_hosts+"/"+"OUTPUT_FILES_ZIPPED_AND_TRANSFERRED"
            zk.create(zipped_znode)
	    break
	else:
	    print "Transferring zipped output failed!!"
    	    zip_output_failed_status()
            break

    else:
        time.sleep(10)
        count=count+1
        print "count:",count
