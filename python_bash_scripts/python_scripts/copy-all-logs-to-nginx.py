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
    
master_ip_file_path="/root/hungryhippos/"+uuid+"/master_ip_file"
nginx_server_path="/root/hungryhippos/scripts/conf/nginx.txt"
    
## Reading IP of Zookeeper
f=open(master_ip_file_path)
    
for line in f:
    hostname=line.strip()
    
## Starting Kazoo Client
zk = KazooClient(hosts=hostname)
zk.start()
    

## Reading IP of nginx server
f1=open(nginx_server_path)
    
for l in f1:
    nginx_ip=l.strip()
    break

    
## Watcher on Zookeeper for Hosts
path_hosts="/rootnode/hostsnode"
    
child_hosts=set()
    
@zk.ChildrenWatch(path_hosts)
def watch_hosts(c):
    for i in c:
        child_hosts.add(i)
    
## Watcher on Zookeeper for Alerts
path_alert="/rootnode/alertsnode"
    
child_alert=set()
    
@zk.ChildrenWatch(path_alert)
def watch_alert(c):
    for i in c:
        child_alert.add(i)
    
    
def failed_job_table():
    cur = db.cursor()
    sql_update= """
    update job a
    set a.status="FAILED",
        a.date_time_finished=now()
    where a.job_uuid= %s"""

    cur.execute(sql_update, (uuid, ))
    db.commit()

## Connection to MySql Database 
db = MySQLdb.connect(host=mysql_server_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")

try:    
    count=0
    while True:
        if 'OUTPUT_FILES_ZIPPED_AND_TRANSFERRED' in child_hosts:
            print "Whole process successfuly completed. Ready to Copy logs to nginx!!"
            cmd="sh /root/hungryhippos/scripts/bash_scripts/zip-all-logs.sh "+uuid+" success"
            rc=os.system(cmd)
            if (rc==0):
                drop_droplets=path_alert+"/DROP_DROPLETS"
                zk.create(drop_droplets)
                break
    
        elif 'ERROR_ENCOUNTERED' in child_alert:
	    print "Updating Job table with Failed status!!"
	    failed_job_table()
            print "Process failed. Copy logs to nginx!!"
            cmd="sh /root/hungryhippos/scripts/bash_scripts/zip-all-logs.sh "+uuid+" failure"
            rc=os.system(cmd)
            if (rc==0):
                drop_droplets=path_alert+"/DROP_DROPLETS"
                zk.create(drop_droplets)
                break
    
        else:
            time.sleep(30)
            count=count+1
            print "count:",count

except:
    error_node="/rootnode/alertsnode/ERROR_ENCOUNTERED"
    zk.create(error_node)
    db.close()
