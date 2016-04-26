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

f=open(master_ip_file_path)

for line in f:
    hostname=line.strip()

#zk = KazooClient(hosts='127.0.0.1:2181')
zk = KazooClient(hosts=hostname)
zk.start()

def my_listener(state):
    if state == KazooState.LOST:
        print "Connection to ZooKeeper lost"
    elif state == KazooState.SUSPENDED:
        print "Connection to ZooKeeper lost"

zk.add_listener(my_listener)

## Entry of the current job_id in process_intsance table

def in_progress():
    cur = db.cursor()
    sql= """
    insert into
        process_instance(process_id,job_id)
    values
        ((select process_id from process where name='OUTPUT_TRANSFER'),
         (select job_id from job where job_uuid= %s))"""
    print "inside fucntion:",uuid
    cur.execute(sql, (uuid, ))
    db.commit()


path="/rootnode/hostsnode"

nodes=list()
if zk.exists(path):
    nodes=zk.get_children(path)
else:
    print "Path: /rootnode/hostsnode/ does not exists!!"

print "children:",nodes

node_path=list()

for i in nodes:
    newpath=path+"/"+i
    node_path.append(newpath)

print "full paths:",node_path

## Connection to MySql Database 
db = MySQLdb.connect(host=mysql_server_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")
in_progress()

for i in node_path:
    cmd="python watcher.py "+i+" "+uuid+" "+mysql_server_ip
    subprocess.Popen(cmd, shell=True)
