#!/usr/bin/env python
import os
import sys
import time
import fileinput
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

path="/rootnode/hostsnode"

## Watcher on Zookeeper 
child=set()

@zk.ChildrenWatch(path)
def watch1(c):
    for i in c:
        child.add(i)


while True:
    if 'ALL_OUTPUT_FILES_DOWNLOADED' in child:
        print "Output files ready to be Zipped!!"
	cmd="sh /root/hungryhippos/scripts/bash_scripts/zip-output-files.sh"+" "+uuid
	rc=os.system(cmd)
        if (rc==0):
            zipped_znode=path+"/"+"OUTPUT_FILES_ZIPPED_AND_TRANSFERRED"
            zk.create(zipped_znode)
        break

    else:
        time.sleep(10)
        count=count+1
        print "count:",count

