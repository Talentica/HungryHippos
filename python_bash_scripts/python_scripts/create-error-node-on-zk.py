#!/usr/bin/env python
import sys
import fileinput
from kazoo.client import KazooClient

args=sys.argv
uuid=args[1]
    
master_ip_file_path="/root/hungryhippos/"+uuid+"/master_ip_file"
    
## Reading IP of Zookeeper
f=open(master_ip_file_path)
    
for line in f:
    hostname=line.strip()
    
## Starting Kazoo Client
zk = KazooClient(hosts=hostname)
zk.start()
    
## Watcher on Zookeeper for Alerts
path_alert="/rootnode/alertsnode/ERROR_ENCOUNTERED"
zk.create(path_alert)

path_alert="/rootnode/alertsnode/ERROR_ENCOUNTERED"
zk.create(path_alert)
