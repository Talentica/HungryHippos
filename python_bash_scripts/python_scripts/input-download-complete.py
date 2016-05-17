#!/usr/bin/env python
import fileinput
import sys
from kazoo.client import KazooClient

args=sys.argv
uuid=args[1]

## Read Zookeeper's IP from the file
master_ip_file_path="/root/hungryhippos/"+uuid+"/master_ip_file"

f=open(master_ip_file_path)
for line in f:
    hostname=line.strip()

## Connect Kazoo Client to Zookeeper
zk = KazooClient(hosts=hostname)
zk.start()

input_download_complete='/rootnode/alertsnode/INPUT_DOWNLOAD_COMPLETED'
zk.create(input_download_complete)
