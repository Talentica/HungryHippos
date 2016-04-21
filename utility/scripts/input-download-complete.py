#!/usr/bin/env python
import fileinput
from kazoo.client import KazooClient

## Read Zookeeper's IP from the file
f=open('/root/hungryhippos/tmp/master_ip_file')

for line in f:
    hostname=line.strip()

## Connect Kazoo Client to Zookeeper
zk = KazooClient(hosts=hostname)
zk.start()

input_download_complete='/rootnode/alertsnode/INPUT_DOWNLOAD_COMPLETED'
zk.create(input_download_complete)
