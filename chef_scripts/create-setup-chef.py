#!/usr/bin/env python

import os
import fileinput
import json
import sys
import time
import subprocess    

## Script requires 2 arguments.
## 1st- Total number of nodes
## 2nd- Prefix name of the nodes

args=sys.argv
total_nodes=args[1]
name_of_node=args[2]

print "total:",total_nodes

## Function to create node name list for CURL

def create_name_list(tot_num_of_nodes,name):
    node_num=1
    list_for_name=list()

    while (len(list_for_name) < tot_num_of_nodes):
        list_for_name.append(name+"-"+str(node_num))
        node_num=node_num+1
    
    l=','.join(list_for_name)
    print list_for_name 
    return l

node_name_list=create_name_list(int(total_nodes),name_of_node)
print node_name_list



"""
## CURL command for Digital Ocean Droplets creation

cmd='''curl -X POST https://api.digitalocean.com/v2/droplets \
-H 'Content-Type: application/json' \
-H 'Authorization: Bearer c5f7329b92561d04df4942bcd1c9b74fdb3665eb2e7ff206f5b6c286bdd9d799' \
-d '{"names":[%s], "region":"nyc2", "size":"512mb",
     "image":"ubuntu-14-04-x64", "ssh_keys": [2066493,1699947,2067114],"backups":false, "ipv6":false, "private_networking":false,

    "user_data":"
#cloud-config

runcmd:
  - echo 45.55.51.90    mayank-chef-wk>>/etc/hosts
  - echo 45.55.51.80    mayank-chef-server>>/etc/hosts"}' | python -mjson.tool >id.json '''

"""

def create_droplets():
    os.system(cmd %node_name_list)
    id_file=open('id.json','r')
json_decode=json.load(id_file)

node_id_file='node_id_file.txt'
f=open(node_id_file,'a')

i=0
while(i< int(total_nodes)):
    node_id=json_decode.get('droplets')[i].get('id')
    f.write(str(node_id)+"\n")
    i=i+1

f.close()

## Retrieving IP addresses of the droplets created

node_ip_file='node_ip_file.txt'
f1=open(node_ip_file,'a')

cmd1='''curl -X GET -H "Content-Type: application/json" -H "Authorization: Bearer c5f7329b92561d04df4942bcd1c9b74fdb3665eb2e7ff206f5b6c286bdd9d799" "https://api.digitalocean.com/v2/droplets/%i" | python -mjson.tool >ip-%i.json '''

with open(node_id_file,'r') as ff:
    for line in ff:
        print "id:",line
	while True:
	    os.system(cmd1 %(int(line),int(line)))
	    ip_file=open('ip-'+line.strip()+'.json')
            json_decode_ip=json.load(ip_file)
	    node_v4=json_decode_ip.get('droplet').get('networks').get('v4')
	    if node_v4:
		node_ip=json_decode_ip.get('droplet').get('networks').get('v4')[0].get('ip_address')		
	        f1.write(str(node_ip)+"\n")
		break	
            else:
		time.sleep(20) 

f1.close()
ff.close()

## Waiting 8 seconds for letting Digital Ocean prepare nodes 

print "waiting for 60 secs"
time.sleep(60)
print "resuming"

## All IP's are recorded in the node_ip_file. Now, bootstrapping every node in Chef

cmd2='''cd /root/chef-repo/.chef 
knife bootstrap %s -x root -A
'''

with open(node_ip_file,'r') as f2:
    for line in f2:
	curr_ip=line.strip()
	os.system(cmd2 %curr_ip)
		
f2.close()

## Archiving all the intermediate .json and .txt files for logging purposes

cmd_archive='''cd /root/chef-setup-automation/archive
mkdir %s
mv /root/chef-setup-automation/node*txt %s
mv /root/chef-setup-automation/ip*json %s
'''
os.system(cmd_archive %(name_of_node,name_of_node,name_of_node))

