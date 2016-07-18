#!/usr/bin/env python

import os
import fileinput
import json
import sys
import time
import subprocess    

## Script requires 2 arguments.
## 1st- File with different Node names
## 2nd- File with list of recipes to be installed on the above IP's

args=sys.argv
node_file=args[1]
recipe_file=args[2]

## Reading IP file 

with open(node_file,'r') as f:
    for line in f:
        node=line.split(",")[1].strip()
	#print "node name:",node
	print node
        with open(recipe_file,'r') as fr:
	    for i in fr:
	        recipe=i.strip()
		print recipe
		cmd='''cd /root/chef-repo/.chef
		    knife node run_list set %s "recipe[%s]"         
		    '''	

		print cmd
		os.system(cmd %(node,recipe))
		

f.close()
fr.close()

## Running cookbooks on the above specified Chef Nodes

with open(node_file,'r') as f3:
    for line in f3:
        curr_ip=line.split(",")[0].strip()
	print "ip:",curr_ip
        cmd4="sh execute-chef-recipes.sh "+curr_ip
        subprocess.Popen(cmd4, shell=True)

