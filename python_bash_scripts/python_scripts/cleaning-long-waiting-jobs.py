#!/usr/bin/python
import MySQLdb
import sys
import datetime
import fileinput
from kazoo.client import KazooClient

args=sys.argv

uuid=args[1]
mysql_server_ip=args[2]
hours_for_deletion=args[3]

master_ip_file_path="/root/hungryhippos/"+uuid+"/master_ip_file"

## Reading IP of Zookeeper
f=open(master_ip_file_path)

for line in f:
    hostname=line.strip()

## Starting Kazoo Client
zk = KazooClient(hosts=hostname)
zk.start()

## Recording current date and time. Time must be around 11PM
curr_time=datetime.datetime.now().replace(microsecond=0)

## Connection to MySql server
db = MySQLdb.connect(host=mysql_server_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")

cur = db.cursor()
sql= """SELECT status,date_time_submitted from job where job_uuid= %s"""

cur.execute(sql,(uuid,))
answer=cur.fetchall()
print answer

for i in answer:
    status= i[0]
    print status
    if (status!='COMPLETED') and (status!='FAILED'):
        dt=i[1]
	print "datetime submitted:",dt
	
	time_diff=curr_time-dt
	print "time difference:",time_diff
	diff_in_hours=time_diff.total_seconds()/(60*60)
	print "diff_in_hours",diff_in_hours
	print "type",type(diff_in_hours)
	if (diff_in_hours >= int(hours_for_deletion)):
	    print "exceeded"
	    zk.create("/rootnode/alertsnode/ERROR_ENCOUNTERED")
		
