#!/usr/bin/env python
import random
import fileinput
import csv
import os
import MySQLdb
import sys
from kazoo.client import KazooClient

#Input file
#f_input=raw_input("Enter the input file name for sampling:")
f_input='/root/hungryhippos/input/input.txt'

#Output file
#f_output=raw_input("Enter the output file name for sampling:")
#f_output='out.txt'
f_output='/root/hungryhippos/sharding/sample_input.txt'

args=sys.argv
job_uuid=args[1]

mysql_server_ip=args[2]

## Read Zookeeper's IP from the file
master_ip_file_path="/root/hungryhippos/"+job_uuid+"/master_ip_file"

f=open(master_ip_file_path)

for line in f:
    hostname=line.strip()

## Connect Kazoo Client to Zookeeper
zk = KazooClient(hosts=hostname)
zk.start()

## Path of Zookeeper node
zk_path='/rootnode/alertsnode/'

def calc_file_size(filename):
    statinfo= os.stat(filename)
    size_in_bytes= statinfo.st_size
    # size_in_mb= float(size_in_bytes)/(1024*1024)
    # size_in_gb= float(size_in_bytes)/(1024*1024*1024)

    return size_in_bytes
    if (size_in_bytes==0):
        print ("Empty File. Please download the file again.")

def calc_recs_in_file(filename):
    with open(filename,'rU') as csvfile:
        total_recs=sum(1 for _ in csvfile)
        return total_recs

def read_in_chunks(file_object, chunk_size=52428800):
    """    Default chunk size: 50 MB."""
    while True:
        data = file_object.readlines(chunk_size)
        if not data:
            break
        yield data

def calc_output_file_size():
    size=calc_file_size(f_input)
    chunks=float(size)/52428800

    samp_size= float(size)/10
    if (samp_size>1073741824):
        sample_size=1073741824
    else:
        sample_size=samp_size

    sizes={'tot_size':size,'sample_size':sample_size,'chunks':chunks}
    return sizes


def calc_lines_for_output():
    total_lines=calc_recs_in_file(f_input)
    x=calc_output_file_size()
    total_size=x['tot_size']
    size_of_sample=x['sample_size']
    chunks=x['chunks']

    size_per_line=float(total_size)/total_lines
    req_lines=size_of_sample/size_per_line

    out={'chunks':chunks,'total_out_lines':req_lines}
    return out

## Connection to MySql Database
db = MySQLdb.connect(host=mysql_server_ip,user="mysql_admin",passwd="password123",db="hungryhippos_tester")

## Function to insert "In-Progress" status into DB
def in_progress():
    cur = db.cursor()

    sql_begin="""
    update job a
    set date_time_submitted=now()
    where a.job_uuid= %s"""

    sql= """
    insert into
        process_instance(process_id,job_id)
    values
        ((select process_id from process where name='SAMPLING'),
         (select job_id from job where job_uuid= %s))"""

    sql1= """
    insert into
        process_instance_detail(process_instance_id,node_ip,status,execution_start_time)
    values
        ((Select process_instance_id from process_instance a, process b,job c
          where a.process_id=b.process_id
          and a.job_id=c.job_id
          and c.job_uuid= %s
          and b.name='SAMPLING')
        ,%s,"IN_PROGRESS",now())"""

    cur.execute(sql_begin, (job_uuid, ))
    db.commit()
    cur.execute(sql, (job_uuid, ))
    db.commit()
    cur.execute(sql1, (job_uuid,hostname))
    db.commit()

## Function to insert "Completed" status into DB
def completed_status():
    cur = db.cursor()
    sql_update= """
    update process_instance_detail a , process_instance b
    set a.status='COMPLETED',
        a.execution_end_time=now()
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='SAMPLING')"""

    cur.execute(sql_update, (job_uuid, ))
    db.commit()

## Function to insert "Failed" status into DB
def failed_status():
    cur = db.cursor()
    sql_failed= """
    update process_instance_detail a , process_instance b
    set a.status='FAILED',
        a.execution_end_time=now(),
        a.error_message='Download of the output file has failed. Please check!!'
    where a.process_instance_id=b.process_instance_id
    and b.job_id=(select job_id from job where job_uuid= %s)
    and b.process_id=(select process_id from process where name='SAMPLING')"""

    cur.execute(sql_failed, (job_uuid, ))
    db.commit()


y=calc_lines_for_output()
total_output_lines=y['total_out_lines']
tot_chunks=y['chunks']

lines_per_chunk = int(total_output_lines)/tot_chunks

f = open(f_input)

in_progress()

try:
    for piece in read_in_chunks(f):
        if(len(piece)>lines_per_chunk):
            lines = random.sample(piece,int(lines_per_chunk))

        else:
            remaining_chunk= int(total_output_lines) - calc_recs_in_file(f_output)
            lines = random.sample(piece,int(remaining_chunk))
        for i in lines:
                f1 = open(f_output,'a')
                f1.write(i)

    completed_status()
    sampling_znode=zk_path+"/"+"SAMPLING_COMPLETED"
    zk.create(sampling_znode)
except:
    failed_status()

f.close()
f1.close()
