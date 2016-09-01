#!/usr/bin/env python
import random
import fileinput
import csv
import os
import sys

args=sys.argv

#Input file
#f_input=raw_input("Enter the input file name for sampling:")
f_input=args[1]

#Output file
#f_output=raw_input("Enter the output file name for sampling:")
#f_output='out.txt'
f_output=args[2]

def calc_file_size(filename):
    statinfo= os.stat(filename)
    size_in_bytes= statinfo.st_size
    # size_in_mb= float(size_in_bytes)/(1024*1024)
    # size_in_gb= float(size_in_bytes)/(1024*1024*1024)
    return size_in_bytes

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

y=calc_lines_for_output()
total_output_lines=y['total_out_lines']
tot_chunks=y['chunks']

lines_per_chunk = int(total_output_lines)/tot_chunks

f = open(f_input)
f1 = open(f_output,'a')
for piece in read_in_chunks(f):
    if(len(piece)>lines_per_chunk):
        lines = random.sample(piece,int(lines_per_chunk))

    else:
        remaining_chunk= int(total_output_lines) - calc_recs_in_file(f_output)
        lines = random.sample(piece,int(remaining_chunk))
    for i in lines:
            f1.write(i)

f.close()
f1.close()
