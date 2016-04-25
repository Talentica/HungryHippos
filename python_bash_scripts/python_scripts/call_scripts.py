#!/usr/bin/env python

import os
import sys

args=sys.argv

count=0
l=list()

for i in args:
    if (count >1):
        l.append(i)
    count=count+1

path=args[1]

newstr=' '.join(l)

cmd ="sh -x "+path+" "+newstr
cmd="java -cp ../lib/test-jobs.jar com.talentica.hungryHippos.test.sum.SumJobMatrixImpl"
a=os.system(cmd)
print "rc:",a
