import sys
import os
import string
import re
import argparse
import glob
from pprint import pprint,pformat
from cassandra.cluster import Cluster


def f(x):
	return float(x[0])


## ==============================================
## main
## ==============================================
cluster = Cluster() 
session = cluster.connect('Keyspace1')

#print "Enter District Number:"

w_ydt = session.execute(""" SELECT blobastext(value) FROM "WAREHOUSE" WHERE column1='W_YTD' ALLOW FILTERING  """)
left = w_ydt[0][0]
print 'W_YTD: ' + str(left)


d_ytds = map(f,session.execute(""" SELECT blobastext(value) FROM "DISTRICT" WHERE column1='D_YTD' ALLOW FILTERING """))
right = sum(d_ytds)
print 'sum(D_YTD): ' + str(right)


if str(left) == str(right):
	print "Consistency Condition Satisfied"
else:
	print "Consistency Condition Violated"

## MAIN
