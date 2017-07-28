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

w_ydt = session.execute(""" SELECT blobastext(value) FROM "WAREHOUSE" WHERE column1='W_YTD' ALLOW FILTERING  """)
left = w_ydt[0][0]
print 'W_YTD: ' + str(left)


h_amounts = map(f,session.execute(""" SELECT blobastext(value) FROM "HISTORY" WHERE column1='H_AMOUNT' ALLOW FILTERING  """))
print 'sum(H_AMOUNT): ' + str(sum(h_amounts))
    
## MAIN
