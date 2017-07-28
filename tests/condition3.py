import sys
import os
import string
import re
import argparse
import glob
from pprint import pprint,pformat
from cassandra.cluster import Cluster


def f(x):
	return int(x[0])

## ==============================================
## main
## ==============================================
cluster = Cluster() 
session = cluster.connect('Keyspace1')

for d_id in range(1,11):
	print '----------------------------------'
	print 'District #' + str(d_id)
	orders = map(f,session.execute(""" SELECT blobastext(value) FROM "NEW_ORDER" WHERE column1='NO_O_ID' ALLOW FILTERING  """))
	left = (max(orders) - min(orders) + 1)
	print 'left: ' + str(left)


	dist_orders = session.execute("""SELECT key FROM "NEW_ORDER" WHERE column1='NO_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
	right = len(map(f,dist_orders))
	print 'right: ' + str (right)
	
	if right == left:
		print "Consistency Condition Satisfied"
	else:
		print "Consistency Condition Violated"

    
## MAIN
