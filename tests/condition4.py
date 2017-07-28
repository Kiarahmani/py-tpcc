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

print '=============='
for d_id in range (1,11):
	print '-----------'
	print str(d_id) + ': '
	result = []
	orders = session.execute("""SELECT key FROM "ORDERS" WHERE column1='O_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
	for order_key in orders:
		res_iter = session.execute("""SELECT blobastext(value) FROM "ORDERS" WHERE key=textasblob('"""+str(order_key[0])+"""') AND column1='O_OL_CNT'""")
		result.append(int(res_iter[0][0]))
	left = sum(result)
	print "left: " + str(left)

	orders_lines = session.execute("""SELECT key FROM "ORDER_LINE" WHERE column1='OL_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
	right = len(map(f,orders_lines))
	print "right: " + str(right)

	print 'diff: ' + str(right - left)
    
## MAIN
