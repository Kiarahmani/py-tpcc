import sys
import os
import string
import re
import argparse
import glob
from pprint import pprint,pformat
from cassandra.cluster import Cluster



def f(x):
	return ((x[0]),int(x[1]))

def g(x):
	return x[0]

def h(x):
	return int(x[0])

def numeberOfRows(o_id,o_d_id):
	key_o_id = map(g,session.execute("""SELECT blobastext(key) FROM "ORDER_LINE" WHERE column1='OL_O_ID' AND value=textasblob('"""+str(o_id)+"""')  ALLOW FILTERING """))
	ol_d_id = []
	for key in key_o_id:
		ol_d_id.append (map(g,session.execute("""SELECT blobastext(value) FROM "ORDER_LINE" WHERE column1='OL_D_ID' AND key=textasblob('"""+str(key)+"""') """)))
	key_result = filter(lambda x: x==int(o_d_id),map(h,ol_d_id))

	return len(key_result)


## ==============================================
## main
## ==============================================
cluster = Cluster() 
session = cluster.connect('Keyspace1')

orders = session.execute("""SELECT key,blobastext(value) FROM "ORDERS" WHERE column1='O_OL_CNT'  ALLOW FILTERING""")
result = True
i = 1
for order in map(f,orders):
	i = i+1
	key = order[0]
	o_ol_cnt = order[1]
	o_id = session.execute("""SELECT blobastext(value) FROM "ORDERS" WHERE key=textasblob('"""+str(key)+"""') AND column1='O_ID'""")[0][0]
	o_d_id = session.execute("""SELECT blobastext(value) FROM "ORDERS" WHERE key=textasblob('"""+str(key)+"""') AND column1='O_D_ID'""")[0][0]
	if (int(o_d_id)<=10):
		nof = numeberOfRows(o_id,o_d_id)
		print 'District: ' + str(o_d_id)
		print 'O_OL_CONT: ' + str(o_ol_cnt)
		print 'Number of Rows: ' + str(nof)
		if (nof!=o_ol_cnt):
			result = False
			break
		print '_______________'

print "======================="
if result:
	print 'Consistency Condition #6 Preserved'
else:
	print 'Consistency Condition #6 violated' 

## MAIN
