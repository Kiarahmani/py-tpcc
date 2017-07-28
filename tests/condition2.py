import sys
import os
import string
import re
import argparse
import glob
from pprint import pprint,pformat
from cassandra.cluster import Cluster


def f(x):
	return int(x[0][0])


## ==============================================
## main
## ==============================================
cluster = Cluster() 
session = cluster.connect('Keyspace1')

for d_id in range (1,11):

	# orders processed by district d_id
	orders = session.execute("""SELECT key,column1,value FROM "ORDERS" WHERE column1='O_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
	o_id = []
	for order in orders:
		o_id.append (order[0])

	results = []
	for id in o_id: 
		results_iter = session.execute("""SELECT blobastext(value) FROM "ORDERS" WHERE key=textasblob('"""+str(id)+"""') AND column1='O_ID'""")
		results.append(results_iter)

	d_next_key = session.execute("""SELECT blobastext(key) FROM "DISTRICT" WHERE column1='D_ID' AND value=textasblob('"""+str(d_id)+"""') ALLOW FILTERING""")[0][0]
	d_next = session.execute(""" SELECT blobastext(value) FROM "DISTRICT" WHERE key=textasblob('"""+str(d_next_key)+"""') AND column1='D_NEXT_O_ID' """)[0][0]

	print "max of O_ID: " + str (max(map(f,results)))
	print "D_NEXT_O_ID: " + d_next

	if max(map(f,results)) == int(d_next) - 1:
		print "Consistency Condition Satisfied"
	else:
		print "Consistency Condition Violated"
## MAIN
