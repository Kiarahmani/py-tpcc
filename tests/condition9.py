import sys
import os
import string
import re
import argparse
import glob
from pprint import pprint,pformat
from cassandra.cluster import Cluster


def f(x):
	return float(x[0][0])

## ==============================================
## main
## ==============================================
cluster = Cluster() 
session = cluster.connect('Keyspace1')

for d_id in range(1,11):
	print '-----------------------'
	print 'District #' + str(d_id)
	d_key = session.execute("""SELECT blobastext(key) FROM "DISTRICT" WHERE column1='D_ID' AND value=textasblob('"""+str(d_id)+"""') ALLOW FILTERING""")[0][0]
	left = session.execute(""" SELECT blobastext(value) FROM "DISTRICT" WHERE key=textasblob('"""+str(d_key)+"""') AND column1='D_YTD' """)[0][0]
	print 'D_YTD: ' + str(left)

	h_d_keys = session.execute("""SELECT blobastext(key) FROM "HISTORY" WHERE column1='H_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
	h_amounts = []
	for key in h_d_keys:
		h_amounts.append(session.execute(""" SELECT blobastext(value) FROM "HISTORY" WHERE key=textasblob('"""+str(key[0])+"""') AND column1='H_AMOUNT'"""))

	right = sum(map(f,h_amounts))
	print 'sum(H_AMOUNT): ' + str(right)    
## MAIN
