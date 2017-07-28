#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

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

print "Enter District Number:"
d_id = 2#input()
assert d_id in range(1,11)


d_key = session.execute("""SELECT blobastext(key) FROM "DISTRICT" WHERE column1='D_ID' AND value=textasblob('"""+str(d_id)+"""') ALLOW FILTERING""")[0][0]
left = session.execute(""" SELECT blobastext(value) FROM "DISTRICT" WHERE key=textasblob('"""+str(d_key)+"""') AND column1='D_YTD' """)[0][0]

print 'D_YTD: ' + str(left)




h_d_keys = session.execute("""SELECT blobastext(key) FROM "HISTORY" WHERE column1='H_D_ID' AND value=textasblob('"""+str(d_id)+"""')  ALLOW FILTERING""")
h_amounts = []
for key in h_d_keys:
	h_amounts.append(session.execute(""" SELECT blobastext(value) FROM "HISTORY" WHERE key=textasblob('"""+str(key[0])+"""') AND column1='H_AMOUNT'"""))

right = sum(map(f,h_amounts))
print 'sum(H_AMOUNT): ' + str(right)




'''
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
'''


	










































    
## MAIN
