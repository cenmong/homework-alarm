import datetime
import numpy as np
import calendar # do not use the time module
import cmlib
import operator
import string
import traceback
import logging
import subprocess
import re
import os
import ast

from cStringIO import StringIO
from netaddr import *
from env import *
#from supporter_class import *

import matplotlib
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
from matplotlib.dates import HourLocator
from matplotlib.dates import DayLocator
from matplotlib.patches import Ellipse
from matplotlib.patches import Rectangle
from matplotlib import cm

len2count = dict()
pfx2list = dict()
pfx2len = dict()

f = open('195.66.224.138result.txt', 'r')
for line in f:
    line = line.rstrip('\n')
    pfx = line.split(':')[0]
    ulist = ast.literal_eval(line.split(':')[1])
    pfx2list[pfx] = ulist
    ll = len(ulist)
    pfx2len[pfx] = ll
    try:
        len2count[ll] += 1
    except:
        len2count[ll] = 1
f.close()

maxlen = max(len2count.keys())

# order prefix by list length
sorted_pfx2len = sorted(pfx2len.items(), key=operator.itemgetter(1))

lol = list() # list of lists for plot

# extend the lists to the maximum length by filling in -1
for item in sorted_pfx2len:
    pfx = item[0]
    thel = pfx2len[pfx]
    extendl = maxlen - thel
    to_extend = [-1]*extendl
    l = pfx2list[pfx]
    l.extend(to_extend)
    lol.append(l)

value2color = {-1:'white',0:'cyan',1:'green',2:'red',3:'yellow',4:'blue',5:'black'}

#-------plot----------
fig = plt.figure(figsize=(10,11))
ax = fig.add_subplot(111)

cmap = matplotlib.colors.ListedColormap(['white','blue','cyan','springgreen','yellow','violet','black'])
cmap.set_over('0.25')
cmap.set_under('0.75')

cax = ax.imshow(lol, interpolation='nearest', aspect='auto', cmap=cmap)
#cbar = fig.colorbar(cax)

plt.savefig('pattern.pdf', bbox_inches='tight')
plt.clf() # clear the figure
plt.close()


#============================================
'''
pfx2AS = dict()
RIB_pfx_set = set()

f=open('rib.20130410.0000.bz2.txt','r')
for line in f:
    line = line.rstrip('\n')
    attr = line.split('|')
    mon = attr[3]
    #if mon != '195.66.224.138':
    #    continue
    pfx = attr[5]
    if ':' in pfx:
        continue
    origin_as = attr[6].split()[-1]
    if origin_as not in (['9121', '47331']):
        continue
    #begin = pfx.split('.')[0]
    #if begin not in (['212','85','88','93','81','95','78']):
    #    continue
    #if origin_as == '47331':
        #RIB_pfx_set.add(pfx)
    RIB_pfx_set.add(pfx)
    pfx2AS[pfx] = origin_as
f.close()

print len(RIB_pfx_set)



my_pfx_set = set()
f = open('target_pfx.txt', 'r')
for line in f:
    line = line.rstrip('\n')
    if ':' in line:
        continue
    my_pfx_set.add(line)
f.close()

print len(my_pfx_set)

begin_set = set()
for p in my_pfx_set:
    begin = p.split('.')[0]
    begin_set.add(begin)

common_pfx_set = RIB_pfx_set & my_pfx_set
print len(common_pfx_set)

#--------------------

new_bset = set()
RIB_bset = set()

new_pfx_set = my_pfx_set - common_pfx_set


for p in new_pfx_set:
    p = cmlib.ip4_to_binary(p)
    new_bset.add(p)

b2pfx = dict()

for p in RIB_pfx_set:
    bp = cmlib.ip4_to_binary(p)
    b2pfx[bp] = p
    RIB_bset.add(bp)


AS2count = dict()
par_set = set()
sub_set = set()
for p in new_bset:
    for pp in RIB_bset:
        if p.startswith(pp):
            sub_set.add(p)
            par_pfx = b2pfx[pp]
            par_set.add(par_pfx)
            par_AS = pfx2AS[par_pfx]
            try:
                AS2count[par_AS] += 1
            except:
                AS2count[par_AS] = 1
            break
print len(par_set),par_set
print len(sub_set)
print AS2count
'''
