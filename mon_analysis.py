from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from plotter_class import Plotter

import cmlib
import os
import subprocess
from cStringIO import StringIO

index_list = [20]

for i in index_list:
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as()

    my_period.get_as2namenation()
    my_period.get_mo2cc()
    my_period.get_mo2tier()

    #show monitor name and nation
    nation_count = dict()
    tier_count = dict()

    count = 0
    for co in my_period.co_mo:
        for m in my_period.co_mo[co]:
            count += 1
            asn = my_period.mo_asn[m]
            try:
                name = my_period.as2name[asn]
            except:
                name = '-1'
            try:
                nation = my_period.as2nation[asn]
            except:
                nation = '-1'
            try:
                nation_count[nation] += 1
            except:
                nation_count[nation] = 1
            cc = my_period.mo_cc[m]
            tier = my_period.mo_tier[m]
            try:
                tier_count[tier] += 1
            except:
                tier_count[tier] = 1
            print m,asn,name,nation,cc,tier

    print count
    print nation_count
    print tier_count

    '''
    #--------------------
    # get all ASes from RIB
    f = open(my_period.rib_info_file, 'r')
    ribfile = None
    for line in f:
        line = line.rstrip('\n')
        co = line.split(':')[0]
        if co == '':
            ribfile = line.split(':')[1]
    f.close()

    as_set = set()
    p = subprocess.Popen(['zcat', ribfile], stdout=subprocess.PIPE)
    f = StringIO(p.communicate()[0])
    assert p.returncode == 0
    for line in f:
        line = line.rstrip('\n')
        try:
            aspath = line.split('|')[6]
        except:
            continue
        aslist = aspath.split()
        for x in aslist:
            try:
                asn = int(x)
            except:
                continue
            as_set.add(asn)
    f.close()

    print 'AS quantity: ', len(as_set)

    my_period.get_as2cc_file()
    as2cc = my_period.get_as2cc_dict()
    as2tier = dict()
    for a in as2cc:
        if a in tier1_asn:
            as2tier[a] = 1
            continue
        cc = as2cc[a]
        if cc < 0:
            as2tier[a] = -1 #unknown
        elif cc <= 4:
            as2tier[a] = 999
        elif cc <= 50:
            as2tier[a] = 3
        else:
            as2tier[a] = 2

    count_dict = dict()
    for a in as2tier:
        tier = as2tier[a]
        try:
            count_dict[tier] += 1
        except:
            count_dict[tier] = 1

    print count_dict
    '''
