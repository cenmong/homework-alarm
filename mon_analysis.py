from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from plotter_class import Plotter

import cmlib
import os

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
            nation = my_period.as2nation[asn]
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
            print m,asn,name,my_period.as2nation[asn],my_period.mo_cc[m],my_period.mo_tier[m]

    print count
    print nation_count
    print tier_count
