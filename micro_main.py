from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from plotter_class import Plotter
from micro_fighter_class import Micro_fighter
from meliae import scanner
scanner.dump_all_objects('memory.json')

import datetime
import cmlib
import os
import logging
import ast
logging.info('Program starts!')

#index_list = [0,1,2,3,4,5,6,7,8,10,11,13,14,15,16,19,20,21,22,23,24]
#index_list = [282]
index_list = [281,282,283,284,285,286,287,288,289,2810]

dt_list = [1365579000, 1365604200, 1365630600, 1365631800, 1365634200, 1365636600, 1365637800, 1365639000, 1365640200, 1365642600, 1365646200, 1365658200, 1365661800, 1371748200, 1371749400, 1371751800, 1371753000, 1371754200]
pfx_set = set()
f = open(datadir+'final_output/target_pfx.txt')
for line in f:
    line = line.rstrip('\n')
    pfx_set.add(line)
f.close()

for index in index_list:
    print 'index = ', index

    #-------------------------------------------
    # Still, we only care about the peers with global view
    my_period = Period(index)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as()

    #---------------------------------------------
    reaper = Reaper(my_period, 20, 0)
    reaper.set_event_thre(0.005, 0.4, 0.8) # to find the correct directory

    mf = Micro_fighter(reaper) # initialize
    #mf.event_update_pattern(1360813800) # largest event in 2013
    #mf.top_AS_ASlink(1360813800)

    # Get pfx->origin AS within an LBE for certain prefixes (optional)
    for dt in dt_list:
        try:
            mf.oriAS_in_updt(dt, pfx_set)
        except: # no such dt in the period
            pass

    #mf.oriAS_in_updt(1360813800, None)


    #----------------------------------
    # obtain the update pattern for LBEs
    '''
    events = reaper.get_events_list()
    for unix_dt in events.keys():
        thelist = events[unix_dt]
        rsize = thelist[0]
        if rsize > global_rsize_threshold:
            mf.event_update_pattern(unix_dt)
    '''

    #micro_fighter.set_sedate(sdt_obj, edt_obj)
    #micro_fighter.analyze_pfx()

    #mf.event_as_link_rank(1365604200)
    #mf.event_analyze_pfx(1365604200)

    #TODO remove the result file if exists

    '''
    ASes = [9121, 47331]
    sdt_obj = datetime.datetime(2013,4,10,0,0)
    edt_obj = datetime.datetime(2013,4,12,0,0)
    mf.analyze_pfx_indate(ASes, sdt_obj, edt_obj)
    '''

    # analyze certain prefixes (optional) define pfx_target = dict() for quick access
    # group prefixes into ASes (very meaningful)
