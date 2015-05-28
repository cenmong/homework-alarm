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
logging.info('Program starts!')

index_list = [0,1,2,3,4,5,6,7,8,10,11,13,14,15,16,19,20,21,22,23,24]

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
    #micro_fighter.set_sedate(sdt_obj, edt_obj)
    #micro_fighter.analyze_pfx()

    #mf.event_as_link_rank(1365604200)
    #mf.event_analyze_pfx(1365604200)

    #TODO remove the result file if exists
    #mf.all_events_ratios()

    #mf.all_events_tpattern()
    mf.all_events_cluster()

    '''
    ASes = [9121, 47331]
    sdt_obj = datetime.datetime(2013,4,10,0,0)
    edt_obj = datetime.datetime(2013,4,12,0,0)
    mf.analyze_pfx_indate(ASes, sdt_obj, edt_obj)
    '''

    # analyze certain prefixes (optional) define pfx_target = dict() for quick access
    # group prefixes into ASes (very meaningful)
