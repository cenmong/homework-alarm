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
index_list = [282]
#index_list = [281,282,283,284,285,286,287,288,289,2810]

pfx_set = set()
f = open(datadir+'final_output/compfx_cluster1_1.txt')
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


    #mf.event_update_pattern(1360813800) # largest event in 01~10 2013
    #mf.top_AS_ASlink(1360813800)
    #mf.oriAS_in_updt(1360813800, None)

    '''
    #----------------------------------------
    # Get pfx->origin AS within all LBEs
    events = reaper.get_event_dict()
    for unix_dt in events.keys():
        thelist = events[unix_dt]
        rsize = thelist[0]
        if rsize > global_rsize_threshold:
            mf.oriAS_in_updt(unix_dt, None)

    #----------------------------------------
    # Get pfx->origin AS within an LBE for certain prefixes (optional)
    for dt in cluster1_1:
        try:
            mf.oriAS_in_updt(dt, pfx_set)
        except: # no such dt in the period
            pass

    #-----------------------------------------
    # Get update pattern for certain prefixes and certain LBEs
    for dt in cluster3:
        try:
            mf.event_update_pattern(dt, pfx_set)
        except:
            pass

    #----------------------------------
    # obtain the update pattern for all LBEs
    events = reaper.get_event_dict()
    for unix_dt in events.keys():
        thelist = events[unix_dt]
        rsize = thelist[0]
        if rsize > global_rsize_threshold:
            mf.event_update_pattern(unix_dt, None) # Do only once for each LBE

    #micro_fighter.set_sedate(sdt_obj, edt_obj)
    #micro_fighter.analyze_pfx()

    #mf.event_as_link_rank(1365604200)
    #mf.event_analyze_pfx(1365604200)
    '''

    pl = Plotter(reaper)

    #sdt_unix = 1365579000 
    #sdt_unix = 1365576600 
    #edt_unix = 1365661800

    sdt_unix = 1360813800
    edt_unix = 1360815000

    #sdt_unix = 1378887000
    #edt_unix = 1378899000
    pfile_path = final_output_root + 'compfx_largestLBE.txt'

    # get the common monitor set
    mfile_path = final_output_root + 'com_mon_largestLBE.txt'
    mf.get_candidate_as(mfile_path, pfile_path, sdt_unix, edt_unix)

    '''
    f = open(mfile_path, 'r')
    count = 0
    for line in f:
        mip = line.rstrip('\n')

        count += 1
        print '# ', count
        print mip
        mf.upattern_mon_pfxset_intime(mip, pfile_path, sdt_unix, edt_unix) # get and record update pattern

        print 'Plotting...'
        # update pattern file path
        upfile = final_output_root + 'upattern_TS/' + str(sdt_unix) + '_' + str(edt_unix) + '/' +\
                pfile_path.split('/')[-1] + '/' + mip + '.txt'
        pl.upattern_TS_for_mon(upfile)

    f.close()
    '''
