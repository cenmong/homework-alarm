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
index_list = [289]
#index_list = [281,282,283,284,285,286,287,288,289,2810]

#pfx_set = set()
#f = open(datadir+'final_output/compfx_cluster1_1.txt')
#for line in f:
#    line = line.rstrip('\n')
#    pfx_set.add(line)
#f.close()

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

    #----------------------------------
    # obtain the update pattern for all LBEs
    events = reaper.get_event_dict()
    for unix_dt in events.keys():
        if unix_dt not in cluster3:
            continue
        thelist = events[unix_dt]
        rsize = thelist[0]
        if rsize > global_rsize_threshold:
            mf.event_update_pattern(unix_dt, None) # Do only once for each LBE # FIXME 

    #micro_fighter.set_sedate(sdt_obj, edt_obj)
    #micro_fighter.analyze_pfx()

    #mf.event_as_link_rank(1365604200)
    #mf.event_analyze_pfx(1365604200)
    '''
    pl = Plotter(reaper)

    #mf.get_rib_end_states(sdt_unix, sdt_unix, sdt_unix, edt_unix)
    rib_unix = cluster3[0]
    mfile = final_output_root + 'com_mon_cluster3.txt'
    pfile = final_output_root + 'compfx_cluster3.txt'
    sdt_unix = 1378887000
    edt_unix = 1378899000
    #mf.get_upattern_pmfile(pfile, mfile, sdt_unix, edt_unix)
    #mf.get_LPM_in_rib_pmfile(pfile, mfile, rib_unix)
    #mf.get_as_recall_in_rib_pmfile('non_exact_pset.txt', mfile, rib_unix)
    #mf.get_as_recall_in_update_pmfile(pfile, mfile, sdt_unix, edt_unix)
    #mf.get_change_detail(sdt_unix, sdt_unix, sdt_unix + 1200)
    #mf.get_as_recall_in_rib(sdt_unix, rib_unix)
    #mf.get_as_precision_in_rib(sdt_unix, rib_unix)
    #mf.get_origin_in_rib(sdt_unix, rib_unix)
    sdt_list = list()
    edt_list = list()
    count = 0
    #for sdt_unix in cluster1:
    #    count += 1
    #    sdt_list.append(sdt_unix)
    #    edt_list.append(sdt_unix+1200)
        #print 'Now:', count
        #mf.get_origin_in_rib(sdt_unix, rib_unix)
        #mf.get_withdrawn_pfx(sdt_unix, rib_unix, sdt_unix, sdt_unix + 1200)
    
        #mf.get_as_recall_in_rib(sdt_unix, rib_unix)
        #mf.get_as_precision_in_rib(sdt_unix, rib_unix)


    # Plotting...
    #pl.rib_end_change(sdt_unix, rib_unix, sdt_unix, edt_unix)
    #pl.withdraw_ratio_9121(sdt_list, sdt_list, edt_list)


    f = open(mfile, 'r')
    count = 0
    for line in f:
        mip = line.rstrip('\n')

        count += 1
        print '# ', count
        print mip
        #mf.upattern_mon_pfxset_intime(mip, pfile, mfile, sdt_unix, edt_unix) # get and record update pattern

        print 'Plotting...'
        # update pattern file path
        upfile = final_output_root + 'upattern_TS/' + str(sdt_unix) + '_' + str(edt_unix) + '/' +\
                pfile.split('/')[-1] + '/' + mip + '.txt'
        pl.upattern_TS_for_mon(upfile)

    f.close()
