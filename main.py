from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from multi_reaper_class import MultiReaper
from plotter_class import Plotter
from micro_fighter_class import Micro_fighter
#from plot_matrix import plot_matrix

import cmlib
import os
import logging
logging.info('Program starts!')

action = {'middle':0, 'final':0, 'micro':1, 'plot':0, 'plot_matrix':0}
option = {'mid_granu':10, 'final_granu':20} # fin_gra should be mid_gra * N # pfx paper

#index_list = [281,282,283,284,285,286,287,288,289,2810]
index_list = [283, 287]

dt_list = [1365579000, 1365604200, 1365630600, 1365631800, 1365634200, 1365636600, 1365637800, 1365639000, 1365640200, 1365642600, 1365646200, 1365658200, 1365661800, 1371748200, 1371749400, 1371751800, 1371753000, 1371754200]

#dt_list1 = [1365579000, 1365604200, 1365630600, 1365631800, 1365634200, 1365636600, 1365637800, 1365639000, 1365640200, 1365642600, 1365646200, 1365658200, 1365661800]
#dt_list2 = [1371748200, 1371749400, 1371751800, 1371753000, 1371754200]

reaperlist = list()
for i in index_list:
    # Note: different applications may require different monitor and prefix sets!
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as()

    if action['middle']:
        alarm = Alarm(my_period, option['mid_granu'])
        alarm.analyze_to_middle() # analyze all updates and store to middle output files

    dv_thre = 0.2 # HDVP
    uq_thre = 200 # HUQP

    reaper = Reaper(my_period, option['final_granu'], shift=0) # in most cases shift is 0
    reaper.set_dv_uq_thre(dv_thre, uq_thre)
    reaper.set_event_thre(0.005, 0.4, 0.8) # set this threshold to a small value
    reaperlist.append(reaper) 

    if action['final']:
        #----------------------------------
        # for the prefix paper 
        #reaper.analyze_pfx()

        #periods = [[],[],[],[],[],[]]
        # ready TODO record DV and UQ distribution for certain periods, e.g., 6 weeks? (stand-alone)
        # : in order to avoid the period when disruptive events happened
        # future TODO select results of only part of the monitors to observe its impact

        reaper.detect_event()
        #reaper.all_events_cluster()
        #reaper.all_events_tpattern()
        #reaper.all_events_ratios()
        #reaper.all_events_oriAS_distri()

    if action['plot']:
        plotter = Plotter(reaper)
        #plotter.TS_event_dot()
        #plotter.TS_event_cluster_dot()
        plotter.all_events_tpattern_curve()

        ''' 
        #--------------------------------------------
        # compare the time series under different parameters
        reaper21 = Reaper(my_period,20,0)
        reaper21.set_event_thre(0.005, 0.4, 0.75) # set this threshold to a small value
        reaper22 = Reaper(my_period,20,0)
        reaper22.set_event_thre(0.005, 0.4, 0.85) # set this threshold to a small value

        reaper31 = Reaper(my_period,10,0)
        reaper31.set_event_thre(0.005, 0.4, 0.8) # set this threshold to a small value
        reaper32 = Reaper(my_period,30,0)
        reaper32.set_event_thre(0.005, 0.4, 0.8) # set this threshold to a small value

        reaper_list = [reaper21, reaper, reaper22]
        reaper_list = [reaper31, reaper, reaper32]
        plotter.TS_all_event_curve(reaper_list)
        '''

        # plotter.scatter_all_rwidth_rsize()
        # plotter.scatter_all_ASratio_rsize()
        # plotter.scatter_all_pfxratio_rsize()    

        # TODO plotter = Plotter(reaper_lol) # list of reaper lists
        # plotter.TS_all_event_dot_compare()
        # plotter.CDF_all_event_onepctg_CDnum_compare()
        # plotter.CDF_all_event_updtpctg_CDnum_compare()

    if action['micro']:
        pfxset = set()
        f = open(datadir+'final_output/target_pfx.txt', 'r')
        for line in f:
            line = line.rstrip('\n')
            pfxset.add(line)
        f.close()

        my_period.pfx2as_LPM(pfxset)

    #------------------------------------------------------------
    # plot matrices of every middle file
    if action['plot_matrix']:
        mdir = my_period.get_middle_dir()
        plotdir = mdir + 'matrix/'
        cmlib.make_dir(plotdir)

        mfiles = os.listdir(mdir)
        for mf in mfiles:
            if not os.path.isfile(mdir+mf):
                mfiles.remove(mf)
            else:
                print 'Ploting matrix:', mdir+mf
                plot_matrix(mdir+mf, plotdir+mf.split('.')[0]+'.pdf') #TODO specify a range?


#------------------------------------------------------------------
#combined analysis of all reapers
mr = MultiReaper(reaperlist)
#mr.get_common_pfx_set(dt_list)
#mr.all_events_cluster()
#pl = Plotter(reaper)
#pl.set_multi_reaper(mr)
#pl.TS_event_cluster_dot_mr()

#pl.ratios_dot_mr()
#pl.width_dot_mr()
#pl.size_CDFs_mr()
logging.info('Program ends!')
