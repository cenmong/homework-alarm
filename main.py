from micro_fighter_class import Micro_fighter
from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from multi_reaper_class import MultiReaper
from plotter_class import Plotter
#from plot_matrix import plot_matrix

import cmlib
import os
import logging
logging.info('Program starts!')

action = {'middle':0, 'final':0, 'micro':0, 'plot':0, 'plot_matrix':0, 'MR':1}
option = {'mid_granu':10, 'final_granu':20} # fin_gra should be mid_gra * N # pfx paper

#index_list = [281, 282, 283, 284,285,286,287,288,289,2810]
index_list = [284, 286]

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


    reaper = Reaper(my_period, option['final_granu'], shift=0) # in most cases shift is 0
    reaper.set_event_thre(0.005, 0.4, 0.8) # set this threshold to a small value
    reaper.set_Tq_Tv(100, 0.4)
    #reaper.set_Tq_Tv(85, 0.35)
    #reaper.set_Tq_Tv(115, 0.45)


    if action['final']:
        #----------------------------------
        # for the prefix paper 
        #reaper.TS_updt_num()
        #reaper.huvp_huqp_TS() # run only once!
        #reaper.get_pfx_data() # get the uv and uq for every prefix in every slot (run only once)
        #reaper.uv_uq_distr()# (run only once)
        #reaper.analyze_pfx() # no use any more

        #--------------------------------
        # for detecting large-scale BGP events
        #reaper.detect_event()
        #reaper.all_events_cluster()
        #reaper.all_events_tpattern()
        #reaper.all_events_ratios()
        reaper.all_events_oriAS_distri()

        pass

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

        #------------------------------------------
        # the impact of Tv and Tq
        reaper1 = Reaper(my_period,20,0)
        reaper1.set_Tq_Tv(85, 0.35)
        reaper2 = Reaper(my_period,20,0)
        reaper2.set_Tq_Tv(100, 0.4)
        reaper3 = Reaper(my_period,20,0)
        reaper3.set_Tq_Tv(115, 0.45)
        reaper_list = [reaper1, reaper2, reaper3]
        plotter.hpfx_compare(reaper_list)

        reaper1 = Reaper(my_period,10,0)
        reaper1.set_Tq_Tv(100, 0.4)
        reaper2 = Reaper(my_period,20,0)
        reaper2.set_Tq_Tv(100, 0.4)
        reaper3 = Reaper(my_period,30,0)
        reaper3.set_Tq_Tv(100, 0.4)
        reaper_list = [reaper1, reaper2, reaper3]
        plotter.hpfx_compare_slot(reaper_list)
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
        f = open(datadir+'final_output/compfx_cluster3.txt', 'r')
        for line in f:
            line = line.rstrip('\n')
            pfxset.add(line)
        f.close()

        my_period.pfx2as_LPM(pfxset)

        #mf = Micro_fighter(reaper)
        #mf.analyze_slot(1369102200)

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

    reaperlist.append(reaper) 


#------------------------------------------------------------------
#combined analysis of all reapers
if action['MR']:
    mr = MultiReaper(reaperlist)

    #-------------------------------------------------------------------
    # Get the existenc frequency of certain AS in the AS paths in the updates for some prefixes
    '''
    pfx_set = set()
    f = open(datadir+'final_output/target_pfx.txt')
    for line in f:
        line = line.rstrip('\n')
        pfx_set.add(line)
    f.close()
    '''
    #mr.AS_exist_in_ASpath_in_updt(dt_list, 9121, pfx_set)
    #mr.new_hpfx() # note: data from previous slot will be used (run once)
    #mr.hpfx_life_time() # (run once for used months only)
    #mr.hpfx_life_time_details() # interesting details analyzing the output of mr.hpfx_life_time()

    mr.get_common_pfx_mon_set(cluster1_1)
    #mr.all_events_cluster()
    #mr.random_slots_upattern(18)

    #mr.get_all_LBE_basic() # Get the basic features (e.g., height). Tag clusters

    #-------------------------------------------------------------
    # plot multiple reapers

    #pl = Plotter(reaper)
    #pl.set_multi_reaper(mr)
    #pl.LBE_updt_pattern(cluster1, len(cluster1))
    #pl.events_oriAS_distr_mr() # The origin ASes of LBEs

    #pl.hpfx_lifetime_distr_mr()
    #pl.TS_hpfx_mr()
    #pl.box_hpfx_mr()
    #pl.new_pfx_mr()
    #pl.TS_total_huvp_huqp_updt_mr()
    #pl.HUQOP_UV_box_mr()
    #pl.hratio_box()
    #pl.uv_uq_distr_mr()
    #pl.updt_ratio_box()
    #pl.TS_event_cluster_dot_mr()

    #pl.ratios_dot_mr()
    #pl.width_dot_mr()
    #pl.size_CDFs_mr()

logging.info('Program ends!')
