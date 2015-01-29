from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from meliae import scanner
scanner.dump_all_objects('memory.json')
from plot_matrix import plot_matrix

import cmlib
import os
import logging
logging.info('Program starts!')

# future TODO Microscopic analysis (e.g., case studies, update content) should be in another logic
# analyze certain prefix, certain AS, certain period, etc.

#action = {'middle':False, 'final':False, 'plot':False} # Specify what to do
#action = {'middle':True, 'final':False, 'plot':False} # Specify what to do
action = {'middle':False, 'final':True, 'plot':False}
#action = {'middle':False, 'final':False, 'plot':True}
option = {'mid_granu':10, 'final_granu':60} # fin_gra should be mid_gra * N

index_list = [27]

for i in index_list:
    # Note: different applications may require different monitor and prefix sets!
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    #my_period.rm_dup_mo() # rm multiple existence of the same monitor
    #my_period.mo_filter_same_as()

    my_period.get_as2namenation()
    my_period.get_mo2cc()
    my_period.get_mo2tier()

    '''
    #show monitor name and nation
    for co in my_period.co_mo:
        for m in my_period.co_mo[co]:
            asn = my_period.mo_asn[m]
            print m,asn,my_period.as2name[asn],my_period.as2nation[asn],my_period.mo_cc[m],my_period.mo_tier[m]
    '''

    if action['middle']:
        alarm = Alarm(my_period, option['mid_granu'])
        alarm.analyze_to_middle() # analyze all updates and store to middle output files

    if action['final']:
        # for paper 1
        reaper = Reaper(my_period, option['final_granu'], shift=0) # in most cases shift is 0
        reaper.set_dv_uq_thre(0.15, 150)
        #periods = [[],[],[],[],[],[]]
        reaper.analyze()
        # ready TODO record DV and UQ distribution for certain periods, e.g., 6 weeks? (stand-alone)
        # : in order to avoid the period when disruptive events happened
        '''
        # for paper 2
        reaper.calculate()
        '''
        # future TODO select results of only part of the monitors to observe its impact

    if action['plot']:
        # TODO No or little logic in plotting
        '''
        plotter = Plotter(my_period)
        '''

    '''
    # plot matrices of every time slot only for observation
    mdir = my_period.get_middle_dir()
    mfiles = os.listdir(mdir)
    for mf in mfiles:
        if not os.path.isfile(mdir+mf):
            mfiles.remove(mf)

    plotdir = mdir + 'matrix/'
    cmlib.make_dir(plotdir)

    for mf in mfiles:
        pf = mf.split('.')[0] + '.pdf'
        print 'Ploting matrix:', mdir+mf
        plot_matrix(mdir+mf, plotdir+pf) #TODO specify a range?
    '''

logging.info('Program ends!')
