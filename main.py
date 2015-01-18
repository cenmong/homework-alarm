from period_class import *
from env import *
from alarm_class import *
from meliae import scanner
scanner.dump_all_objects('memory.json')

import logging
logging.info('Program starts!')

# TODO Microscopic analysis (e.g., case studies, update content) should be in another logic

action = {'middle':False, 'final':False, 'plot':False} # Specify what to do
#action = {'middle':True, 'final':False, 'plot':False} # Specify what to do
#action = {'middle':False, 'final':True, 'plot':False}
#action = {'middle':False, 'final':False, 'plot':True}
option = {'mid_granu':10, 'final_granu':20} # fin_gra = mid_gra * N

index_list = [27]

for i in index_list:
    # Note: different applications may require different monitor and prefix sets!
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as() # FIXME still under test

    if action['middle']:
        alarm = Alarm(my_period, option['mid_granu'])
        alarm.analyze_to_middle() # analyze all updates and store all middle output files

    if action['final']:
        '''
        reaper = Reaper(my_period, option['final_granu'])
        reaper.get_XXX
        reaper.get_XXX
        '''

    if action['plot']:
        # TODO Plotting into the same dir as final output dir. No logic in plotting
        final_dir = my_period.get_final_dir()
        '''
        plotter = Plotter(my_period)
        '''

logging.info('Program ends!')
