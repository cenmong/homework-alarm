from period_class import *
from env import *
from alarm_class import *
from meliae import scanner
scanner.dump_all_objects('memory.json')

import logging
logging.info('Program starts!')

# TODO Microscopic analysis (e.g., case studies, update content) should be in another logic

for i in [0]:

    # stores: collectors monitors prefixes filelists...
    # XXX different applications may require different monitor and prefix sets!
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.mo_filter_same_as() # FIXME still under test

    # XXX Some applications may use sliding window rather than fixed window
    alarm = Alarm(my_period, 10) # granularity in minutes Note: do not change 10
    alarm.analyze() # analyze all updates and store all middle output files
    # TODO record the middle_dir information in a separate file
    middle_dir = alarm.middle_dir # where the middle files stored

    # FIXME what if we analyze existent middle files
    # TODO Analyze the middle files for tabling, plotting and analysis...
    # TODO enable longer interval like 20 and 60 minutes (combine files)
    '''
    reaper = Reaper(middle_dir)
    reaper.get_XXX
    reaper.get_XXX
    final_dir = reaper.final_dir
    '''
    # TODO Plotting

logging.info('Program ends!')
