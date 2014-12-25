from period_class import *
from env import *
from alarm_class import *
from meliae import scanner
scanner.dump_all_objects('memory.json')

import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Program starts!')

#-----------------------------------------------------------------
# From now on we deal with each application separately
# Note: different applications may require different monitor and prefix sets!
# Some applications may use sliding window rather than fixed window
# Microscopic analysis (e.g., case studies) should be in other .py

for i in [27]:

    # stores: collectors monitors prefixes filelist outputDir granularity...
    my_period = Period(i)
    # different actions according to specific needs
    my_period.get_global_monitors() # decide my_period.monitors
    
    # filelist = my_period.get_file_list()
    filelist = '/media/usb/update_list/20141130_20141201/_list.txt' # Test

    alarm = Alarm(period, 10) # this class does not care about index. granularity in minutes
    #####alarm = Alarm(granu, i, cl_list)
    # output = alarm.output() # info about all output files

    # Analyze the output files
    # Plot

logging.info('Program ends!')
