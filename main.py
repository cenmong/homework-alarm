from period_class import *
from env import *
from alarm_class import *
from meliae import scanner
scanner.dump_all_objects('memory.json')

import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Program starts!')

#-----------------------------------------------------------------
# Note: different applications may require different monitor and prefix sets!
# Some applications may use sliding window rather than fixed window
# Microscopic analysis (e.g., case studies) should be in other .py

for i in [27]:

    # stores: collectors monitors prefixes filelist outputDir granularity...
    # does not care about output dir
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    
    # filelist = my_period.get_file_list()
    filelist = '/media/usb/update_list/20141130_20141201/_list.txt' # Test

    alarm = Alarm(period, 10) # this class does not care about index. granularity in minutes
    alarm.analyze()

    # TODO about the location and content of all output files
    info_file = alarm.get_output_info()

    # TODO Analyze the output files

    # TODO Plot

logging.info('Program ends!')
