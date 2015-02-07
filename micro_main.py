from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from plotter_class import Plotter
from micro_fighter_class import Micro_fighter
from meliae import scanner
scanner.dump_all_objects('memory.json')
from plot_matrix import plot_matrix

import datetime
import cmlib
import os
import logging
logging.info('Program starts!')

#-------------------------------------------------------------------
# Specify the target to analyze
index = 0 # the event to analyze
sdt_obj = datetime.datetime(1989,10,24,0,0) # starting unix datetime
edt_obj = datetime.datetime(1989,10,25,10,0) # ending unix datetime
granu = 20

#-------------------------------------------
# Still, we only care about the peers with global view
my_period = Period(index)
my_period.get_global_monitors() # decide my_period.monitors
my_period.rm_dup_mo() # rm multiple existence of the same monitor
my_period.mo_filter_same_as()

#---------------------------------------------
# TODO can analyze the original updates or middle files and generate reports
micro_fighter = Micro_fighter(my_period, granu, sdt_obj, edt_obj)
micro_fighter.analyze_pfx()
# analyze certain prefixes (optional) define pfx_target = dict() for quick access
# group prefixes into ASes (very meaningful)
