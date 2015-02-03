from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from plotter_class import Plotter
from meliae import scanner
scanner.dump_all_objects('memory.json')
from plot_matrix import plot_matrix

import cmlib
import os
import logging
logging.info('Program starts!')

index = 0 # the event to analyze

# Still, we only care about the peers with global view
my_period = Period(i)
my_period.get_global_monitors() # decide my_period.monitors
my_period.rm_dup_mo() # rm multiple existence of the same monitor
my_period.mo_filter_same_as()

# TODO analyze the original updates
# specify a period
# analyze certain prefixes (optional) define pfx_target = dict() for quick access
# group prefixes into ASes (very meaningful)
