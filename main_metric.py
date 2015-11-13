from micro_fighter_class import Micro_fighter
from period_class import Period
from env import *
from alarm_class import Alarm
from reaper_class import Reaper
from multi_reaper_class import MultiReaper
from plotter_class import Plotter
from UpdateDetailScanner_class import UpdateDetailScanner

import cmlib
import os

index_list = [285]

for i in index_list:
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as()

    UDS = UpdateDetailScanner(my_period, 20)
