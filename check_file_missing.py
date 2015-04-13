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

index_list = [3,4]

for i in index_list:
    my_period = Period(i)
    flist = my_period.get_filelist()
    f = open(flist, 'r')
    for line in f:
        line = line.split('|')[0]
        url = line.replace('.txt.gz', '')
        line = datadir + line
        line2 = datadir + url
        '''
        if not os.path.isfile(line):
            print 'parsing ', line.replace('.txt.gz', '')
            cmlib.parse_mrt(line.replace('.txt.gz',''),line)
        else:
            pass
            #os.remove(line)
            #print 'removed ', line
        '''
        try:
            os.remove(line)
            print 'removed ', line
            os.remove(line2)
            print 'removed ', line2
        except:
            pass
        
    f.close()
