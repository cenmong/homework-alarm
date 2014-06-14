import os
import time 
import subprocess
import sys

from alarm_class import *
from alarm_c_class import *
from getfile import collectors
from datetime import datetime
from netaddr import *
from env import *

class Analyzer():

    def __init__(self, filelist, granu, sdate, atype):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        if atype == 1:
            self.alarm = Alarm(granu, sdate)
        if atype == 2:  # longitudinal
            self.alarm = Alarm_c(granu)
        
        self.atype = atype
        self.cl_first = dict()  # cl: True or False
        for cl in collectors:
            self.cl_first[cl[0]] = True

    def parse_update(self):
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '')
            print ff
            subprocess.call('gunzip -c '+ff+' >\
                    '+ff.replace('txt.gz', 'txt'), shell=True)

            # get collector
            cl = ff.split('/')[5]
            if cl == 'bgpdata':  # route-views2
                cl = ''

            with open(ff.replace('txt.gz', 'txt'), 'r') as f:
                if self.cl_first[cl] == True:  # this collector first appears
                    for line in f:  # get first (ipv4) line
                        line = line.replace('\n', '')
                        try:
                            if ':' in line.split('|')[3]:
                                continue
                        except:
                            continue
                        break
                    self.alarm.set_first(cl, line)  # set colllector's dt
                    self.alarm.add(line)
                    self.cl_first[cl] = False
                for line in f:
                    line = line.replace('\n', '')
                    try:
                        if ':' in line.split('|')[3]:  # ipv6
                            continue
                    except:
                        continue  # met messy codes
                    self.alarm.add(line)
            f.close()
            os.remove(ff.replace('txt.gz', 'txt'))

            self.alarm.set_now(cl, line)  # set collector's dt
            self.alarm.check_memo(False)

        self.alarm.check_memo(True)
        filelist.close()

        if self.atype == 1:
            self.alarm.plot_index()
        elif self.atype == 2:
            self.alarm.get_avg_med()
            self.alarm.plot()    
        return 0
