import os
import time 
import subprocess
import sys
import string

from alarm_class import *
from datetime import datetime
from netaddr import *
from env import *

class Analyzer():

    def __init__(self, filelist, granu, sdate, act_threshold, atype):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        self.allowed = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+'\
                '+'{'+'}'+','+'-')

        self.cl_list = []  # the collectors this analyzer has
        dir_list = os.listdir('metadata/'+sdate+'/')
        for f in dir_list:
            if not 'filelist' in f:
                continue
            if 'test' in f:
                continue
            
            cl = f.split('_')[-1]
            if cl == 'comb':
                continue
            self.cl_list.append(cl)

        print 'cl_list:',str(self.cl_list)

        self.cl_first = dict()  # cl: True or False
        for cl in collectors:
            self.cl_first[cl[0]] = True
                
        if atype == 1:
            self.alarm = Alarm(granu, sdate, act_threshold, self.cl_list)
        if atype == 2:  # longitudinal
            self.alarm = Alarm_c(granu, self.cl_list)
        self.atype = atype

    def is_normal(self, update):
        if set(update).issubset(self.allowed) and len(update.split('|')) > 5:
            return True
        else:
            return False

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

            lastline = 'Do not delete me!'
            with open(ff.replace('txt.gz', 'txt'), 'r') as f:
                if self.cl_first[cl] == True:  # this collector first appears
                    for line in f:  # get first (ipv4) line
                        line = line.replace('\n', '')
                        if not self.is_normal(line):
                            continue
                        break
                    self.alarm.set_first(cl, line)  # set colllector's dt
                    self.alarm.add(line)
                    self.cl_first[cl] = False
                for line in f:
                    line = line.replace('\n', '')
                    if not self.is_normal(line):
                        continue
                    self.alarm.add(line)
                    lastline = line
            f.close()
            os.remove(ff.replace('txt.gz', 'txt'))

            try:
                self.alarm.set_now(cl, lastline)  # set collector's dt
            except:
                pass
            self.alarm.check_memo(False)

        self.alarm.check_memo(True)
        filelist.close()

        self.alarm.plot()

        return 0
