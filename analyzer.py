import os
import subprocess
import string
import cmlib

from alarm_class import *
from netaddr import *
from env import *

class Analyzer():

    def __init__(self, filelist, granu, sdate, act_threshold, atype, thres,\
                soccur, eoccur, des):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        self.allowed = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+'\
                '+'{'+'}'+','+'-')

        try:
            self.cl_list = cmlib.get_collector(sdate)  # the collectors this event has
        except:
            self.cl_list = []

        self.cl_first = dict()  # collector: First existence True or False
        for cl in collectors:
            self.cl_first[cl[0]] = True
                
        if atype == 1:
            self.alarm = Alarm(granu, sdate, act_threshold, self.cl_list,\
                    thres, soccur, eoccur, des)
        if atype == 2:  # longitudinal study
            self.alarm = Alarm_c(granu, self.cl_list)
        self.atype = atype
        # TODO: define a new class to get these values
        cmlib.get_pfx2as_file(sdate)

    def direct(self):
        try:
            self.alarm.direct_plot()
        except Exception,e:
            print str(e)
            return False

        print 'plotting from existent data'
        return True

    def is_normal(self, update):
        if set(update).issubset(self.allowed) and len(update.split('|')) > 5:
            return True
        else:
            return False

    def parse_update(self):
        self.alarm.get_monitor()
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '')
            ff = ff.replace('archive.', '')
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
                    self.alarm.set_start(cl, line)  # set colllector's dt
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
