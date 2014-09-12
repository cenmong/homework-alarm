import os
import subprocess
import string
import cmlib

from alarm_class import *
from netaddr import *
from env import *

class Analyzer():

    def __init__(self, filelist, granu, sdate, act_threshold, atype, thres,\
                soccur, eoccur, des):
        self.filelist = filelist  # filelist file name 
        self.allowed = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+'\
                '+'{'+'}'+','+'-')

        try:
            self.cl_list = cmlib.get_collector(sdate)  # the collectors this event has
        except:
            self.cl_list = []

        self.cl_first = dict()  # collector first existence, True or False
        for cl in collectors:
            self.cl_first[cl[0]] = True
                
        if atype == 1:
            self.alarm = Alarm(granu, sdate, act_threshold, self.cl_list,\
                    thres, soccur, eoccur, des, None)
        #if atype == 2:  # longitudinal study TODO: useless right now
        #    self.alarm = Alarm_c(granu, self.cl_list)
        #self.atype = atype

    def direct(self):
        try:
            self.alarm.direct_plot()
        except Exception, e:
            print str(e) # print the exact information of the exception
            return False

        print 'Plotting from existent data...'
        return True

    # check whether an update is normal or not
    def is_normal(self, update):
        if set(update).issubset(self.allowed) and len(update.split('|')) > 5:
            return True
        else:
            return False

    def parse_updates(self):
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '').replace('archive.', '')
            print ff

            # unpack the update file
            subprocess.call('gunzip -c '+ff+' > '+ff.replace('txt.gz', 'txt'), shell=True)

            # get collector
            cl = ff.split('/')[5]
            if cl == 'bgpdata':  # route-views2, the special case
                cl = ''

            lastline = 'Do not delete me!'
            with open(ff.replace('txt.gz', 'txt'), 'r') as f:

                # this collector appears for the first time
                if self.cl_first[cl] == True:
                    for line in f:  # get first (ipv4) line
                        line = line.replace('\n', '')
                        if not self.is_normal(line):
                            continue
                        break
                    self.alarm.set_start(cl, line)  # set colllector's starting dt
                    self.alarm.add(line)
                    self.cl_first[cl] = False

                for line in f:
                    line = line.replace('\n', '')
                    if not self.is_normal(line):
                        continue
                    self.alarm.add(line)
                    lastline = line

            f.close()
            # remove the unpacked file to save space
            os.remove(ff.replace('txt.gz', 'txt'))

            try:
                self.alarm.set_now(cl, lastline)  # set collector's current/latest dt
            except:
                pass
            self.alarm.check_memo(False) # not the ending check

        self.alarm.check_memo(True) # the ending check
        filelist.close()

        self.alarm.plot()

        return 0
