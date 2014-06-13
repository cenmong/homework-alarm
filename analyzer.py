from netaddr import *
from env import *
import os
from datetime import datetime
import time 
from alarm_class import *
from alarm_c_class import *
import subprocess

class Analyzer():

    def __init__(self, filelist, granu, sdate, atype):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        if atype == 1:
            self.alarm = Alarm(granu, sdate)
        if atype == 2:
            self.alarm = Alarm_c(granu)
        
        self.atype = atype

    def parse_update(self):
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '')
            subprocess.call('gunzip -c '+ff+' >\
                    '+ff.replace('txt.gz', 'txt'), shell=True)
            #print ff

            # get collector
            cl = ff.split('/')[5]
            if cl = 'bgpdata':  # route-views2
                cl = ''

            with open(ff.replace('txt.gz', 'txt'), 'r') as f:
                line = f.readline().replace('\n', '')
                if '.' in line.split('|')[3]:  # ipv4
                    #self.alarm.add(line)
                    self.alarm.set_from(line)
                for line in f:
                    if line == first_line:
                        print 'Happy!'
                    line = line.replace('\n', '')
                    if ':' in line.split('|')[3]:  # ipv6
                        continue
                    #self.alarm.add(line)
            f.close()
            os.remove(ff.replace('txt.gz', 'txt'))

            self.alarm.set_to(cl, line)

        filelist.close()
        '''
        if self.atype == 1:
            #self.alarm.plot_50_90()  
            self.alarm.plot_index()
        elif self.atype == 2:
            self.alarm.get_avg_med()
            self.alarm.plot()    
        '''
        return 0
