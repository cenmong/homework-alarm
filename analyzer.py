from netaddr import *
from env import *
import os
from datetime import datetime
import time 
from update_class import *
from alarm_class import *
from alarm_c_class import *
import subprocess

class Analyzer():

    def __init__(self, filelist, granu, ymd, atype):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        #self.update_count = {}  # {datetime: (4 update count, 6 update count)}
        if atype == 1:
            self.alarm = Alarm(granu, ymd)
        if atype == 2:
            self.alarm = Alarm_c(granu)
        
        self.atype = atype

    def parse_update(self):
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '')
            subprocess.call('gunzip -c '+ff+' >\
                    '+ff.replace('txt.gz', 'txt'), shell=True)
            print ff
            with open(ff.replace('txt.gz', 'txt'), 'r') as f:
                for line in f:
                    line = line.replace('\n', '')
                    updt_obj = Update(line)
                    if updt_obj.protocol == 4:
                        print line
            f.close()
            os.remove(ff.replace('txt.gz', 'txt'))

        '''
        if self.atype == 1:
            #self.alarm.plot_50_90()  
            self.alarm.plot_index()
        elif self.atype == 2:
            self.alarm.get_avg_med()
            self.alarm.plot()    
        '''
        filelist.close()
        return 0
