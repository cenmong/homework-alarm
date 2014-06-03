from netaddr import *
from env import *
import os
from datetime import datetime
import time 
from update_class import *
from alarm_class import *
from alarm_c_class import *

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
        for ff in filelist.readlines():
            ff = ff.replace('\n', '')
            print ff
            update_chunk = ''
            with open(hdname + ff, 'r') as f:
                for line in f:
                    if line == '':
                        continue
                    elif line == '\n':
                        if update_chunk == '':  # Game start
                            continue
                        else:
                            updt = Update(update_chunk)
                            if updt.get_protocol() == 4:
                                self.alarm.add(updt)
                            update_chunk = ''
                    else:        
                        update_chunk += line.replace('\n', '').strip() + '@@@'
            f.close()

        if self.atype == 1:
            #self.alarm.plot_50_90()  
            self.alarm.plot_index()
        elif self.atype == 2:
            self.alarm.plot()    

        filelist.close()
        return 0
