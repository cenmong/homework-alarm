from netaddr import *
from env import *
import os
from datetime import datetime
import time 
from update_class import *
from alarm_class import *

class Analyzer():

    def __init__(self, filelist, granu, ymd):  # granularity in minutes
        self.filelist = filelist  # filelist file name 
        #self.update_count = {}  # {datetime: (4 update count, 6 update count)}
        self.alarm = Alarm(granu, ymd)

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

        self.alarm.plot()  
        filelist.close()
        return 0
