import radix # takes 1/4 the time as patricia
import datetime
import numpy as np
import calendar # do not use the time module
import cmlib
import operator
import string
import gzip
import traceback
import logging
import subprocess

from netaddr import *
from env import *
from cStringIO import StringIO

class UpdateDetailScanner():

    def __init__(self, period, granu):
        self.filelist = period.get_filelist()
        self.sdate = period.sdate
        self.edate = period.edate

        self.sdt_obj = period.sdatetime_obj
        self.edt_obj = period.edatetime_obj
        self.granu = granu

        self.uflist = self.period.get_filelist()

        self.monitors = list()
        for co in self.period.co_mo.keys():
            self.monitors.extend(self.period.co_mo[co])
        self.mcount = len(self.monitors) # the number of monitors

        # * Note: I do not stream in all the files and record the statistics for each slot
        # dynamically because I want to eliminate the difficulty of time alignment.

        # Get a list of dt objects (datetime1, datetime2) to specify each slot
        self.dtobj_list = list()
        now = self.sdt_obj
        next = self.sdt_obj + datetime.timedelta(minutes=self.granu)
        while next <= self.edt_obj:
            pair = [now, next]
            self.dtobj_list.append(pair)
            now = next
            next += datetime.timedelta(minutes=self.granu)

    def output_dir(self):
        return  metrics_output_root + str(self.granu) + '/' + self.sdate + '_' + self.edate + '/'

    def analyze_metrics(self):
        for slot in self.dtobj_list:
            print '********************Now processing slot ', slot
            self.get_metrics_for_slot(slot)
        
    def get_metics_for_slot(self, slot):
        sdt_unix = calendar.timegm(slot[0].utctimetuple())
        edt_unix = calendar.timegm(slot[1].utctimetuple())

        # Numerical metrics
        # 0:updates 1:A 2:W    3:WW 4:AADup1 5:AADup2 6:AADiff 7:WAUnknown 8:WADup 9:WADiff 10:AW
        metric_num = 11
        tmetrics = dict() # total metrics

        mon2metrics = dict()
        for m in self.monitors:
            mon2metrics[m] = dict()

        # Updated prefix sets
        # updated prefix set announced prefix set withdrawn prefix set
        # TODO

        # initialization
        for i in range(metric_num):
            tmetrics[i] = 0
            for key in mon2metrics:
                mon2metrics[key][i] = 0

        # tmp variables
        mp_last_A = dict() # mon: prefix: latest announcement full content
        mp_last_type = dict()
        for m in self.monitors:
            mp_last_A[m] = dict() # NOTE: does not record W, only record A
            mp_last_type[m] = dict()

        # obtain and read the update files
        flist = cmlib.select_update_files(self.uflist, slot[0], slot[1])
        for fpath in flist:
            print 'Reading ', fpath
            p = subprocess.Popen(['zcat', fpath],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')

                    unix = int(attr[1])
                    if unix < sdt_unix or unix > edt_unix:
                        continue
                    
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    mon2metrics[mon][0] += 1
                    tmetrics[0] += 1

                    if type == 'A':
                        as_path = attr[6]
                        mon2metrics[mon][1] += 1
                        tmetrics[1] += 1
                    else:
                        mon2metrics[mon][2] += 1
                        tmetrics[2] += 1

                    # Obtain existent information
                    try:
                        last_A = mp_last_A[mon][pfx]
                        last_as_path = last_A.split('|')[6]
                    except:
                        last_A = None
                        last_as_path = None

                    try:
                        last_type = mp_last_type[mon][pfx]
                    except: # this is the first update for the mon-pfx pair
                        last_type = None

                    if last_type == 'W':
                        if type == 'W':
                            mon2metrics[mon][3] += 1
                            tmetrics[3] += 1
                        elif type == 'A':
                            if last_as_path:
                                if as_path == last_as_path:
                                    mon2metrics[mon][8] += 1
                                    tmetrics[8] += 1
                                else:
                                    mon2metrics[mon][9] += 1
                                    tmetrics[9] += 1
                            else:
                                mon2metrics[mon][7] += 1
                                tmetrics[7] += 1
                            mp_last_A[mon][pfx] = line
                    elif last_type == 'A':
                        if type == 'W':
                            mon2metrics[mon][10] += 1
                            tmetrics[10] += 1
                        elif type == 'A':
                            if line == last_A:
                                mon2metrics[mon][4] += 1
                                tmetrics[4] += 1
                            elif as_path == last_as_path:
                                mon2metrics[mon][5] += 1
                                tmetrics[5] += 1
                            else:
                                mon2metrics[mon][6] += 1
                                tmetrics[6] += 1
                            mp_last_A[mon][pfx] = line
                    else: # last_type == None
                        pass
                
                    if type == 'W':
                        mp_last_type[mon][pfx] = 'W'
                    elif type == 'A':
                        mp_last_type[mon][pfx] = 'A'
                        mp_last_A[mon][pfx] = line

                except:
                    pass

            myf.close()

        # Output the overall and per-monitor statistics
        outpath = self.output_dir() + str(sdt_unix) + '.txt'


    # TODO def analyze_active_pfx(self): 
    # note: use middle files
    # note: monitor id -> ip
