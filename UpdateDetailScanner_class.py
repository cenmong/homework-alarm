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

        # 0:update 1:A 2:W 
        # 3:updated prefix 4:announced prefix 5:withdrawn prefix
        # 6:WW 7:AADup1 8:AADup2 9:AADiff 10:WAUnknown 11:WADup 12:WADiff 13:AW
        metric_num = 14
        tmetrics = dict() # total metrics

        mon2metrics = dict()
        for m in self.monitors:
            mon2metrics[m] = dict()

        # initialization
        for i in range(metric_num):
            tmetrics[i] = 0
            for key in mon2metrics:
                mon2metrics[key][i] = 0

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
                except:
                    pass

            myf.close()

        # Output the overall and per-monitor statistics
        outpath = self.output_dir() + str(sdt_unix) + '.txt'


    # TODO def analyze_active_pfx(self): 
    # note: use middle files
    # note: monitor id -> ip
