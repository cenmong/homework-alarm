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
import os
import ast

from cStringIO import StringIO
from netaddr import *
from env import *
#from supporter_class import *
from cStringIO import StringIO

class Reaper():

    def __init__(self, period, granu, shift):
        self.period = period
        self.mo_number = float(self.period.get_mo_number())

        self.middle_dir = period.get_middle_dir()
        self.final_dir = period.get_final_dir()

        mfiles = os.listdir(self.middle_dir)
        for f in mfiles:
            if not f.endswith('.gz'):
                mfiles.remove(f)
        mfiles.sort(key=lambda x:int(x.rstrip('.txt.gz')))

        # get granularity of middle files
        m_granu = (int(mfiles[1].rstrip('.txt.gz')) - int(mfiles[0].rstrip('.txt.gz'))) / 60
        shift_file_c = shift / m_granu
        mfiles = mfiles[shift_file_c:] # shift the interval

        self.granu = granu
        group_size = self.granu / m_granu
        self.filegroups = list() # list of file groups
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                self.filegroups.append(group)
                group = []

        # DV and UQ threshold (set by a self function)
        self.dv_uq_thre = dict()


        #--------------------------------------------------------------------
        # values for specific tasks
        # TODO check memory pressure. if too hard, scan two or more times.

        # recore time series of three types of prefixes. datetime: value
        self.hdv_ts = dict()
        self.huq_ts = dict()
        self.h2_ts = dict()

        # overall updates TS of certain prefixes
        self.uq_hdv_ts = dict()
        self.uq_huq_ts = dict()
        self.uq_h2_ts = dict()

        # overall DV distribution of certain prefixes
        self.dv_hdv_distr = dict() # DV value: existence
        self.dv_huq_distr = dict() # DV value: existence
        self.dv_h2_distr = dict() # DV value: existence

        # DV and uQ distribution for certain period # TODO need INPUT
        self.dv_distr[period1] = dict()
        self.uq_distr[period1] = dict()

        # New HDV and HUQ quantity time series
        self.p_hset = radix.Radix() # previous interval H prefix set
        self.p10_hset = radix.Radix() # previous 10 interval H prefix set

        self.new_hdv_ts = dict()
        self.new_huq_ts = dict()

        # Lifetime of H prefixes
        self.pfx_lifetime = radix.Radix() # XXX certain value plus 1

    def set_dv_uq_thre(self, input): # Input a dict of exact format
        self.dv_uq_thre = input

    def read_a_file(self, floc, unix_dt):
        print 'Reading ', floc
        p = subprocess.Popen(['zcat', self.middle_dir+f],stdout=subprocess.PIPE)
        fin = StringIO(p.communicate()[0])
        assert p.returncode == 0
        for line in fin:
            line = line.rstrip('\n')
            if line == '':
                continue

            pfx = line.split(':')[0]
            data = ast.literal_eval(line.split(':')[1])

            count = 0
            uq = 0 # update quantity
            for d in data:
                if d > 0:
                    count += 1
                    uq += d
            dv = count/self.mo_number # dynamic visibility

        fin.close()

    # Do many tasks in only one scan of all files!
    def analyze(self):
        for fg in self.filegroups:
            dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            for f in fg:
                self.read_a_file(self.middle_dir+f, dt)
        # TODO output here
        # be careful when constructing the outpur dir and file name
        # consider DV HQ thre, original and final granu and shift 
        return 0
