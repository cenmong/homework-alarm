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
        self.dv_thre = None
        self.uq_thre = None


        #--------------------------------------------------------------------
        # values for specific tasks
        # TODO check memory pressure. if too hard, scan two or more times. Or only use one radix

        # recore time series of three types of prefixes. datetime: value
        self.hdv_ts = dict()
        self.huq_ts = dict()
        self.h2_ts = dict()

        # overall updates time series of certain prefixes
        self.uq_hdv_ts = dict()
        self.uq_huq_ts = dict()
        self.uq_h2_ts = dict()
        # total updates time series 
        self.uq_ts = dict()

        # overall DV distribution of certain prefixes
        self.dv_hdv_distr = dict() # DV value: existence
        self.dv_huq_distr = dict() # DV value: existence
        self.dv_h2_distr = dict() # DV value: existence

        # Lifetime of 3 types of H prefixes
        self.pfx_lifetime = radix.Radix() # XXX costs memo

        # total DV and UQ distribution
        self.dv_distr_all = dict()
        self.uq_distr_all = dict()
        # DV and UQ distribution for certain period # TODO need INPUT
        self.dv_distr[period1] = dict()
        self.uq_distr[period1] = dict()

        # New HDV and HUQ quantity time series
        self.hset = radix.Radix() # H prefix set for all previous intervals
        self.new_hdv_ts = dict()
        self.new_huq_ts = dict()
        self.p_hset = radix.Radix() # H prefix set in the previous interval 
        self.newp_hdv_ts = dict()
        self.new_huq_ts = dict()

    def set_dv_uq_thre(self, dvt, uqt): # Input a dict of exact format
        self.dv_thre = dvt
        self.uq_thre = uqt

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

            huq_flag = False
            if uq > self.uq_thre: # the prefix is a HUQ prefix
                self.huq_ts[unix_dt] += 1
                self.uq_huq_ts[unix_dt] += uq
                try:
                    self.dv_huq_distr[dv] += 1
                except:
                    self.dv_huq_distr[dv] = 1
                huq_flag = True

                rnode = self.pfx_lifetime.search_exact(pfx)
                if rnode is None:
                    mynode = self.pfx_lifetime.add(pfx)
                    mynode.data['huq_LT'] = 1
                else:
                    rnode.data['huq_LT'] += 1

            if dv > self.dv_thre: # the prefix is a HDV prefix
                self.hdv_ts[unix_dt] += 1
                self.uq_hdv_ts[unix_dt] += uq
                try:
                    self.dv_hdv_distr[dv] += 1
                except:
                    self.dv_hdv_distr[dv] = 1

                rnode = self.pfx_lifetime.search_exact(pfx)
                if rnode is None:
                    mynode = self.pfx_lifetime.add(pfx)
                    mynode.data['hdv_LT'] = 1
                else:
                    rnode.data['hdv_LT'] += 1

                if huq_flag: # the prefix is a H2 prefix
                    self.h2_ts[unix_dt] += 1
                    self.uq_h2_ts[unix_dt] += uq
                    try:
                        self.dv_h2_distr[dv] += 1
                    except:
                        self.dv_h2_distr[dv] = 1

                    rnode = self.pfx_lifetime.search_exact(pfx)
                    try:
                        rnode['h2_LT'] += 1
                    except:
                        rnode['h2_LT'] = 1

            # Total update quantity
            self.uq_ts[unix_dt] += uq

            # Total DV and UQ distribution
            try:
                self.dv_distr_all[dv] += 1
            except:
                self.dv_distr_all[dv] = 1
            try:
                self.uq_distr_all[uq] += 1
            except:
                self.uq_distr_all[uq] = 1

        fin.close()

    # Do many tasks in only one scan of all files!
    def analyze(self):
        for fg in self.filegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            self.hdv_ts[unix_dt] = 0
            self.huq_ts[unix_dt] = 0
            self.h2_ts[unix_dt] = 0

            self.uq_hdv_ts[unix_dt] = 0
            self.uq_huq_ts[unix_dt] = 0
            self.uq_h2_ts[unix_dt] = 0
            self.uq_ts[unix_dt] = 0
            for f in fg:
                self.read_a_file(self.middle_dir+f, dt)
        # TODO output here
        # release memo if necessary
        # be careful when constructing the outpur dir and file name
        # consider DV HQ thre, original and final granu and shift 
        return 0
