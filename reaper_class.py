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

from netaddr import *
from env import *
#from supporter_class import *
from cStringIO import StringIO

class Reaper():

    def __init__(self, period, granu, shift):
        self.period = period
        self.granu = granu
        self.shift = shift

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

        group_size = self.granu / m_granu
        filegroups = list() # list of file groups
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                filegroups.append(group)
                group = []

        # Is it necessary? datatime obj : [file1,file2]

    # get the dv and uq of all prefixes in this slot
    # write into final files? return a all_dv_uq_files list. Jump if files exist.
    def get_all_dv_uq(self, slot):
        return 0

    # XXX Note: we should Read these final files as less as possible to save time
    # get all time seris after one reading
    def get_ts(self, options):
        return 0
    # high DV time series obtained from final files
    # argument: a list of thresholds
