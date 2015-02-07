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

class Micro_fighter():

    def __init__(self, period, granu, sdt_obj, edt_obj):
        self.period = period
        self.sdt_obj = sdt_obj
        self.edt_obj = edt_obj
        self.granu = granu

        self.filelist = period.get_filelist()

        self.middle_dir = period.get_middle_dir()
        self.final_dir = period.get_final_dir()

        mfiles = os.listdir(self.middle_dir)
        for f in mfiles:
            if not f.endswith('.gz'):
                mfiles.remove(f)
        mfiles.sort(key=lambda x:int(x.rstrip('.txt.gz')))

        #----------------------------------------------------------------------
        # group middle files according to the desired granularity

        # get granularity of middle files
        self.m_granu = (int(mfiles[1].rstrip('.txt.gz')) - int(mfiles[0].rstrip('.txt.gz'))) / 60
        shift_file_c = shift / self.m_granu
        mfiles = mfiles[shift_file_c:] # shift the interval

        group_size = self.granu / self.m_granu
        self.filegroups = list() # list of file groups
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                self.filegroups.append(group)
                group = []

        #--------------------------------------------------------
        # delete the files that is not in our concern range
        group_to_delete = []
        for fg in self.filegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            dt_obj = datetime.datetime.utcfromtimestamp(unix_dt)
            if dt_obj < self.sdt_obj or dt_obj > self.edt_obj:
                group_to_delete.append(fg)

        for gd in group_to_delete:
            self.filegroups.remove(gd)
