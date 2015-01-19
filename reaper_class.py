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
#from supporter_class import *
from cStringIO import StringIO

class Reaper():

    def __init__(self, period, granu, shift):
        self.period = period
        self.granu = granu
        self.shift = shift

        self.middle_dir = period.get_middle_dir()
        self.final_dir = period.get_final_dir()

        os.listdir(self.middle_dir)
        # TODO sort the file list by time
        # obtain the time granularity of these files
        # group the files into a list of self.granu/middle_granu files (take care when ==1)
        # remove the last one if it is not full
        # sort the new list: its the basic element of analysis
        # Is it necessary? datatime obj : [file1,file2]

    # XXX a slot here looks like [file1,file2]
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

    def ts_hdv(self, tlist):
        return 0

    # high UQ time series
    def ts_huq(self):
        return 0

    # high both time series
    def ts_h2(self):
        return 0

    def ts_hdvo(self):
        return 0

    def ts_huqo(self):
        return 0
