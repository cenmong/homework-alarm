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

    def __init__(self, reaper):
        self.reaper = reaper
        self.period = reaper.period
        self.sdt_obj = None
        self.edt_obj = None

        self.filelist = self.period.get_filelist()

        self.mfilegroups = None

        self.middle_dir = self.period.get_middle_dir()
        self.final_dir = period.get_final_dir()


    def set_sedate(self, sdt_obj, edt_obj):
        self.sdt_obj = sdt_obj
        self.edt_obj = edt_obj

    def get_mfile_group_trange(self):
        self.mfilegroups = list() # list of middle file groups

        mfiles = os.listdir(self.middle_dir)
        for f in mfiles:
            if not f.endswith('.gz'):
                mfiles.remove(f)
        mfiles.sort(key=lambda x:int(x.rstrip('.txt.gz')))

        #----------------------------------------------------------------------
        # group middle files according to the desired granularity

        # get granularity of middle files
        self.m_granu = (int(mfiles[1].rstrip('.txt.gz')) - int(mfiles[0].rstrip('.txt.gz'))) / 60
        shift = self.reaper.shift
        shift_file_c = shift / self.m_granu
        mfiles = mfiles[shift_file_c:] # shift the interval

        group_size = self.granu / self.m_granu
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                self.mfilegroups.append(group)
                group = []

        #--------------------------------------------------------
        # delete the files that is not in our concern range
        group_to_delete = []
        for fg in self.mfilegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            dt_obj = datetime.datetime.utcfromtimestamp(unix_dt)
            if dt_obj < self.sdt_obj or dt_obj > self.edt_obj:
                group_to_delete.append(fg)

        for gd in group_to_delete:
            self.mfilegroups.remove(gd)


    def analyze_event_origin(self, unix_dt):
        event_sdt = datetime.datetime.utcfromtimestamp(unix_dt)
        event_unix_sdt = unix_dt
        event_edt = event_sdt + datetime.timedelta(minutes=self.granu)
        event_unix_edt = unix_dt + self.granu * 60

        # TODO get event prefix and monitor set (or dict?)

        # obtain the update list
        f = open(self.filelist, 'r')
        for fline in f:
            # get date from file name
            updatefile = fline.rstrip('\n')

            file_attr = updatefile.split('.')
            fattr_date, fattr_time = file_attr[-5], file_attr[-4]
            fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))
            

            fline = datadir + fline.split('|')[0]

            # get current file's collector name
            attributes = fline.split('/') 
            j = -1
            for a in attributes:
                j += 1
                if a.startswith('data.ris') or a.startswith('archi'):
                    break

            co = fline.split('/')[j + 1]
            if co == 'bgpdata':  # route-views2, the special case
                co = ''

            # Deal with several special time zone problems
            if co == 'route-views.eqix' and fname_dt_obj <= dt_anchor2: # PST time
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
            elif not co.startswith('rrc') and fname_dt_obj <= dt_anchor1:
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

            if co.startswith('rrc'):
                shift = -10
            else:
                shift = -30

            # Check whether the file is a possible target
            if not event_sdt+datetime.timedelta(minutes=shift)<=fname_dt_obj<=event_edt:
                continue


            # read the update file
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    attr = updt.rstrip('\n').split('|')
                    pfx = attr[5]
                    if not event_unix_sdt<=int(attr[1])<=event_unix_edt:

                except Exception, err:
                    if updt != '':
                        logging.info(traceback.format_exc())
                        logging.info(updt)

            myf.close()

        f.close()
        # read the unix_dt.txt file to get prefix and monitor set (a separate function)
        # read each update file to identify the interested pfx and mon
        # record the data we care
