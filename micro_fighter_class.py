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
import ast

from netaddr import *
from env import *
from cStringIO import StringIO

class Micro_fighter():

    def __init__(self, reaper):
        self.reaper = reaper
        self.granu = self.reaper.granu
        self.period = reaper.period
        self.sdt_obj = None
        self.edt_obj = None

        self.filelist = self.period.get_filelist()

        self.mfilegroups = None

        self.middle_dir = self.period.get_middle_dir()
        self.final_dir = self.period.get_final_dir()


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


    def bgp_leak_pfx(self):
        pfx_set = set()
        fname = datadir + 'final_output/bell-leak.txt'
        f = open(fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            pfx = line.split('=')[1]
            pfx_set.add(pfx)
        f.close()

        return pfx_set

    def event_as_link_rank(self, unix_dt):
        as_link_count = dict()
        as_count = dict()

        event_sdt = datetime.datetime.utcfromtimestamp(unix_dt)
        event_unix_sdt = unix_dt
        event_edt = event_sdt + datetime.timedelta(minutes=self.granu)
        event_unix_edt = unix_dt + self.granu * 60

        # get event prefix and monitor set
        pfx_set = set()
        mon_index_set = set()
        event_detail_fname = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_detail_fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            if '#' in line: # monitor line
                mondict = line.split('#')[1]
                mondict = ast.literal_eval(mondict)
                mon_index_set = set(mondict.keys())
            else:
                pfx = line.split(':')[0]
                pfx_set.add(pfx)
        f.close()

        pfx_count = len(pfx_set)
        mon_count = len(mon_index_set)
        event_size = pfx_count * mon_count

        #leak_pfx_set = self.bgp_leak_pfx()
        #common_set = pfx_set & leak_pfx_set
        #common_count = len(common_set)
        #print 'pfx set in event:',pfx_count
        #print 'pfx set in leak',len(leak_pfx_set)
        #print 'common pfx set in leak:',common_count
            
        index2mon = dict()
        mon2index_file = self.period.get_mon2index_file_path()
        f = open(mon2index_file, 'r')
        for line in f:
            line = line.rstrip('\n')
            ip = line.split(':')[0]
            index = int(line.split(':')[1])
            index2mon[index] = ip
        f.close()

        mon_set = set()
        for i in mon_index_set:
            mon_set.add(index2mon[i])

         
        # pfx=>xxxxxx, mon=>xxx, pfx+mon=>xxxxxxxxx, to save memory
        pfx2tag = dict()
        mon2tag = dict()

        start = 100000
        for pfx in pfx_set:
            pfx2tag[pfx] = str(start)
            start += 1

        start = 100
        for mon in mon_set:
            mon2tag[mon] = str(start)
            start += 1


        # obtain the target update file list
        f = open(self.filelist, 'r')
        for fline in f:
            # get date from file name
            updatefile = fline.split('|')[0]

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
            print 'Reading ', fline
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    attr = line.rstrip('\n').split('|')
                    pfx = attr[5]
                    mon = attr[3]

                    if not event_unix_sdt<=int(attr[1])<=event_unix_edt:
                        continue

                    if (pfx not in pfx_set) or (mon not in mon_set):
                        continue

                    # now do something
                    the_tag = pfx2tag[pfx] + mon2tag[mon]

                    as_list = attr[6].split()
                    mylen = len(as_list)
                    for i in xrange(0, mylen-1):
                        as1 = as_list[i]
                        as2 = as_list[i+1]

                        if as1 == as2:
                            continue

                        if int(as1) > int(as2):
                            as_link = as2+'_'+as1
                        else:
                            as_link = as1+'_'+as2

                        try:
                            as_link_count[as_link].add(the_tag)
                        except:
                            as_link_count[as_link] = set()
                            as_link_count[as_link].add(the_tag)

                        try:
                            as_count[as1].add(the_tag)
                        except:
                            as_count[as1] = set()
                            as_count[as1].add(the_tag)

                    try:
                        as_count[as2].add(the_tag)
                    except:
                        as_count[as2] = set()
                        as_count[as2].add(the_tag)


                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)

            myf.close()

        f.close()

        tmp_dict = dict()
        for al in as_link_count:
            tmp_dict[al] = float(len(as_link_count[al])) / event_size

        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+'as_link_rank_'+str(unix_dt)+'.txt', 'w')
        for item in tmp_list:
            as_link = item[0]
            count = item[1]
            f.write(str(as_link)+':'+str(count)+'\n')
        f.close()


        tmp_dict = dict()
        for a in as_count:
            tmp_dict[a] = float(len(as_count[a])) / event_size
        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+'as_rank_'+str(unix_dt)+'.txt', 'w')
        for item in tmp_list:
            asn = item[0]
            count = item[1]
            f.write(str(asn)+':'+str(count)+'\n')
        f.close()

        # read the unix_dt.txt file to get prefix and monitor set (a separate function)
        # read each update file to identify the interested pfx and mon
        # record the data we care
