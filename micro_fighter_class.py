import radix # takes 1/4 the time as patricia
import datetime
import numpy as np
import calendar # do not use the time module
from cmlib import *
import operator
import string
import gzip
import traceback
import logging
import subprocess
import ast
import calendar
import os
import urllib

import sklearn
print('The scikit-learn version is {}.'.format(sklearn.__version__))

from sklearn.cluster import DBSCAN

from netaddr import *
from env import *
from cStringIO import StringIO

#from sklearn.datasets.samples_generator import make_blobs
#from sklearn.preprocessing import StandardScaler

class Micro_fighter():

    def __init__(self, reaper):
        self.reaper = reaper
        self.sdate = self.reaper.period.sdate
        self.granu = self.reaper.granu
        self.period = reaper.period
        self.sdt_obj = None
        self.edt_obj = None

        self.updt_filel = self.period.get_filelist()

        self.mfilegroups = None

        self.middle_dir = self.period.get_middle_dir()
        self.final_dir = self.period.get_final_dir()

    def all_events_ratios(self):

        event_dict = self.get_events_list()

        for unix_dt in event_dict:
            rel_size = event_dict[unix_dt][0] # relative size
            width = event_dict[unix_dt][4]
            size = event_dict[unix_dt][1]
            height = event_dict[unix_dt][3] # or prefix number


            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()


            #-----------------------------------
            # read the middle files
            target_fg = None
            for fg in self.reaper.filegroups:
                if int(fg[0].rstrip('.txt.gz')) == unix_dt:
                    target_fg = fg
                    break

            pfx_int_data = dict()
            for fname in target_fg:
                floc = self.reaper.middle_dir + fname
                print 'Reading ', floc
                p = subprocess.Popen(['zcat', floc],stdout=subprocess.PIPE)
                fin = StringIO(p.communicate()[0])
                assert p.returncode == 0
                for line in fin:
                    line = line.rstrip('\n')
                    if line == '':
                        continue

                    pfx = line.split(':')[0]
                    datalist = ast.literal_eval(line.split(':')[1])

                    try:
                        c_list = pfx_int_data[pfx]
                        combined = [x+y for x,y in zip(datalist, c_list)]
                        pfx_int_data[pfx] = combined
                    except:
                        pfx_int_data[pfx] = datalist

            #------------------------------------------
            # get the number of updates and 1s in and out of the event
            udt_num = 0
            for pfx in pfx_int_data:
                the_sum = sum(pfx_int_data[pfx])
                udt_num += the_sum

            udt_in_num = 0
            for pfx in pfx_set:
                for mon_index in mon_set:
                    udt_in_num += pfx_int_data[pfx][mon_index]

            udt_out_num = udt_num - udt_in_num
            print udt_num
            print udt_in_num

            ones_num = 0
            for pfx in pfx_int_data:
                datalist = pfx_int_data[pfx]
                for data in datalist:
                    if data > 0:
                        ones_num += 1

            ones_in_num = 0
            for pfx in pfx_set:
                for mon_index in mon_set:
                    if pfx_int_data[pfx][mon_index] > 0:
                        ones_in_num += 1

            ones_out_num = ones_num - ones_in_num
            print ones_num
            print ones_in_num

            #--------------------------------------------
            # analyze prefixes
            all_pfx_num = self.reaper.period.get_fib_size()
            prefix_ratio = float(height) / float(all_pfx_num)
            print prefix_ratio

            #-------------------------------------------
            # distribution of origin ASes TODO: move to somewhere else
            all_AS_num = self.reaper.period.get_AS_num()

            pfx2as = self.get_pfx2as()
            asn_dict = dict()
            for pfx in pfx_set:
                try:
                    asn = pfx2as[pfx]
                except:
                    asn = -1
                try:
                    asn_dict[asn] += 1
                except:
                    asn_dict[asn] = 1

            #for asn in asn_dict:
            #    asn_dict[asn] = float(asn_dict[asn]) / float(all_AS_num)
            print asn_dict

            #-----------------------------------------
            # TODO: write result to files


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


    def event_update_pattern(self, unix_dt):
        pfx_set = set()
        mon_iset = set()
        mon_set = set()

        event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_fpath, 'r')
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('Mo'):
                mon_iset = ast.literal_eval(line.split('set')[1])
            else:
                pfx_set.add(line.split(':')[0])
        f.close()

        i2ip = dict()
        f = open(self.reaper.period.get_mon2index_file_path(), 'r')
        for line in f:
            line = line.rstrip('\n')
            ip = line.split(':')[0]
            index = int(line.split(':')[1])
            i2ip[index] = ip
        f.close()

        for index in mon_iset:
            mon_set.add(i2ip[index])

        pattern2count = dict()
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

        # get the number of ones within the event
        ones_num = 0
        f = open(self.reaper.events_ratios_path(), 'r')
        for line in f:
            if not line.startswith('ONE'):
                continue
            attr = line.rstrip('\n').split('|')
            unix = int(attr[1])
            if unix == unix_dt:
                ones_num = int(attr[4])
        f.close()
        print 'ones_num=',ones_num

        #--------------------------------------------------------
        # Read update files
        sdt_unix = unix_dt
        edt_unix = unix_dt + self.reaper.granu * 60
        updt_files = list()
        fmy = open(self.updt_filel, 'r')
        for fline in fmy:
            updatefile = fline.split('|')[0]
            updt_files.append(datadir+updatefile)

        num2type = {0:'WW',1:'AADup1',2:'AADup2',3:'AADiff',40:'WAUnknown',\
                    41:'WADup',42:'WADiff',5:'AW',798:'FD',799:'FD(include WADup)',\
                    800:'patho',801:'patho(include WADup)',802:'policy'}
        for n in num2type:
            pattern2count[n] = set()
        #WW:0,AAdu1:1,AAdu2:2,AAdiff:3,WA:4(WADup:41,WADiff:42,WAUnknown:40),AW:5
        mp_dict = dict() # mon: prefix: successive update type series (0~5)
        mp_last_A = dict() # mon: prefix: latest full update
        mp_last_type = dict()
        for m in mon_set:
            mp_dict[m] = dict()
            mp_last_A[m] = dict() # NOTE: does not record W, only record A
            mp_last_type[m] = dict()

        fpathlist = select_update_files(updt_files, sdt_unix, edt_unix)
        for fpath in fpathlist:
            print 'Reading ', fpath
            p = subprocess.Popen(['zcat', fpath],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    if (mon not in mon_set) or (pfx not in pfx_set):
                        continue

                    unix = int(attr[1])
                    if unix < sdt_unix or unix > edt_unix:
                        continue

                    if type == 'A':
                        as_path = attr[6]

                    the_tag = pfx2tag[pfx] + mon2tag[mon]

                    try:
                        test = mp_dict[mon][pfx]
                    except:
                        mp_dict[mon][pfx] = list() # list of 0~5

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
                            mp_dict[mon][pfx].append(0)
                            pattern2count[0].add(the_tag)
                            pattern2count[800].add(the_tag)
                            pattern2count[801].add(the_tag)
                        elif type == 'A':
                            if last_as_path:
                                if as_path == last_as_path:
                                    mp_dict[mon][pfx].append(41)
                                    pattern2count[41].add(the_tag)
                                    pattern2count[799].add(the_tag)
                                    pattern2count[801].add(the_tag)
                                else:
                                    mp_dict[mon][pfx].append(42)
                                    pattern2count[42].add(the_tag)
                                    pattern2count[799].add(the_tag)
                                    pattern2count[798].add(the_tag)
                            else: # no A record
                                mp_dict[mon][pfx].append(40)
                                pattern2count[40].add(the_tag)
                            mp_last_A[mon][pfx] = line
                
                    elif last_type == 'A':
                        if type == 'W':
                            mp_dict[mon][pfx].append(5)
                            pattern2count[5].add(the_tag)
                        elif type == 'A':
                            if line == last_A:
                                mp_dict[mon][pfx].append(1)
                                pattern2count[1].add(the_tag)
                                pattern2count[800].add(the_tag)
                                pattern2count[801].add(the_tag)
                            elif as_path == last_as_path:
                                mp_dict[mon][pfx].append(2)
                                pattern2count[2].add(the_tag)
                                pattern2count[802].add(the_tag)
                            else:
                                mp_dict[mon][pfx].append(3)
                                pattern2count[3].add(the_tag)
                                pattern2count[799].add(the_tag)
                                pattern2count[798].add(the_tag)
                            mp_last_A[mon][pfx] = line
                
                    else: # last_type is None
                        if type == 'W':
                            mp_last_type[mon][pfx] = 'W'
                        elif type == 'A':
                            mp_last_type[mon][pfx] = 'A'
                            mp_last_A[mon][pfx] = line
                        else:
                            assert False
                        
                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)
            myf.close()


        type2num = dict()
        type2ratio = dict()
        total = 0
        for mon in mp_dict:
            for pfx in mp_dict[mon]:
                for t in mp_dict[mon][pfx]:
                    name = num2type[t]
                    total += 1
                    try:
                        type2num[name] += 1
                    except:
                        type2num[name] = 1
        
        for t in type2num:
            type2ratio[t] = float(type2num[t]) / float(total)

        print type2num
        print type2ratio
        sorted_list = sorted(type2num.items(), key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+str(unix_dt)+'_updt_pattern.txt', 'w')
        for item in sorted_list:
            tp = item[0]
            count = item[1]
            ratio = type2ratio[tp]
            f.write(str(tp)+':'+str(count)+' '+str(ratio)+'\n')
        f.close()

        p2ratio = dict()
        for p in pattern2count:
            p2ratio[num2type[p]] = float(len(pattern2count[p])) / float(ones_num)
        sorted_list = sorted(p2ratio.items(), key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+str(unix_dt)+'_updt_pattern_in_ones.txt', 'w')
        for item in sorted_list:
            p = item[0]
            ratio = item[1]
            f.write(str(p)+':'+str(ratio)+'\n')
        f.close()

    def oriAS_in_updt(self, unix_dt):
        pfx_set = set()
        mon_iset = set()
        mon_set = set()

        event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_fpath, 'r')
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('Mo'):
                mon_iset = ast.literal_eval(line.split('set')[1])
            else:
                pfx_set.add(line.split(':')[0])
        f.close()

        i2ip = dict()
        f = open(self.reaper.period.get_mon2index_file_path(), 'r')
        for line in f:
            line = line.rstrip('\n')
            ip = line.split(':')[0]
            index = int(line.split(':')[1])
            i2ip[index] = ip
        f.close()

        for index in mon_iset:
            mon_set.add(i2ip[index])

        #--------------------------------------------------------
        # Read update files
        sdt_unix = unix_dt
        edt_unix = unix_dt + self.reaper.granu * 60
        updt_files = list()
        fmy = open(self.updt_filel, 'r')
        for fline in fmy:
            updatefile = fline.split('|')[0]
            updt_files.append(datadir+updatefile)

        # XXX Note: 
        # (1) we record the last existence if multiple A exist
        # (2) we record when only W exist
        # (3) we record when inconsistency exists between monitors
        pfx2oriAS = dict()
        for pfx in pfx_set:
            pfx2oriAS[pfx] = -10

        fpathlist = select_update_files(updt_files, sdt_unix, edt_unix)
        for fpath in fpathlist:
            print 'Reading ', fpath
            p = subprocess.Popen(['zcat', fpath],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    if (mon not in mon_set) or (pfx not in pfx_set):
                        continue

                    unix = int(attr[1])
                    if unix < sdt_unix or unix > edt_unix:
                        continue

                    if type == 'A':
                        as_path = attr[6]

                    oriAS = int(as_path.split()[-1])
                    '''
                    # seems not necessary. The origin AS is the same
                    existed = pfx2oriAS[pfx]
                    if existed != -10:
                        assert existed == oriAS
                    else:
                        pfx2oriAS[pfx] = oriAS
                    ''' 
                    pfx2oriAS[pfx] = oriAS
                        
                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)
            myf.close()

        AS2pfx = dict()
        for pfx in pfx2oriAS:
            ASN = pfx2oriAS[pfx]
            try:
                AS2pfx[ASN] += 1
            except:
                AS2pfx[ASN] = 1

        sorted_list = sorted(AS2pfx.items(), key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+str(unix_dt)+'_pfx_oriAS.txt', 'w')
        for item in sorted_list:
            ASN = item[0]
            count = item[1]
            f.write(str(ASN)+':'+str(count)+'\n')
        f.close()


    def top_AS_ASlink(self, unix_dt):
        pfx_set = set()
        mon_iset = set()
        mon_set = set()

        event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_fpath, 'r')
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('Mo'):
                mon_iset = ast.literal_eval(line.split('set')[1])
            else:
                pfx_set.add(line.split(':')[0])
        f.close()

        i2ip = dict()
        f = open(self.reaper.period.get_mon2index_file_path(), 'r')
        for line in f:
            line = line.rstrip('\n')
            ip = line.split(':')[0]
            index = int(line.split(':')[1])
            i2ip[index] = ip
        f.close()

        for index in mon_iset:
            mon_set.add(i2ip[index])

        # get the number of ones within the event
        ones_num = 0
        f = open(self.reaper.events_ratios_path(), 'r')
        for line in f:
            if not line.startswith('ONE'):
                continue
            attr = line.rstrip('\n').split('|')
            unix = int(attr[1])
            if unix == unix_dt:
                ones_num = int(attr[4])
        f.close()
        print 'ones_num=',ones_num

        #--------------------------------------------------------
        # Read update files
        sdt_unix = unix_dt
        edt_unix = unix_dt + self.reaper.granu * 60
        updt_files = list()
        fmy = open(self.updt_filel, 'r')
        for fline in fmy:
            updatefile = fline.split('|')[0]
            updt_files.append(datadir+updatefile)


        as_count = dict()
        as_link_count = dict()
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


        fpathlist = select_update_files(updt_files, sdt_unix, edt_unix)
        for fpath in fpathlist:
            print 'Reading ', fpath
            p = subprocess.Popen(['zcat', fpath],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    if (mon not in mon_set) or (pfx not in pfx_set):
                        continue

                    unix = int(attr[1])
                    if unix < sdt_unix or unix > edt_unix:
                        continue

                    if type == 'A':
                        as_path = attr[6]

                    # now do something
                    the_tag = pfx2tag[pfx] + mon2tag[mon]

                    as_list = as_path.split()
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
                            as_link_count[as_link] = set([the_tag])

                        try:
                            as_count[as1].add(the_tag)
                        except:
                            as_count[as1] = set([the_tag])

                    try:
                        as_count[as2].add(the_tag)
                    except:
                        as_count[as2] = set([the_tag])

                        
                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)
            myf.close()


        tmp_dict = dict()
        for al in as_link_count:
            tmp_dict[al] = float(len(as_link_count[al])) / ones_num

        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+str(unix_dt)+'_topASlink.txt', 'w')
        for item in tmp_list:
            as_link = item[0]
            count = item[1]
            f.write(str(as_link)+':'+str(count)+'\n')
        f.close()


        tmp_dict = dict()
        for a in as_count:
            tmp_dict[a] = float(len(as_count[a])) / ones_num
        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+str(unix_dt)+'_topAS.txt', 'w')
        for item in tmp_list:
            asn = item[0]
            count = item[1]
            f.write(str(asn)+':'+str(count)+'\n')
        f.close()


    def analyze_pfx_indate(self, ASes, sdt_obj, edt_obj):
        fmy = open(self.updt_filel, 'r')
        sdt_unix = calendar.timegm(sdt_obj.utctimetuple())
        edt_unix = calendar.timegm(edt_obj.utctimetuple())
        print sdt_unix, edt_unix

        #pfx_set = set() # 2156 prefixes in total

        target_mon = (['195.66.224.138', '89.149.178.10'])
        target_pfx = set()
        f = open('target_pfx.txt','r')
        for line in f:
            line = line.rstrip('\n')
            target_pfx.add(line)
        f.close()

        #WW:0,AAdu1:1,AAdu2:2,AAdiff:3,WA:4,AW:5
        target_dict = dict() # mon: prefix: successive update type series (0~5)
        target_record = dict() # mon: prefix: latest full update
        for m in target_mon:
            target_dict[m] = dict()
            target_record[m] = dict()

        for fline in fmy:
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
            if not sdt_obj+datetime.timedelta(minutes=shift)<=fname_dt_obj<=edt_obj:
                continue

            # read the update file
            print 'Reading ', fline
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    #if type == 'W':
                    #    continue
                    if mon not in target_mon:
                        continue
                        
                    if pfx not in target_pfx:
                        continue

                    if type == 'A':
                        as_path = attr[6]
                        #as_list = as_path.split()
                        #mylen = len(as_list)
                        #for i in xrange(0, mylen-1):
                        #    as1 = int(as_list[i])
                        #    as2 = int(as_list[i+1])

                        #    if as1 == as2:
                        #        continue
                        #    
                        #    if as1 in ASes and as2 in ASes and i == mylen-2: # last hop
                        #        analyze = True
                        #        break

                    #pfx_set.add(pfx)
                    try:
                        test = target_dict[mon][pfx]
                    except:
                        target_dict[mon][pfx] = list() # list of 0~5

                    try:
                        last_update = target_record[mon][pfx]
                        last_attr = last_update.split('|')
                        last_type = last_attr[2]
                        if last_type is 'A':
                            last_as_path = last_attr[6]
                    except:
                        last_type = 'W'
                        last_as_path = 'Nothing'

                    if last_type is 'W':
                        if type is 'W':
                            print 'WW'
                            target_dict[mon][pfx].append(0)
                        elif type is 'A':
                            print 'WA'
                            target_dict[mon][pfx].append(4)
                            target_record[mon][pfx] = line
                
                    elif last_type is 'A':
                        if type is 'W':
                            print 'AW'
                            target_dict[mon][pfx].append(5)
                            target_record[mon][pfx] = 'Nothing'
                        elif type is 'A':
                            if line == last_update:
                                print 'AAdu1'
                                target_dict[mon][pfx].append(1)
                            elif as_path == last_as_path:
                                print 'AAdu2'
                                target_dict[mon][pfx].append(2)
                                target_record[mon][pfx] = line
                            else:
                                print 'AAdiff'
                                target_dict[mon][pfx].append(3)
                                target_record[mon][pfx] = line
                
                    else: # abnormal
                        continue
                        
                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)
                print len(target_dict['195.66.224.138'])
            myf.close()
            '''
            f3 = open('target_pfx.txt', 'w')
            for pfx in pfx_set:
                f3.write(pfx+'\n')
            f3.close()
            '''
        for mon in target_dict:
            print len(target_dict[mon])
            ff = open(mon+'result.txt','w')
            for pfx in target_dict[mon]:
                ff.write(pfx+':'+str(target_dict[mon][pfx])+'\n')
            ff.close()

        f.close()

    def event_analyze_pfx(self, unix_dt):

        pfx2as = self.get_pfx2as()
        as2count = dict()
        odd_pfx = set()

        event_detail_fname = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_detail_fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            if '#' in line: # monitor line
                pass
            else:
                pfx = line.split(':')[0]
                try:
                    asn = pfx2as[pfx]
                except:
                    asn = -1
                    odd_pfx.add(pfx)
                try:
                    as2count[asn] += 1
                except:
                    as2count[asn] = 1
        f.close()

        tmp_list = sorted(as2count.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f2 = open('as_result.txt','w')
        for item in tmp_list:
            asn = item[0]
            count = item[1]
            f2.write(str(asn)+':'+str(count)+'\n')
        f2.close()

        f = open('odd_pfx.txt', 'w')
        for pfx in odd_pfx:
            f.write(pfx+'\n')
        f.close()
        

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
        f = open(self.updt_filel, 'r')
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
