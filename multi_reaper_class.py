import collections
import random
import radix # takes 1/4 the time as patricia
import numpy as np
import cmlib
import operator
import logging
import subprocess
import os
import ast
import traceback
from sklearn.cluster import DBSCAN
from env import *

from cStringIO import StringIO

class MultiReaper():

    def __init__(self, reaper_list):
        self.rlist = reaper_list
        self.pfx_root = datadir + 'final_output_pfx/' # TODO change to granularity


    def AS_exist_in_ASpath_in_updt(self, dt_list, the_asn, target_pfx):
        pfx2path_num = dict()
        pfx2path_exist = dict()
        for pfx in target_pfx:
            pfx2path_num[pfx] = 0
            pfx2path_exist[pfx] = 0

        unix2event, unix2reaper = self.get_dt2event_dt2reaper()
        for unix_dt in unix2event:
            if unix_dt not in dt_list:
                continue
            reaper = unix2reaper[unix_dt]

            mon_set = set()
            event_fpath = reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                    mon_set = set(mon_set)
            f.close()

            mon_set_ip = set()
            i2ip = dict()
            f = open(reaper.period.get_mon2index_file_path(), 'r')
            for line in f:
                line = line.rstrip('\n')
                ip = line.split(':')[0]
                index = int(line.split(':')[1])
                i2ip[index] = ip
            f.close()

            for index in mon_set:
                mon_set_ip.add(i2ip[index])

            #---------------------------
            # read file
            mon_set = mon_set_ip
            pfx_set = target_pfx
            sdt_unix = unix_dt
            edt_unix = unix_dt + reaper.granu * 60
            updt_files = list()
            fmy = open(reaper.period.get_filelist(), 'r')
            for fline in fmy:
                updatefile = fline.split('|')[0]
                updt_files.append(datadir+updatefile)

            fpathlist = cmlib.select_update_files(updt_files, sdt_unix, edt_unix)
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
                            pfx2path_num[pfx] += 1

                            asn_list = as_path.split()
                            for asn in asn_list:
                                try:
                                    asn = int(asn)
                                    if asn == the_asn:
                                        pfx2path_exist[pfx] += 1
                                        break
                                except:
                                    pass

                            
                    except Exception, err:
                        if line != '':
                            logging.info(traceback.format_exc())
                            logging.info(line)
                myf.close()

        f = open(datadir+'final_output/9121-in-path.txt', 'w')
        for pfx in pfx2path_num:
            f.write(pfx+':'+str(pfx2path_num[pfx])+'|'+str(pfx2path_exist[pfx])+'\n')
        f.close()


    def get_common_pfx_set(self, dt_list):
        pfxset_list = list()
        unix2event, unix2reaper = self.get_dt2event_dt2reaper()
        for unix_dt in unix2event:
            if unix_dt not in dt_list:
                continue
            reaper = unix2reaper[unix_dt]

            pfx_set = set()
            mon_set = set()

            event_fpath = reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                    mon_set = set(mon_set)
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()

            pfxset_list.append(pfx_set)
            print len(pfx_set)

        comset = set.intersection(*pfxset_list)
        print 'pfx num:', len(comset)


        f = open(cmlib.datadir+'final_output/target_pfx.txt', 'w')
        for pfx in comset:
            f.write(pfx+'\n')
        f.close()

    def get_dt2event_dt2reaper(self):
        event_dict = dict()
        unix2reaper = dict()
        for reaper in self.rlist:
            path = reaper.get_output_dir_event() + reaper.events_brief_fname
            f = open(path, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix_dt = int(line.split(':')[0])
                content = line.split(':')[1]
                thelist = ast.literal_eval(content)
                rsize = thelist[0]
                if rsize < global_rsize_threshold:
                    continue
                event_dict[unix_dt] = thelist
                unix2reaper[unix_dt] = reaper
            f.close()

        return (event_dict, unix2reaper)

    def all_events_cluster(self):
        pfx_set_dict = dict()
        mon_set_dict = dict()

        event_dict, unix2reaper = self.get_dt2event_dt2reaper()
        for unix_dt in event_dict:
            reaper = unix2reaper[unix_dt]
            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                    mon_set = set(mon_set)
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()

            pfx_set_dict[unix_dt] = pfx_set
            mon_set_dict[unix_dt] = mon_set

        # obtain the jaccard distance between these events
        d_matrix = list()
        unix_dt_list = sorted(event_dict.keys()) # sorted list

        for unix_dt in unix_dt_list:
            the_list = list()
            for unix_dt2 in unix_dt_list:
                pset1 = pfx_set_dict[unix_dt]
                pset2 = pfx_set_dict[unix_dt2]
                JD_p = 1 - float(len(pset1&pset2)) / float(len(pset1|pset2)) # jaccard distance

                mset1 = mon_set_dict[unix_dt]
                mset2 = mon_set_dict[unix_dt2]
                JD_m = 1 - float(len(mset1&mset2)) / float(len(mset1|mset2))

                #JD = 0.9 * JD_p + 0.1 * JD_m # XXX note the parameters
                JD = JD_p

                #the_list.append(JD_p)
                the_list.append(JD)

            d_matrix.append(the_list)


        the_ndarray = np.array(d_matrix)

        print unix_dt_list
        print d_matrix
        db = DBSCAN(eps=0.65, min_samples=4, metric='precomputed').fit(the_ndarray)
        print db.core_sample_indices_
        print db.components_
        print db.labels_

        cluster2dt = dict()
        assert len(unix_dt_list) == len(db.labels_)
        num = len(unix_dt_list)
        outpath = self.events_cluster_path()
        f = open(outpath, 'w')
        '''
        for i in xrange(0, num):
            f.write(str(unix_dt_list[i])+':'+str(db.labels_[i])+'\n')
            try:
                cluster2dt[db.labels_[i]].append(unix_dt_list[i])
            except:
                cluster2dt[db.labels_[i]] = [unix_dt_list[i]]
        f.write('##################\n')
        for c in cluster2dt:
            f.write(str(c)+'|'+str(len(cluster2dt[c]))+':'+str(cluster2dt[c])+'\n')
        '''
        for i in db.labels_:
            f.write(str(i)+'|')
        f.close()

    def events_cluster_path(self):
        return  cmlib.datadir+'final_output/clustering.txt'

    def random_slots_upattern(self, num):
        unix_set = set()
        delete_unix_set = set()
        unix2reaper = dict()
        for reaper in self.rlist:
            fpath = reaper.get_output_dir_event() + 'all_slot_size.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix = line.split(':')[0]
                unix2reaper[unix] = reaper
                unix_set.add(unix)
            f.close()

            fpath = reaper.get_output_dir_event() + 'events_plusminus.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix = line.split(':')[0]
                delete_unix_set.add(unix)
            f.close()

        goal_unix_set = unix_set - delete_unix_set
        print random.sample(goal_unix_set, 18)

        for unix in goal_unix_set:
            reaper = unix2reaper[unix]
            sdt_unix = unix_dt
            edt_unix = unix_dt + reaper.granu * 60

            updt_files = list()
            updt_filel = reaper.period.get_filelist()

            fmy = open(updt_filel, 'r')
            for fline in fmy:
                updatefile = fline.split('|')[0]
                updt_files.append(datadir+updatefile)
            fmy.close()
        
            num2type = {0:'WW',1:'AADup1',2:'AADup2',3:'AADiff',40:'WAUnknown',\
                        41:'WADup',42:'WADiff',5:'AW',798:'FD',799:'FD(include WADup)',\
                        800:'patho',801:'patho(include WADup)',802:'policy'}
            for n in num2type:
                pattern2count[n] = set()

            mp_dict = dict() # mon: prefix: successive update type series (0~5)
            mp_last_A = dict() # mon: prefix: latest full update
            mp_last_type = dict()

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

    def new_huqp_huvp(self):
        # all h prefixes that have existed
        all_huvp = set()
        all_huqp = set()
        all_hap = set()

        # the h prefixes in the previous new_n slots
        new_n = 10
        nhuvp = collections.deque()
        nhuqp = collections.deque()
        nhap = collections.deque()

        count = 0
        for reaper in self.rlist:
            mydir = reaper.get_output_dir_pfx()
            outpath = mydir+'new_huvp_'+str(reaper.Tv)+'_huqp_'+str(reaper.Tq)+'_TS.txt'
            fo = open(outpath, 'w')

            for fg in reaper.filegroups:
                count += 1
                print '******************Round ', count
                unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group

                # sets for this slot only
                huvp_set = set()
                huqp_set = set()
                hap_set = set()

                fpath = mydir + str(unix_dt) + '_pfx.txt'
                f = open(fpath, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    pfx = line.split(':')[0]
                    line = line.split(':')[1].split('|')
                    uq = int(line[0])
                    uv = float(line[1])
                    if uq >= reaper.Tq:
                        huqp_set.add(pfx)
                        if uv >= reaper.Tv:
                            hap_set.add(pfx)
                    if uv >= reaper.Tv:
                        huvp_set.add(pfx)
                f.close()

                #--------------------------------------------
                # new h prefixs that exist for the first time
                new_huvp_set = set()
                new_huqp_set = set()
                new_hap_set = set()

                for p in huvp_set:
                    if p not in all_huvp:
                        new_huvp_set.add(p)
                for p in huqp_set:
                    if p not in all_huqp:
                        new_huqp_set.add(p)
                for p in hap_set:
                    if p not in all_hap:
                        new_hap_set.add(p)

                all_huvp = all_huvp | huvp_set
                all_huqp = all_huqp | huqp_set
                all_hap = all_hap | hap_set


                #-------------------------------------------
                # new prefixes that have not existed in the previous N slots
                new_huvp_N = set()
                new_huqp_N = set()
                new_hap_N = set()

                for p in huvp_set:
                    existed = False
                    for s in nhuvp:
                        if p in s:
                            existed = True
                            break
                    if existed == False:
                        new_huvp_N.add(p)
                for p in huqp_set:
                    existed = False
                    for s in nhuqp:
                        if p in s:
                            existed = True
                            break
                    if existed == False:
                        new_huqp_N.add(p)
                for p in hap_set:
                    existed = False
                    for s in nhap:
                        if p in s:
                            existed = True
                            break
                    if existed == False:
                        new_hap_N.add(p)

                nhuvp.append(huvp_set)
                if len(nhuvp) > new_n:
                    nhuvp.popleft()
                nhuqp.append(huqp_set)
                if len(nhuqp) > new_n:
                    nhuqp.popleft()
                nhap.append(hap_set)
                if len(nhap) > new_n:
                    nhap.popleft()

                fo.write(str(unix_dt)+':'+str(len(new_huqp_set))+'|'+str(len(new_huvp_set))+'|'+\
                         str(len(new_hap_set))+'&'+str(len(new_huqp_N))+'|'+str(len(new_huvp_N))+\
                         '|'+str(len(new_hap_N))+'\n')

            fo.close()

    def hpfx_life_time(self):
        Tv = self.rlist[0].Tv
        Tq = self.rlist[0].Tq

        huqp2lt = dict() # h prefix 2 total lifetime/slots
        huvp2lt = dict() # h prefix 2 total lifetime/slots
        hap2lt = dict() # h prefix 2 total lifetime/slots

        '''
        huqp2lt_cont = dict() # h prefix 2 longest continuous lifetime/slots
        huvp2lt_cont = dict() # h prefix 2 longest continuous lifetime/slots
        hap2lt_cont = dict() # h prefix 2 longest continuous lifetime/slots

        pre_huqp_set = set([]) 
        pre_huvp_set = set([]) 
        pre_hap_set = set([]) 
        '''

        count = 0
        for reaper in self.rlist:
            for fg in reaper.filegroups:
                count += 1
                print '******************Round ', count
                unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group

                # sets for this slot only
                huvp_set = set()
                huqp_set = set()
                hap_set = set()

                mydir = reaper.pfx_final_dir + 'default/'
                fpath = mydir + str(unix_dt) + '_pfx.txt'
                f = open(fpath, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    pfx = line.split(':')[0]
                    line = line.split(':')[1].split('|')
                    uq = int(line[0])
                    uv = float(line[1])
                    if uq >= reaper.Tq:
                        huqp_set.add(pfx)
                        if uv >= reaper.Tv:
                            hap_set.add(pfx)
                    if uv >= reaper.Tv:
                        huvp_set.add(pfx)
                f.close()

                for p in huqp_set:
                    try:
                        huqp2lt[p] += 1
                    except:
                        huqp2lt[p] = 1
                for p in huvp_set:
                    try:
                        huvp2lt[p] += 1
                    except:
                        huvp2lt[p] = 1
                for p in hap_set:
                    try:
                        hap2lt[p] += 1
                    except:
                        hap2lt[p] = 1

        mydir = self.pfx_root
        outpath = mydir+'lifetime_huvp_'+str(Tv)+'_huqp_'+str(Tq)+'.txt'
        fo = open(outpath, 'w')
        for p in huqp2lt:
            fo.write('#'+p+':'+str(huqp2lt[p])+'\n')
        for p in huvp2lt:
            fo.write('%'+p+':'+str(huvp2lt[p])+'\n')
        for p in hap2lt:
            fo.write('A'+p+':'+str(hap2lt[p])+'\n')
        fo.close()
