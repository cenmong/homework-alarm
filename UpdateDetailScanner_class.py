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

class UpdateDetailScanner():

    def __init__(self, period, granu):
        self.period = period
        self.filelist = period.get_filelist()
        self.sdate = period.sdate
        self.edate = period.edate

        self.sdt_obj = period.sdatetime_obj
        self.edt_obj = period.edatetime_obj
        self.granu = granu

        self.uflist = list()
        f = open(self.period.get_filelist(), 'r')
        for line in f:
            ufile = line.split('|')[0]
            self.uflist.append(datadir + ufile)

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

    def numf_distr_output_dir(self):
        dir = metrics_output_root + str(self.granu) + '/' + self.sdate + '_' + self.edate + '/'
        cmlib.make_dir(dir)
        return dir

    def get_num_feature_distr(self):
        for slot in self.dtobj_list:
            print '********************Now processing slot ', slot
            self.get_distr_for_slot(slot)
        
    def get_distr_for_slot(self, slot):
        sdt_unix = calendar.timegm(slot[0].utctimetuple())
        edt_unix = calendar.timegm(slot[1].utctimetuple())

        #------------------ Numerical metrics ------------------
        # 0:updates 1:A 2:W    3:WW 4:AADup1 5:AADup2 6:AADiff 7:WAUnknown 8:WADup 9:WADiff 10:AW
        # 11: updates pfx 12: announced pfx 13: withdrawn pfx (11,12,13 are obtianed in the end)
        metric_num = 11
        tmetrics = dict() # total metrics

        mon2metrics = dict()
        for m in self.monitors:
            mon2metrics[m] = dict()

        # initialization
        for i in range(metric_num):
            tmetrics[i] = 0
            for key in mon2metrics:
                mon2metrics[key][i] = 0


        # ------------------ a special mechanism -----------------
        # the number of pfx (unconfined) can be quite large, which may lead to memory issue
        # So we map pfx to an integer (0~1,000,000) to save memory 
        pfxindex = 0
        pfx2num = dict()


        # ----------------- Update prefix metrics ----------------
        # updated prefix set    announced prefix set    withdrawn prefix set
        Am2pset = dict()
        Wm2pset = dict()
        Tm2pset = dict() # total
        Apset = set()
        Wpset = set()
        Tpset = set() # total

        # ----------------- tmp variables ----------------
        mp_last_A = dict() # mon: prefix: latest announcement full content
        mp_last_type = dict()
        for m in self.monitors:
            mp_last_A[m] = dict() # NOTE: does not record W, only record A
            mp_last_type[m] = dict()

            Am2pset[m] = set()
            Wm2pset[m] = set()
            Tm2pset[m] = set()

        # ----------------- Obtain and read the update files ----------------
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
                    try:
                        pfx = pfx2num[pfx] # pfx string to num to save memory
                    except:
                        pfx2num[pfx] = pfxindex
                        pfx = pfxindex
                        pfxindex += 1

                    type = attr[2]
                    mon = attr[3]

                    # (1) increase the number of U, A, and W
                    # (2) increase the number of prefix sets
                    mon2metrics[mon][0] += 1
                    tmetrics[0] += 1

                    if type == 'A':
                        as_path = attr[6]
                        mon2metrics[mon][1] += 1
                        tmetrics[1] += 1

                        Apset.add(pfx)
                        Am2pset[mon].add(pfx)
                    else:
                        mon2metrics[mon][2] += 1
                        tmetrics[2] += 1

                        Wpset.add(pfx)
                        Wm2pset[mon].add(pfx)

                    # Obtain existent information
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
                            mon2metrics[mon][3] += 1
                            tmetrics[3] += 1
                        elif type == 'A':
                            if last_as_path:
                                if as_path == last_as_path:
                                    mon2metrics[mon][8] += 1
                                    tmetrics[8] += 1
                                else:
                                    mon2metrics[mon][9] += 1
                                    tmetrics[9] += 1
                            else:
                                mon2metrics[mon][7] += 1
                                tmetrics[7] += 1
                            mp_last_A[mon][pfx] = line
                    elif last_type == 'A':
                        if type == 'W':
                            mon2metrics[mon][10] += 1
                            tmetrics[10] += 1
                        elif type == 'A':
                            if line == last_A:
                                mon2metrics[mon][4] += 1
                                tmetrics[4] += 1
                            elif as_path == last_as_path:
                                mon2metrics[mon][5] += 1
                                tmetrics[5] += 1
                            else:
                                mon2metrics[mon][6] += 1
                                tmetrics[6] += 1
                            mp_last_A[mon][pfx] = line
                    else: # last_type == None
                        pass
                
                    # Update existent information
                    if type == 'A':
                        mp_last_type[mon][pfx] = 'A'
                        mp_last_A[mon][pfx] = line
                    else:
                        mp_last_type[mon][pfx] = 'W'

                except:
                    pass

            myf.close()

        # obtain the 11th, 12th, and 13th metrics
        Tpset = Apset | Wpset
        for m in Wm2pset:
            Tm2pset[m] = Am2pset[m] | Wm2pset[m]

        tmetrics[11] = len(Tpset)
        tmetrics[12] = len(Apset)
        tmetrics[13] = len(Wpset)

        for m in mon2metrics:
            mon2metrics[m][11] = len(Tm2pset[m])
            mon2metrics[m][12] = len(Am2pset[m])
            mon2metrics[m][13] = len(Wm2pset[m])

        # Output the overall and per-monitor statistics
        outpath = self.numf_distr_output_dir() + str(sdt_unix) + '.txt'
        f = open(outpath, 'w')
        f.write('T:'+str(tmetrics)+'\n')
        for m in mon2metrics:
            f.write(m+':'+str(mon2metrics[m])+'\n')
        f.close()

        # free memory
        del Am2pset
        del Wm2pset
        del Apset
        del Wpset
        del mp_last_A
        del mp_last_type
        del tmetrics
        del mon2metrics


    def get_num_feature_metric(self):
        slot2metrics = dict() # slot sdt_unix: metric: a list of metric values

        for slot in self.dtobj_list:
            print '********************Getting metrics for slot ', slot
            total_dict = None
            mon2dict = dict() 

            sdt_unix = calendar.timegm(slot[0].utctimetuple())
            slot2metrics[sdt_unix] = dict()

            rpath = self.numf_distr_output_dir() + str(sdt_unix) + '.txt'
            f = open(rpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                name = line.split(':')[0]
                mydict = line.replace(name+':', '')
                mydict = ast.literal_eval(mydict)

                if name == 'T':
                    total_dict = mydict
                else:
                    mon2dict[name] = mydict
            f.close()

            # feature_num = len(total_dict.keys()) # number of features
            feature_num = 11 # we omit the updated prefix quantity


            # Obtain the GINI index
            slot2metrics[sdt_unix]['GINI'] = dict()
            for i in range(feature_num):
                tvalue = total_dict[i] * 1.0

                sorted_list = list()
                for mon in mon2dict:
                    mvalue = mon2dict[mon][i]
                    sorted_list.append(mvalue*1.0)
                sorted_list.sort()

                for j in range(1,len(sorted_list)): 
                    sorted_list[j] += sorted_list[j-1]

                #print tvalue
                #print sorted_list
                assert sorted_list[-1] == tvalue

                num = len(sorted_list) * 1.0
                sum = 0.0
                for item in sorted_list:
                    sum += item
                sum -= tvalue / 2
                if tvalue != 0:
                    result = 1 - 2*sum / (num*tvalue)
                    slot2metrics[sdt_unix]['GINI'][i] = result
                else:
                    slot2metrics[sdt_unix]['GINI'][i] = -1 # XXX not applicable

            # Obtain the HI values
            slot2metrics[sdt_unix]['HI'] = dict()
            for i in range(feature_num):
                HI = 0
                tvalue = total_dict[i]
                for mon in mon2dict:
                    mvalue = mon2dict[mon][i]
                    if mvalue != 0:
                        ratio = float(mvalue) / float(tvalue)
                        HI += ratio * ratio
                slot2metrics[sdt_unix]['HI'][i] = HI
                if tvalue == 0:
                    slot2metrics[sdt_unix]['HI'][i] = -1 # XXX not applicable

            # Obtain the Dynamic Visibiliy values
            slot2metrics[sdt_unix]['DV'] = dict()
            for i in range(feature_num):
                DV = 0
                tvalue = total_dict[i]
                for mon in mon2dict:
                    mvalue = mon2dict[mon][i]
                    if mvalue != 0:
                        DV += 1
                DV = float(DV) / float(len(self.monitors))
                slot2metrics[sdt_unix]['DV'][i] = DV

            # Obtain the concentration ratios
            CR_ints = [1, 4, 8]
            for my_int in CR_ints:
                slot2metrics[sdt_unix][my_int] = dict()

            CR_ratios = [0.1, 0.2, 0.3]
            for my_r in CR_ratios:
                slot2metrics[sdt_unix][my_r] = dict()

            for i in range(feature_num):
                tvalue = total_dict[i]
                mvalues = list()
                for mon in mon2dict:
                    mvalue = mon2dict[mon][i]
                    mvalues.append(mvalue)
                mvalues.sort(reverse=True) # large to small

                for my_int in CR_ints:
                    sum = 0
                    for j in range(my_int):
                        sum += mvalues[j]
                    if tvalue != 0:
                        final = float(sum) / float(tvalue)
                        slot2metrics[sdt_unix][my_int][i] = final
                    else:
                        slot2metrics[sdt_unix][my_int][i] = -1

                for my_r in CR_ratios:
                    sum = 0
                    mon_num = int(len(self.monitors)*my_r)
                    for j in range(mon_num):
                        sum += mvalues[j]
                    if tvalue != 0:
                        final = float(sum) / float(tvalue)
                        slot2metrics[sdt_unix][my_r][i] = final
                    else:
                        slot2metrics[sdt_unix][my_r][i] = -1

        # output
        f = open(self.numf_metrics_fpath(), 'w')
        for slot in sorted(slot2metrics.keys()):
            for metric in slot2metrics[slot]:
                f.write(str(slot)+'|'+str(metric)+'|'+str(slot2metrics[slot][metric])+'\n')
        f.close()


    def numf_metrics_fpath(self): 
        dir = metrics_output_root + str(self.granu) + '/' + self.sdate + '_' + self.edate + '/'
        cmlib.make_dir(dir)
        return dir+'num_fea_metrics.txt'

    # TODO def analyze_active_pfx(self): 
    # note: use middle files
    # note: monitor id -> ip
