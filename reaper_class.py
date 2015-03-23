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
        self.shift = shift

        self.middle_dir = period.get_middle_dir()
        self.final_dir = period.get_final_dir()

        self.blank_file = period.get_blank_dir() + 'blank.txt'
        self.blank_info = list() # list of lists
        rm_char = [' ', '[',']','\'']
        if os.path.isfile(self.blank_file):
            f = open(self.blank_file, 'r')
            for line in f:
                line = line.strip('\n')
                for c in rm_char:
                    line = line.replace(c, '')
                attr = line.split(',')
                tmp_list = [int(attr[1]),int(attr[2]),\
                        float(attr[3]),attr[0]] # start,end,mcount,collector
                self.blank_info.append(tmp_list)
            f.close()

        mfiles = os.listdir(self.middle_dir)
        for f in mfiles:
            if not f.endswith('.gz'):
                mfiles.remove(f)
        mfiles.sort(key=lambda x:int(x.rstrip('.txt.gz')))

        # get granularity of middle files
        self.m_granu = (int(mfiles[1].rstrip('.txt.gz')) - int(mfiles[0].rstrip('.txt.gz'))) / 60
        shift_file_c = shift / self.m_granu
        mfiles = mfiles[shift_file_c:] # shift the interval

        self.granu = granu
        group_size = self.granu / self.m_granu
        self.filegroups = list() # list of file groups
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                self.filegroups.append(group)
                group = []

        #self.filegroups = self.filegroups[:3] #XXX test

        # DV and UQ thresholds (set by a self function)
        self.dv_thre = None
        self.uq_thre = None

        self.pfx2as = radix.Radix()
        
        #--------------------------------------------------------------------
        # variables for analyzing all types of prefixes
        # XXX if memo too hard, scan two or more times

        # record all pfx and data in the current interval
        self.c_pfx_data = radix.Radix()

        # recore time series of three types of prefixes. datetime: value
        self.hdv_ts = dict()
        self.huq_ts = dict()
        self.h2_ts = dict()
        self.pfx_ts = dict()

        # overall updates time series of certain prefixes
        self.uq_ts_hdv = dict()
        self.uq_ts_huq = dict()
        self.uq_ts_h2 = dict()
        # total updates time series 
        self.uq_ts = dict()

        # overall DV distribution of certain prefixes
        self.dv_distr_hdv = dict() # DV value: existence
        self.dv_distr_huq = dict() # DV value: existence
        self.dv_distr_h2 = dict() # DV value: existence

        # Lifetime of 3 types of H prefixes
        self.pfx_lifetime = radix.Radix() # XXX costs memo
        self.lifet_distr_hdv = dict() # life time: prefix count
        self.lifet_distr_huq = dict() # life time: prefix count
        self.lifet_distr_h2 = dict() # life time: prefix count

        self.p_hset = radix.Radix() # H prefix set in the previous interval 
        self.c_hset = radix.Radix() # current high prefix set
        # New HDV and HUQ quantity time series
        self.new_hdv_ts = dict()
        self.new_huq_ts = dict()
        self.new_h2_ts = dict()
        self.newp_hdv_ts = dict()
        self.newp_huq_ts = dict()
        self.newp_h2_ts = dict()

        # total DV and UQ distribution
        self.dv_distr_all = dict()
        self.uq_distr_all = dict()
        # DV and UQ distribution for certain period # TODO implement when dealing with 2013
        #self.dv_distr[period1] = dict()
        #self.uq_distr[period1] = dict()


        #---------------------------------------------------------------
        # variables for detecting events
        self.bmatrix = None # a binary matrix
        self.thre_size = None
        self.width_ratio = None
        self.thre_width = None
        self.thre_den = None # density threshold

        self.rows = dict() # row number: attribute dict 
        self.cols = dict() #

        self.submatrix = dict() # time: final submatrix feature list TODO
        self.events = dict() # time: event feature list
        self.pfx_number = self.period.get_fib_size()

    # get prefix 2 as mapping from only RouteViews2 collector's RIB
    # TODO test needed
    def get_pfx2as(self):
        # After identifying 'active' prefixes, analyze their origin ASes!
        print 'Getting prefix to AS mapping...'
        ribfile = None
        f = open(self.period.rib_info_file, 'r')
        for line in f:
            line = line.rstrip('\n')
            co = line.split(':')[0]
            if co == '':
                ribfile = line.split(':')[1]
        f.close()

        p = subprocess.Popen(['zcat', ribfile], stdout=subprocess.PIPE)
        f = StringIO(p.communicate()[0])
        assert p.returncode == 0
        for line in f:
            line = line.rstrip('\n').split('|')
            pfx = line[5]
            origin_as = int(line[6].split()[-1])
            rnode = self.pfx2as.add(pfx)
            rnode.data[0] = origin_as
        f.close()
        

    def set_dv_uq_thre(self, dvt, uqt): # Input a dict of exact format
        self.dv_thre = dvt
        self.uq_thre = uqt

    def get_output_dir_pfx(self):
        assert self.dv_thre != None and self.uq_thre != None
        return self.final_dir + str(self.dv_thre).lstrip('0.') + '_' + str(self.uq_thre) +\
                '_' + str(self.m_granu) + '_' + str(self.granu) + '_' + str(self.shift) + '/'

    # Do many tasks in only one scan of all files!
    def analyze_pfx(self):
        for fg in self.filegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group

            self.hdv_ts[unix_dt] = 0
            self.huq_ts[unix_dt] = 0
            self.h2_ts[unix_dt] = 0
            self.pfx_ts[unix_dt] = 0

            self.uq_ts_hdv[unix_dt] = 0
            self.uq_ts_huq[unix_dt] = 0
            self.uq_ts_h2[unix_dt] = 0
            self.uq_ts[unix_dt] = 0

            self.new_hdv_ts[unix_dt] = 0
            self.new_huq_ts[unix_dt] = 0
            self.new_h2_ts[unix_dt] = 0
            self.newp_hdv_ts[unix_dt] = 0
            self.newp_huq_ts[unix_dt] = 0
            self.newp_h2_ts[unix_dt] = 0

            for f in fg:
                self.read_a_file_pfx(self.middle_dir+f)

            self.analyze_interval(unix_dt)
            self.p_hset = self.c_hset
            self.c_hset = radix.Radix() # set the current high radix to empty
            self.c_pfx_data = radix.Radix()
            print 'Analyzed one interval.'

        self.output_pfx()


    def read_a_file_pfx(self, floc):
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

            rnode = self.c_pfx_data.search_exact(pfx)
            if rnode is None:
                rnode = self.c_pfx_data.add(pfx)
                rnode.data[0] = datalist
            else:
                c_datalist = rnode.data[0]
                combined = [x+y for x,y in zip(datalist, c_datalist)]
                rnode.data[0] = combined
        fin.close()

    def analyze_interval(self, unix_dt):
        uq_total = 0
        for rnode in self.c_pfx_data:
            pfx = rnode.prefix
            data = rnode.data[0]

            count = 0
            uq = 0 # update quantity
            for d in data:
                if d > 0:
                    count += 1
                    uq += d
            dv = count/self.mo_number # dynamic visibility

            self.pfx_ts[unix_dt] += 1
            #---------------------------------------------------------
            # analyze the DV and UQ
            huq_flag = False
            if uq > self.uq_thre: # the prefix is a HUQ prefix
                self.huq_ts[unix_dt] += 1
                self.uq_ts_huq[unix_dt] += uq

                self.distr_add_one(self.dv_distr_huq, dv)

                if self.is_newp(pfx, 'huq'):
                    self.newp_huq_ts[unix_dt] += 1
                if self.is_new(pfx, 'huq'):
                    self.new_huq_ts[unix_dt] += 1
                self.add_c_high_set(pfx, 'huq')
                self.increase_lifetime(pfx, 'huq')

                huq_flag = True
                #print uq,dv
    
            # note: high dv usually indicate high uq but not vice versa

            if dv > self.dv_thre: # the prefix is a HDV prefix
                self.hdv_ts[unix_dt] += 1
                self.uq_ts_hdv[unix_dt] += uq

                self.distr_add_one(self.dv_distr_hdv, dv)

                if self.is_newp(pfx, 'hdv'):
                    self.newp_hdv_ts[unix_dt] += 1
                if self.is_new(pfx, 'hdv'):
                    self.new_hdv_ts[unix_dt] += 1
                self.add_c_high_set(pfx, 'hdv')
                self.increase_lifetime(pfx, 'hdv')

                if huq_flag: # the prefix is a H2 prefix
                    self.h2_ts[unix_dt] += 1
                    self.uq_ts_h2[unix_dt] += uq

                    self.distr_add_one(self.dv_distr_h2, dv)

                    if self.is_newp(pfx, 'h2'):
                        self.newp_h2_ts[unix_dt] += 1
                    if self.is_new(pfx, 'h2'):
                        self.new_h2_ts[unix_dt] += 1
                    self.add_c_high_set(pfx, 'h2')
                    self.increase_lifetime(pfx, 'h2')

            # Total update quantity
            self.uq_ts[unix_dt] += uq

            # Total DV and UQ distribution
            self.distr_add_one(self.dv_distr_all, dv)
            uq_total += uq

        self.distr_add_one(self.uq_distr_all, uq_total)


    def output_pfx(self):
        print 'Writing to final output...'
        output_dir = self.get_output_dir_pfx()
        print output_dir
        cmlib.make_dir(output_dir)

        # conduct some final calculation before outputing
        for rn in self.pfx_lifetime:
            the_data = rn.data
            for htype in the_data.keys():
                v = the_data[htype]
                eval('self.distr_add_one(self.lifet_distr_'+htype+','+str(v)+')')

        selfv = self.__dict__.keys()
        ts_v = []
        distr_v = []
        for v in selfv:
            if '_ts' in v:
                ts_v.append(v)
            elif '_distr' in v:
                distr_v.append(v)

        for v in ts_v:
            value = eval('self.' + v)
            fname = v + '.txt'
            self.output_ts(value, output_dir + fname)

        for v in distr_v:
            value = eval('self.' + v)
            fname = v + '.txt'
            self.output_distr(value, output_dir + fname)

        self.output_radix(self.pfx_lifetime, output_dir + 'pfx_lifetime.txt')

    def output_ts(self, mydict, floc):
        f = open(floc, 'w')
        for dt in mydict:
            f.write(str(dt)+':'+str(mydict[dt])+'\n')
        f.close()

    def output_distr(self, mydict, floc):
        f = open(floc, 'w')
        for v in mydict:
            f.write(str(v)+':'+str(mydict[v])+'\n')
        f.close()

    def output_radix(self, rtree, floc):
        f = open(floc, 'w')
        for rnode in rtree:
            f.write(rnode.prefix+'@')
            rdata = rnode.data
            for k in rdata.keys():
                f.write(k+':'+str(rdata[k])+'|')
            f.write('\n')
        f.close()

    def increase_lifetime(self, pfx, key):
        rnode = self.pfx_lifetime.search_exact(pfx)
        if rnode is None:
            mynode = self.pfx_lifetime.add(pfx)
            mynode.data[key] = 1
        else:
            try:
                rnode.data[key] += 1
            except:
                rnode.data[key] = 1

    def distr_add_one(self, distr_dict, key):
        try:
            distr_dict[key] += 1
        except:
            distr_dict[key] = 1

    def add_c_high_set(self, pfx, key):
        rnode = self.c_hset.add(pfx)
        rnode.data[key] = True

    def is_newp(self, pfx, key):
        rnode = self.p_hset.search_exact(pfx)
        if rnode is None:
            return True
        else:
            try:
                test = rnode.data[key]
                return False #Found
            except:
                return True

    def is_new(self, pfx, key):
        rnode = self.pfx_lifetime.search_exact(pfx)
        if rnode is None:
            return True
        else:
            try:
                test = rnode.data[key]
                return False #Found
            except:
                return True


    #----------------------------------------------------------------------
    # For detecting disruptive events
    def set_event_thre(self, size_ratio, width_ratio, density):
        self.thre_size = size_ratio * self.pfx_number * self.mo_number # recommand: 0.5%
        self.width_ratio = width_ratio
        self.thre_den = density # recommand: 0.8 or 0.85?
        logging.info('thre_size:%d',self.thre_size)

        #min_row_sum = 0.1 * self.thre_width # XXX good?
        #min_col_sum = 0.1 * (float(self.thre_size) / float(self.mo_number)) # XXX 
        #logging.info('preprocess thresholds row %f col %f', min_row_sum, min_col_sum)

    def analyze_bmatrix(self):
        size = self.bmatrix.size
        if size < self.thre_size:
            logging.info('%d final submatrix info: too small', unix_dt)
            return -1

        #-------------------
        # preprocess the matrix
        height = self.bmatrix.shape[0]
        width = self.bmatrix.shape[1]
        row_must_del = []
        col_must_del = []
        
        min_row_sum = 0.1 * self.thre_width # XXX good?
        min_col_sum = 0.1 * (float(self.thre_size) / float(self.mo_number)) # XXX 

        for i in xrange(0, height):
            if self.bmatrix[i].sum() <= min_row_sum:
                row_must_del.append(i)
        for i in xrange(0, width):
            if self.bmatrix[:,i].sum() <= min_col_sum:
                col_must_del.append(i)

        self.bmatrix = np.delete(self.bmatrix,row_must_del,0)
        self.bmatrix = np.delete(self.bmatrix,col_must_del,1)

        size = float(self.bmatrix.size)
        width = self.bmatrix.shape[1]
        if size < self.thre_size or width < self.thre_width:
            logging.info('%d final submatrix info: too small after preprocess', unix_dt)
            return -1

        #--------------------
        # process the matrix
        sum = float(np.sum(self.bmatrix))
        density = sum/size
        height = self.bmatrix.shape[0]
        width = self.bmatrix.shape[1]
        now_den = density

        row_del_score = None
        col_del_score = None
        while(now_den < self.thre_den):
            #-------------------------
            sum = float(np.sum(self.bmatrix))
            size = float(self.bmatrix.size)
            density = sum/size
            height = self.bmatrix.shape[0]
            width = self.bmatrix.shape[1]

            #-----------------------------------------------
            # obtain candidate prefixes to delete
            if row_del_score != -1:
                row_one = dict()
                for i in xrange(0, height):
                    row_one[i] = self.bmatrix[i].sum()

                row_one = sorted(row_one.items(), key=operator.itemgetter(1))
                min_row_one = row_one[0][1]

                row_to_del = list()
                for item in row_one:
                    if item[1] == min_row_one:
                        row_to_del.append(item[0])
                    else:
                        break

                row_dsize = float(len(row_to_del) * width)
                row_dsum = 0.0
                for index in row_to_del:
                    row_dsum += self.bmatrix[index].sum()
                row_del_score = ((sum-row_dsum)/(size-row_dsize)-density)/row_dsize

                new_rsize = size - len(row_to_del) * width
                if new_rsize < self.thre_size:
                    row_del_score = -1

            #-----------------------------------------------
            # obtain candidate monitors to delete
            if col_del_score != -1:
                col_one = dict()
                for i in xrange(0, width):
                    col_one[i] = self.bmatrix[:,i].sum()

                col_one = sorted(col_one.items(), key=operator.itemgetter(1))
                min_col_one = col_one[0][1]

                col_to_del = list()
                for item in col_one:
                    if item[1] == min_col_one:
                        col_to_del.append(item[0])
                    else:
                        break

                col_dsize = float(len(col_to_del) * height)
                col_dsum = 0.0
                for index in col_to_del:
                    col_dsum += self.bmatrix[:,index].sum()
                col_del_score = ((sum-col_dsum)/(size-col_dsize)-density)/col_dsize

                new_width = width - len(col_to_del)
                if new_width < self.thre_width:
                    col_del_score = -1 # never del col any more

                new_csize = size - len(col_to_del) * height
                if new_csize < self.thre_size:
                    col_del_score = -1

            #-------------------------------------------
            # decide which to delete
            if row_del_score == -1 and col_del_score == -1:
                print 'fail to find such submatrix'
                logging.info('%d, fail to find such submatrix', unix_dt)
                return -1 # XXX note that now_den may still be small
            elif col_del_score == -1 or row_del_score >= col_del_score:
                self.bmatrix = np.delete(self.bmatrix,row_to_del,0)
                now_den = (sum-row_dsum)/(size-row_dsize)
                #print 'deleted row:', row_to_del
            else:
                self.bmatrix = np.delete(self.bmatrix,col_to_del,1)
                now_den = (sum-col_dsum)/(size-col_dsize)
                #print 'deleted col:', col_to_del

        sum = float(np.sum(self.bmatrix))
        size = float(self.bmatrix.size)
        density = sum/size
        height = self.bmatrix.shape[0]
        width = self.bmatrix.shape[1]
        # No matter size, density, etc now stores current bmatrix's info
        logging.info('%d final submatrix info: %s', unix_dt,str([size, density, height, width]))
        if size >= self.thre_size and density >= self.thre_den and width >= self.thre_width:
            self.events[unix_dt] = [size, density, height, width]
            logging.info('found event!')

    def analyze_bmatrix_new(self):
        size = self.bmatrix.size
        if size < self.thre_size:
            logging.info('%d final submatrix info: too small', unix_dt)
            return -1

        #-------------------
        # preprocess the matrix
        height = self.bmatrix.shape[0]
        width = self.bmatrix.shape[1]
        row_must_del = []
        col_must_del = []
        
        min_row_sum = 0.1 * self.thre_width # XXX good?
        min_col_sum = 0.1 * (float(self.thre_size) / float(self.mo_number)) # XXX 

        for i in xrange(0, height):
            if self.bmatrix[i].sum() <= min_row_sum:
                row_must_del.append(i)
        for i in xrange(0, width):
            if self.bmatrix[:,i].sum() <= min_col_sum:
                col_must_del.append(i)

        self.bmatrix = np.delete(self.bmatrix,row_must_del,0)
        self.bmatrix = np.delete(self.bmatrix,col_must_del,1)

        size = float(self.bmatrix.size)
        width = self.bmatrix.shape[1]
        if size < self.thre_size or width < self.thre_width:
            logging.info('%d final submatrix info: too small after preprocess', unix_dt)
            return -1

        #--------------------
        # process the matrix
        sum = float(np.sum(self.bmatrix))
        density = sum/size
        height = self.bmatrix.shape[0]
        width = self.bmatrix.shape[1]
        now_den = density

        row_del_score = None
        col_del_score = None
        while(now_den < self.thre_den):
        


    def detect_event(self):
        for fg in self.filegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            self.thre_width = self.mo_number * self.width_ratio

            fstart = unix_dt
            fend = unix_dt + self.granu * 60    
            for item in self.blank_info:
                start = item[0]
                end = item[1]
                mcount = item[2]

                if fstart >= start and fend <= end:
                    self.thre_width -= mcount * self.width_ratio

            logging.info('self.thre_width=%f',self.thre_width)

            for f in fg:
                self.read_a_file_event(self.middle_dir+f)

            # convert integer lists to binary lists just before preprocessing
            blists = list()
            for rnode in self.c_pfx_data:
                blist = []
                ilist = rnode.data[0]
                for value in ilist:
                    if value > 0:
                        blist.append(1)
                    else:
                        blist.append(0)
                blists.append(blist)

            print 'analyzing matrix...'
            self.bmatrix = np.array(blists)
            #self.analyze_bmatrix() # old algorithm
            self.analyze_bmatrix_new() # new algorithm
            self.bmatrix = None

            self.c_pfx_data = radix.Radix()

        self.output_event()

    def read_a_file_event(self, floc):
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

            rnode = self.c_pfx_data.search_exact(pfx)
            if rnode is None:
                rnode = self.c_pfx_data.add(pfx)
                rnode.data[0] = datalist
            else:
                c_datalist = rnode.data[0]
                combined = [x+y for x,y in zip(datalist, c_datalist)]
                rnode.data[0] = combined

        fin.close()

    def get_output_dir_event(self):
        s = str(self.thre_size)
        tmp = s.index('.')
        s = s[:tmp]
        w = str(self.thre_width)
        tmp = w.index('.')
        w = w[:tmp]
        return self.final_dir + s + '_' + w +\
                '_' + str(self.thre_den).lstrip('0.') + '_' + str(self.m_granu) +\
                '_' + str(self.granu) + '_' + str(self.shift) + '/'

    def output_event(self):
        print 'Writing to final output...'
        output_dir = self.get_output_dir_event()
        print output_dir
        cmlib.make_dir(output_dir)
        
        #f = open(output_dir+'events.txt', 'w') # first try deteting events
        f = open(output_dir+'events_new.txt', 'w')
        for dt in self.events:
            f.write(str(dt)+':'+str(self.events[dt])+'\n')
        f.close()
