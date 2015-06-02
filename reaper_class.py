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
from sklearn.cluster import DBSCAN

from cStringIO import StringIO
from netaddr import *
from env import *
#from supporter_class import *
from cStringIO import StringIO

class Reaper():

    def __init__(self, period, granu, shift):
        self.period = period
        self.mo_number = float(self.period.get_mo_number())
        self.pfx_number = self.period.get_fib_size()
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

        self.pfx2as = dict()
        
        # record all pfx and data in the current interval
        ##self.c_pfx_data = radix.Radix()
        self.c_pfx_data = dict()


        #--------------------------------------------------------------------
        # variables for analyzing all types of prefixes
        # XXX if memo too hard, scan two or more times

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



        #============================================================
        # variables for detecting events
        self.bmatrix = None # the original prefix: list binary matrix
        self.index2pfx = dict()

        #------------------------------------------
        # thresholds
        self.size_ratio = None
        self.thre_size = None
        self.width_ratio = None
        self.thre_width = None
        self.thre_den = None # density threshold

        #--------------------------------------
        # For detection algorithm
        #self.in_rows = set() 
        #self.in_cols = set()

        #self.out_rows = set() # deleted rows
        #self.out_cols = set()

        self.in_value2rowset = dict()
        self.in_value2colset = dict()
        self.out_value2rowset = dict()
        self.out_value2colset = dict()

        #self.in_row_ones = dict() # row number: quantity of 1s
        #self.in_col_ones = dict() # col number: quantity of 1s
        #self.out_row_ones = dict() # row number: quantity of 1s
        #self.out_col_ones = dict() # col number: quantity of 1s

        # update these 4 attributes after each matrix manipulation
        self.in_candi_row = -1
        self.in_candi_col = -1
        self.out_candi_row = -1
        self.out_candi_col = -1
        
        self.in_candirow_ones = -1
        self.in_candicol_ones = -1
        self.out_candirow_ones = -1
        self.out_candicol_ones = -1

        #self.row_weight = dict() # row number: weight
        #self.col_weight = dict() # col number: weight
    
        #---------------------------------------
        # attributes for the event submatrix
        self.event_size = None
        self.event_ones = None
        self.event_height = None
        self.event_width = None
        self.event_den = None

        self.events = dict() # time: event feature list
        #self.events_brief_fname = 'events_new.txt'
        self.events_brief_fname = 'events_plusminus.txt'


        self.dt2size = dict()

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
            self.pfx2as[pfx]= origin_as
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
            ##self.c_pfx_data = radix.Radix()
            self.c_pfx_data = dict()
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

            ##rnode = self.c_pfx_data.search_exact(pfx)
            ##if rnode is None:
            ##    rnode = self.c_pfx_data.add(pfx)
            ##    rnode.data[0] = datalist
            ##else:
            ##    c_datalist = rnode.data[0]
            ##    combined = [x+y for x,y in zip(datalist, c_datalist)]
            ##    rnode.data[0] = combined
            
            try:
                c_datalist = self.c_pfx_data[pfx]
                combined = [x+y for x,y in zip(datalist, c_datalist)]
                self.c_pfx_data[pfx] = combined
            except:
                self.c_pfx_data[pfx] = datalist

        fin.close()

    def analyze_interval(self, unix_dt):
        uq_total = 0
        for pfx in self.c_pfx_data:
            ##pfx = rnode.prefix
            ##data = rnode.data[0]
            data = self.c_pfx_data[pfx]

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
        self.size_ratio = size_ratio
        self.width_ratio = width_ratio
        self.thre_den = density # recommand: 0.8 or 0.85?
        logging.info('thre_size_ratio:%d', self.size_ratio)

        #min_row_sum = 0.1 * self.thre_width # XXX good?
        #min_col_sum = 0.1 * (float(self.thre_size) / float(self.mo_number)) # XXX 
        #logging.info('preprocess thresholds row %f col %f', min_row_sum, min_col_sum)

    def analyze_bmatrix(self, unix_dt):
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
        min_col_sum = 0.1 * (float(self.thre_size) / float(self.thre_width)) # XXX 

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


    # initialize event row and column attributes
    def init_attri(self, all_rows, all_cols):
        col2ones = dict()
        for c in all_cols:
            col2ones[c] = 0

        total_sum = 0.0
        for r in all_rows:
            sum = 0
            for c in all_cols:
                value = self.bmatrix[r, c]
                sum += value
                col2ones[c] += value
                total_sum += value

            try:
                self.in_value2rowset[sum].add(r)
            except:
                self.in_value2rowset[sum] = set([r])


        for c in all_cols:
            value = col2ones[c]
            try:
                self.in_value2colset[value].add(c)
            except:
                self.in_value2colset[value] = set([c])
            

        small = min(self.in_value2rowset.keys())
        self.in_candi_row = self.in_value2rowset[small].pop()
        self.in_value2rowset[small].add(self.in_candi_row)
        self.in_candirow_ones = small

        small = min(self.in_value2colset.keys())
        self.in_candi_col = self.in_value2colset[small].pop()
        self.in_value2colset[small].add(self.in_candi_col)
        self.in_candicol_ones = small

        self.event_ones = total_sum
        self.event_den = total_sum / self.event_size


    # get binary lists information
    def analyze_bmatrix_new(self, unix_dt):
        size = self.bmatrix.size
        if size < self.thre_size:
            logging.info('%d final submatrix info: too small', unix_dt)
            return -1

        #--------------------------
        #initialize the event submatrix
        for index in xrange(0, len(self.bmatrix.tolist())):
            self.in_rows[index] = True

        for i in xrange(0, int(self.mo_number)):
            self.in_cols[i] = True

        #-------------------
        # preprocess the matrix
        self.event_height = self.bmatrix.shape[0]
        self.event_width = self.bmatrix.shape[1]
        
        min_row_sum = 0.2 * self.thre_width # XXX good?
        for i in xrange(0, self.event_height):
            if self.bmatrix[i].sum() <= min_row_sum:
                del self.in_rows[i]
                self.event_height -= 1

        min_col_sum = 0.2 * (float(self.thre_size) / float(self.thre_width)) # XXX 
        for i in xrange(0, self.event_width):
            if self.bmatrix[:,i].sum() <= min_col_sum:
                del self.in_cols[i]
                self.event_width -= 1

        self.event_size = float(self.event_height * self.event_width)

        if self.event_size < self.thre_size or self.event_width < self.thre_width:
            logging.info('%d : too small after preprocess', unix_dt)
            return -1

        self.init_attri() # initialize row and col attributes and event density

        #--------------------
        # process the matrix
        while(self.event_den < self.thre_den):

            if self.event_size < self.thre_size:
                break

            cand_rows = self.get_dict_min_list(self.row_ones, None)
            cand_cols = self.get_dict_min_list(self.col_ones, None)

            #print self.col_ones

            row_ones = float(self.row_ones[cand_rows[0]])
            col_ones = float(self.col_ones[cand_cols[0]])

            print row_ones,col_ones
            print self.event_width,self.event_height,self.event_den

            rows_eff = ((self.event_ones-row_ones)/\
                    (self.event_size-self.event_width)-self.event_den)/self.event_width
            cols_eff = ((self.event_ones-col_ones)/\
                    (self.event_size-self.event_height)-self.event_den)/self.event_height

            # consider the width threshold
            if self.event_width - 1 < self.thre_width: # cannot delete any more columns
                # if cutting off all row candidates
                rlen = len(cand_rows)
                one_quantity = rlen * rlen
                size_sum = self.event_width * rlen
                tmp_new_den = (self.event_ones - one_quantity) / (self.event_size - size_sum)

                if tmp_new_den < self.thre_den and tmp_new_den > self.event_den:
                    self.event_size -= size_sum
                    self.event_height -= rlen
                    self.event_ones -= one_quantity 
                    self.event_den = tmp_new_den
                    for index in cand_rows:
                        del self.in_rows[index]
                        del self.row_ones[index]
                        del self.row_weight[index]
                    logging.info('Removed %f rows in one loop.', rlen)
                else:
                    self.event_rm_line_ronly(cand_rows[0])
                continue

            print rows_eff, cols_eff

            #if rows_eff < 0:
            #    break

            if rows_eff >= cols_eff:
                target_rows = self.get_dict_min_list(self.row_weight, cand_rows) 
                self.event_rm_line(target_rows[0], 0)
            else:
                target_cols = self.get_dict_min_list(self.col_weight, cand_cols)
                self.event_rm_line(target_cols[0], 1)


        relative_size = self.event_size / (self.thre_width * 2.5 * self.pfx_number)
        logging.info('%d final submatrix: %s', unix_dt,str([relative_size, self.event_size,\
                self.event_den, self.event_height, self.event_width]))
        if self.event_size >= self.thre_size and self.event_den >= self.thre_den\
                and self.event_width >= self.thre_width:
            self.events[unix_dt]=[relative_size,self.event_size,self.event_den,self.event_height,self.event_width]
            logging.info('%d found event: %s', unix_dt,str([relative_size, self.event_size,\
                    self.event_den, self.event_height, self.event_width]))
            return 100
        
        return -1


    def analyze_bmatrix_plusminus(self, unix_dt):
        #------------------------------
        # record the rows and columns after pre-processing
        all_rows = set()
        all_cols = set()

        for index in xrange(0, len(self.bmatrix.tolist())):
            all_rows.add(index)

        for index in xrange(0, int(self.mo_number)):
            all_cols.add(index)

        #-------------------
        # preprocessing the matrix
        
        self.event_height = len(all_rows)
        min_row_sum = 0.2 * self.thre_width
        for i in xrange(0, self.event_height):
            if self.bmatrix[i].sum() <= min_row_sum:
                all_rows.remove(i)
                self.event_height -= 1

        self.event_width = len(all_cols)
        min_col_sum = 0.2 * (float(self.thre_size) / float(self.thre_width))
        for i in xrange(0, self.event_width):
            if self.bmatrix[:,i].sum() <= min_col_sum:
                all_cols.remove(i)
                self.event_width -= 1

        self.event_size = float(self.event_height * self.event_width)
        if self.event_size == 0:
            self.dt2size[unix_dt] = 0
            return 0

        if self.event_size < self.thre_size or self.event_width < self.thre_width:
            logging.info('%d : too small after preprocessing', unix_dt)
            #-------------------------------------
            # XXX comment out: now we want the size for each slot
            #return -1

        self.init_attri(all_rows, all_cols)

        #--------------------
        # process the matrix
        while(self.event_den < self.thre_den):
            #-----------------------------
            # addition (initially, out candidates are -1, ones are also -1, no addtion conducted)

            can_add_r = False
            can_add_c = False
  
            #print self.out_candirow_ones, self.out_candicol_ones
            if (self.event_ones+self.out_candirow_ones)/(self.event_size+self.event_width) >=\
                    self.event_den:
                can_add_r = True
            if (self.event_ones+self.out_candicol_ones)/(self.event_size+self.event_height) >=\
                    self.event_den:
                can_add_c = True

            if can_add_r is True or can_add_c is True:
                if can_add_r is True and can_add_c is True:
                    if self.out_candirow_ones >= self.out_candicol_ones:
                        self.event_add_row()
                    else:
                        self.event_add_col()
                elif can_add_r is True:
                    self.event_add_row()
                else:
                    self.event_add_col()
                
                print 'Row or column added ***********************************************'
                continue

            #----------------------------------
            # deletion 1) compare deletion utility 2) check width threshold

            print self.event_size, self.event_width, self.event_height
            if self.event_size > self.event_width:
                rows_du = ((self.event_ones-self.in_candirow_ones)/\
                        (self.event_size-self.event_width)-self.event_den)/self.event_width
            else:
                rows_du = -999
            if self.event_size > self.event_height:
                cols_du = ((self.event_ones-self.in_candicol_ones)/\
                        (self.event_size-self.event_height)-self.event_den)/self.event_height
            else:
                cols_du = -999
            #print self.event_ones,self.in_candicol_ones,self.event_size,self.event_height,self.event_den

            # we ignore any height threshold because the size threshold will be adequate
            if self.event_width - 1 < self.thre_width: # cannot delete any more columns
                cols_du = -999
            if len(self.in_value2rowset.keys()) == 1: # no more row deletion possible
                rows_du = -999

            if rows_du == -999 and cols_du == -999:
                break
            elif rows_du >= cols_du: # if cols_du is -999 this will definitly be true
                self.event_del_row()
            else:
                self.event_del_col()

            print 'After operation: ', self.event_den


        # addition in the end
        while(self.event_den >= self.thre_den):
            print self.out_candirow_ones
            # addition utility
            row_au = (self.event_ones+self.out_candirow_ones)/(self.event_size+\
                    self.event_width)-self.event_den
            if (self.event_ones+self.out_candirow_ones)/(self.event_size+\
                    self.event_width)<self.thre_den or self.out_candirow_ones==-1:
                row_au = None

            print self.out_candicol_ones
            # addition utility
            col_au = (self.event_ones+self.out_candicol_ones)/(self.event_size+\
                    self.event_height)-self.event_den
            if (self.event_ones+self.out_candicol_ones)/(self.event_size+\
                    self.event_height)<self.thre_den or self.out_candicol_ones==-1:
                col_au = None
            
            if row_au is None and col_au is None: # no addition is possible
                break
            elif col_au is None and row_au is not None:
                self.event_add_row()
                print 'end adding*****************'
            elif row_au is None and col_au is not None:
                self.event_add_col()
                print 'end adding***************'
            elif col_au is not None and row_au is not None:
                print 'end adding******************'
                if row_au >= col_au:
                    self.event_add_row()
                else:
                    self.event_add_col()


        #----------------------------------------------------------
        # summary
        relative_size = self.event_size / (self.thre_width * 2.5 * self.pfx_number)
        logging.info('%d final submatrix: %s', unix_dt,str([relative_size, self.event_size,\
                self.event_den, self.event_height, self.event_width]))

        if self.event_size >= self.thre_size and self.event_den >= self.thre_den\
                and self.event_width >= self.thre_width:
            self.events[unix_dt]=[relative_size,self.event_size,\
                    self.event_den,self.event_height,self.event_width]
            logging.info('%d this is an event!')
            return 100
        elif self.event_den >= self.thre_den:
            self.dt2size[unix_dt] = relative_size
        else:
            self.dt2size[unix_dt] = 0
        
        return -1

    def event_add_row(self):
        print 'Add row...'
        index = self.out_candi_row
        ones_value = self.out_candirow_ones

        self.event_size += self.event_width
        self.event_height += 1
        self.event_ones += ones_value
        self.event_den = self.event_ones / self.event_size

        # get new out candidate
        self.out_value2rowset, self.out_candi_row, self.out_candirow_ones =\
                self.after_deletion(self.out_value2rowset, ones_value, index, 1)

        # new in row candidate
        self.in_value2rowset, self.in_candi_row, self.in_candirow_ones =\
                self.after_addition(self.in_value2rowset, ones_value, index, -1)

        # get new out column candidate 
        if not self.out_value2colset: #it is possible that out column set is empty. must?
            self.out_candi_col = -1
            self.out_candicol_ones = -1
        else:
            self.out_value2colset, self.out_candi_col, self.out_candicol_ones =\
                    self.get_new_col_candidate(self.out_value2colset, 1, index, 1)

        # get new in column candidate
        self.in_value2colset, self.in_candi_col, self.in_candicol_ones =\
                self.get_new_col_candidate(self.in_value2colset, 1, index, -1)


    def event_del_row(self): # the most common action
        print 'Delete row...'
        index = self.in_candi_row
        ones_value = self.in_candirow_ones

        # obtain the new values for the basic attributes
        self.event_size -= self.event_width
        self.event_height -= 1
        self.event_ones -= ones_value
        self.event_den = self.event_ones / self.event_size

        self.in_value2rowset, self.in_candi_row, self.in_candirow_ones =\
                self.after_deletion(self.in_value2rowset, ones_value, index, -1)

        self.out_value2rowset, self.out_candi_row, self.out_candirow_ones =\
                self.after_addition(self.out_value2rowset, ones_value, index, 1)

        if not self.out_value2colset: #it is possible that out column set is empty. must?
            self.out_candi_col = -1
            self.out_candicol_ones = -1
        else:
            self.out_value2colset, self.out_candi_col, self.out_candicol_ones =\
                    self.get_new_col_candidate(self.out_value2colset, -1, index, 1)

        self.in_value2colset, self.in_candi_col, self.in_candicol_ones =\
                self.get_new_col_candidate(self.in_value2colset, -1, index, -1)


    def after_deletion(self, v2set, key, index, status):
        v2set[key].remove(index)
        if not v2set[key]: # empty set
            del v2set[key]
            if status < 0:
                i = min(v2set.keys())
            else:
                i = max(v2set.keys())
            candi = v2set[i].pop()
            v2set[i].add(candi)
        else: # this condition holds most of the time, which is efficient
            candi = v2set[key].pop()
            v2set[key].add(candi) # must
            i = key
        return (v2set, candi, i)
    

    def after_addition(self, v2set, key, index, status):
        try:
            v2set[key].add(index)
        except:
            v2set[key] = set([index])
        if status < 0:
            i = min(v2set.keys())
        else:
            i = max(v2set.keys())
        candi = v2set[i].pop()
        v2set[i].add(candi)
        return (v2set, candi, i)


    def get_new_col_candidate(self, v2set, factor, rindex, status):
        tmpdict = dict()
        for v in v2set:
            for col in v2set[v]:
                new_value = v + factor * self.bmatrix[rindex, col]
                try:
                    tmpdict[new_value].add(col)
                except:
                    tmpdict[new_value] = set([col])
        if status < 0:
            i = min(tmpdict.keys())
        else:
            i = max(tmpdict.keys())
        candi_col = tmpdict[i].pop()
        tmpdict[i].add(candi_col)
        return (tmpdict, candi_col, i)


    def event_add_col(self):
        print 'Add column...'
        index = self.out_candi_col
        ones_value = self.out_candicol_ones

        self.event_size += self.event_height
        self.event_width += 1
        self.event_ones += ones_value
        self.event_den = self.event_ones / self.event_size


        # new in candidate col
        self.in_value2colset, self.in_candi_col, self.in_candicol_ones =\
                self.after_addition(self.in_value2colset, ones_value, index, -1)

        # get new out candidate
        self.out_value2colset[ones_value].remove(index)
        if not self.out_value2colset[ones_value]:
            del self.out_value2colset[ones_value]
            if not self.out_value2colset: # it is possible that out column set is empty
                self.out_candi_col = -1
                self.out_candicol_ones = -1
            else:
                large = max(self.out_value2colset.keys())
                self.out_candi_col = self.out_value2colset[large].pop()
                self.out_value2colset[large].add(self.out_candi_col)
                self.out_candicol_ones = large
        else:
            self.out_candi_col = self.out_value2colset[ones_value].pop()
            self.out_value2colset[ones_value].add(self.out_candi_col) # must

        # get new out row candidate
        if not self.out_value2rowset:
            self.out_candi_row = -1
            self.out_candirow_ones = -1
        else:
            self.out_value2rowset, self.out_candi_row, self.out_candirow_ones =\
                    self.get_new_row_candidate(self.out_value2rowset, 1, index, 1)

        # get new in row candidate
        self.in_value2rowset, self.in_candi_row, self.in_candirow_ones =\
                self.get_new_row_candidate(self.in_value2rowset, 1, index, -1)


    def event_del_col(self):
        print 'Delete column...'
        index = self.in_candi_col
        ones_value = self.in_candicol_ones

        self.event_size -= self.event_height
        self.event_width -= 1
        self.event_ones -= ones_value
        self.event_den = self.event_ones / self.event_size

        # get new in col candidate
        self.in_value2colset, self.in_candi_col, self.in_candicol_ones =\
                self.after_deletion(self.in_value2colset, ones_value, index, -1)

        # get new out col candidate
        self.out_value2colset, self.out_candi_col, self.out_candicol_ones =\
                self.after_addition(self.out_value2colset, ones_value, index, 1)

        # get new out row candidate
        if not self.out_value2rowset:
            self.out_candi_row = -1
            self.out_candirow_ones = -1
        else:
            self.out_value2rowset, self.out_candi_row, self.out_candirow_ones =\
                    self.get_new_row_candidate(self.out_value2rowset, -1, index, 1)

        # get new in row candidate
        self.in_value2rowset, self.in_candi_row, self.in_candirow_ones =\
                self.get_new_row_candidate(self.in_value2rowset, -1, index, -1)


    def get_new_row_candidate(self, v2set, factor, cindex, status):
        tmpdict = dict()
        for v in v2set:
            for row in v2set[v]:
                new_value = v + factor * self.bmatrix[row, cindex]
                try:
                    tmpdict[new_value].add(row)
                except:
                    tmpdict[new_value] = set([row])
        if status < 0:
            i = min(tmpdict.keys())
        else:
            i = max(tmpdict.keys())
        candi_row = tmpdict[i].pop()
        tmpdict[i].add(candi_row)
        return (tmpdict, candi_row, i)


    def event_rm_line_ronly(self, index): # do not remove column any more
        self.event_size -= self.event_width
        self.event_height -= 1
        self.event_ones -= self.row_ones[index]
        self.event_den = self.event_ones / self.event_size

        del self.in_rows[index]
        del self.row_ones[index]
        del self.row_weight[index]

    def event_rm_line(self, index, option):
        if option is 0:
            self.event_size -= self.event_width
            self.event_height -= 1
            self.event_ones -= self.row_ones[index]
            self.event_den = self.event_ones / self.event_size

            one_indexes = []
            for j in self.in_cols:
                if self.bmatrix[index,j]: # XXX why 'is 1' does not work?
                    one_indexes.append(j)
                    self.col_ones[j] -= 1
                    self.col_weight[j] -= self.row_ones[index]
            
            #print one_indexes

            del self.in_rows[index]
            for r in self.in_rows:
                for i in one_indexes:
                    self.row_weight[r] -= self.bmatrix[r,i]

            # to save memory
            del self.row_ones[index]
            del self.row_weight[index]

        elif option is 1:
            self.event_size -= self.event_height
            self.event_width -= 1
            self.event_ones -= self.col_ones[index]
            self.event_den = self.event_ones / self.event_size

            one_indexes = []
            for j in self.in_rows:
                if self.bmatrix[j,index]:
                    one_indexes.append(j)
                    self.row_ones[j] -= 1
                    self.row_weight[j] -= self.col_ones[index]
            
            del self.in_cols[index]
            for c in self.in_cols:
                for i in one_indexes:
                    self.col_weight[c] -= self.bmatrix[i,c]

            # to save memory
            del self.col_ones[index]
            del self.col_weight[index]

        else:
            assert False


    def get_dict_min_list(self, mydict, keylist):
        if keylist is None:
            keylist = mydict.keys()

        mylist = list()
        small = 9999999999

        for k in keylist:
            value = mydict[k]
            if value == small:
                mylist.append(k)
            elif value < small:
                small = value
                mylist = [k]

        return mylist

    def get_dict_max_list(self, mydict, keylist):
        if keylist is None:
            keylist = mydict.keys()

        mylist = list()
        max = -999999999

        for k in keylist:
            value = mydict[k]
            if value == max:
                mylist.append(k)
            elif value > max:
                max = value
                mylist = [k]

        return mylist

    def get_events_list(self):
        event_dict = dict()

        path = self.get_output_dir_event() + self.events_brief_fname
        f = open(path, 'r')
        for line in f:
            line = line.rstrip('\n')
            unix_dt = int(line.split(':')[0])
            content = line.split(':')[1]
            thelist = ast.literal_eval(content)
            event_dict[unix_dt] = thelist
        f.close()

        return event_dict

    def all_events_cluster(self):
        pfx_set_dict = dict()
        mon_set_dict = dict()

        event_dict = self.get_events_list()
        for unix_dt in event_dict:

            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.get_output_dir_event() + str(unix_dt) + '.txt'
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

                JD = 0.9 * JD_p + 0.1 * JD_m # XXX note the parameters

                #the_list.append(JD_p)
                the_list.append(JD)

            d_matrix.append(the_list)


        the_ndarray = np.array(d_matrix)

        print unix_dt_list
        print d_matrix
        db = DBSCAN(eps=0.7, min_samples=4, metric='precomputed').fit(the_ndarray)
        print db.core_sample_indices_
        print db.components_
        print db.labels_

        outpath = self.events_cluster_path()
        f = open(outpath, 'w')
        for label in db.labels_:
            f.write(str(label)+'|')
        f.close()

    def events_cluster_path(self):
        return self.get_output_dir_event() + 'clustering.txt'

    def detect_event(self):
        #------------------------------------
        # XXX for getting the sizes of all slots
        analyzed_dt = set()
        dt2event = self.get_events_list()
        for dt in dt2event:
            self.dt2size[dt] = dt2event[dt][0]
            analyzed_dt.add(dt)

        print self.dt2size

        #self.filegroups = self.filegroups[17:] # FIXME test
        for fg in self.filegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            #------------------------------------
            # XXX for getting the sizes of all slots
            if unix_dt in analyzed_dt:
                continue

            #if unix_dt != 1229733600: # test
            #    continue

            #reset size and width thresholds to cope with collector blank period
            self.thre_width = self.mo_number * self.width_ratio
            self.thre_size = self.size_ratio * self.pfx_number * self.mo_number # recommand: 0.5%

            fstart = unix_dt
            fend = unix_dt + self.granu * 60    
            for item in self.blank_info:
                start = item[0]
                end = item[1]
                mcount = item[2]

                if fstart >= start and fend <= end:
                    self.thre_width -= mcount * self.width_ratio
                    self.thre_size -= mcount * self.size_ratio * self.pfx_number

            logging.info('thre_width=%f,thre_size=%f',self.thre_width,self.thre_size)

            #read a middle file into c_pfx_data
            for f in fg:
                self.read_a_file_event(self.middle_dir+f)

            # convert integer lists to binary lists just before preprocessing
            blists = list()
            cindex = 0
            for pfx in self.c_pfx_data:
                self.index2pfx[cindex] = pfx
                blist = []
                ilist = self.c_pfx_data[pfx]
                for value in ilist:
                    if value > 0:
                        blist.append(1)
                    else:
                        blist.append(0)
                blists.append(blist)
                cindex += 1


            print 'Identifying event...'
            self.bmatrix = np.array(blists)
            #self.analyze_bmatrix(unix_dt) # old algorithm
            #code = self.analyze_bmatrix_new(unix_dt) # new algorithm
            code = self.analyze_bmatrix_plusminus(unix_dt)

            '''
            if code == 100: # found an event
                col_set = set()
                row_set = set()
                for v in self.in_value2colset:
                    for c in self.in_value2colset[v]:
                        col_set.add(c)
                for v in self.in_value2rowset:
                    for r in self.in_value2rowset[v]:
                        row_set.add(r)

                fname = str(unix_dt) + '.txt'
                f = open(self.get_output_dir_event()+fname, 'w')
                f.write('MonitorIndexes#' + str(col_set) + '\n')
                for r in row_set:
                    pfx = self.index2pfx[r]
                    f.write(pfx+':'+str(self.c_pfx_data[pfx])+'\n')
                f.close()
            '''

            # release memory 
            del self.bmatrix

            del self.in_value2rowset
            self.in_value2rowset = dict()

            del self.out_value2rowset
            self.out_value2rowset = dict()

            del self.in_value2colset
            self.in_value2colset = dict()

            del self.out_value2colset
            self.out_value2colset = dict()

            del self.c_pfx_data
            self.c_pfx_data = dict()

            del self.index2pfx
            self.index2pfx = dict()

            self.out_candirow_ones = -1
            self.out_candicol_ones = -1
            self.in_candirow_ones = -1
            self.in_candicol_ones = -1
            self.out_candi_row = -1
            self.out_candi_col = -1
            self.in_candi_row = -1
            self.in_candi_col = -1

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

            ##rnode = self.c_pfx_data.search_exact(pfx)
            ##if rnode is None:
            ##    rnode = self.c_pfx_data.add(pfx)
            ##    rnode.data[0] = datalist
            ##else:
            ##    c_datalist = rnode.data[0]
            ##    combined = [x+y for x,y in zip(datalist, c_datalist)]
            ##    rnode.data[0] = combined

            try:
                c_datalist = self.c_pfx_data[pfx]
                combined = [x+y for x,y in zip(datalist, c_datalist)]
                self.c_pfx_data[pfx] = combined
            except:
                self.c_pfx_data[pfx] = datalist


        fin.close()

    def get_output_dir_event(self):
        s = str(self.size_ratio)
        tmp = s.index('.')
        s = s[tmp+1:]
        w = str(self.width_ratio)
        tmp = w.index('.')
        w = w[tmp+1:]

        mydir = self.final_dir + s + '_' + w +\
                '_' + str(self.thre_den).lstrip('0.') + '_' + str(self.m_granu) +\
                '_' + str(self.granu) + '_' + str(self.shift) + '/'
        cmlib.make_dir(mydir)

        return mydir

    def get_output_fpath_event_brief(self):
        return self.get_output_dir_event() + self.events_brief_fname

    def output_event(self):
        '''
        output_path = self.get_output_fpath_event_brief()
        
        f = open(output_path, 'w')
        for dt in self.events:
            f.write(str(dt)+':'+str(self.events[dt])+'\n')
        f.close()
        '''

        all_size_path = self.get_output_dir_event() + 'all_slot_size.txt'
        f = open(all_size_path, 'w')
        for dt in self.dt2size:
            f.write(str(dt)+':'+str(self.dt2size[dt])+'\n')
        f.close()
            
    def all_events_tpattern(self): # time patterns of all events
        event_dict = self.get_events_list()
        dt_denlist = dict() 
        for unix_dt in event_dict:
            event_size = event_dict[unix_dt][1]
            event_den = event_dict[unix_dt][2]

            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()


            slot_num = 4 # number of time slots before and after the event
            #-----------------------------------
            # determine and read the middle files
            head = unix_dt - self.granu * 60 * slot_num 
            tail = unix_dt + self.granu * 60 * slot_num
    
            if head < int(self.filegroups[0][0].rstrip('.txt.gz')) or\
                    tail > int(self.filegroups[-1][0].rstrip('.txt.gz')):
                print 'cannot analyze the time pattern of the event'
                continue

            # file group list ordered by datetime
            unix_set = set()
            for i in xrange(0, 2*slot_num+1):
                unix_set.add(head + self.granu*60*i)

            fg_list = list() # it is supposed to be ordered
            for fg in self.filegroups:
                if int(fg[0].rstrip('.txt.gz')) in unix_set:
                    fg_list.append(fg)

            # -----------------------------------------
            # obtain density list
            den_list = list() # density list ordered by time
            for fg in fg_list:
                if int(fg[0].rstrip('.txt.gz')) == unix_dt: # current event datetime
                    den_list.append(event_den)
                    continue

                pfx_int_data = dict()
                for fname in fg:
                    floc = self.middle_dir + fname
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


                ones_in_num = 0
                for pfx in pfx_set:
                    for index in mon_set:
                        try:
                            if pfx_int_data[pfx][index] > 0:
                                ones_in_num += 1
                        except: # no such pfx or mon
                            pass

                cden = float(ones_in_num) / float(event_size)
                den_list.append(cden)

            print den_list
            dt_denlist[unix_dt] = den_list

            f = open(self.events_tpattern_path(), 'w')
            for dt in dt_denlist: 
                f.write(str(dt)+':'+str(dt_denlist[dt])+'\n')
            f.close()

    def events_tpattern_path(self):
        return self.get_output_dir_event() + 'time_pattern.txt'
