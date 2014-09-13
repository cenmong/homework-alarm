import patricia
import datetime
import time as time_lib
import numpy as np
import cmlib
import operator

from netaddr import *
from env import *
from supporter_class import *

class Alarm():

    def __init__(self, granu, sdate, hthres, cl_list, dthres, cdfbound):
        ##############################################
        # For coordinating different collectors
        #################################################
        self.cl_list = cl_list
        self.cl_dt = {}  # collector: [from_dt, now_dt] 
        for cl in self.cl_list:
            self.cl_dt[cl] = [0, 0]  # start dt, now dt
        self.ceiling = 0  # we aggregate everything before ceiling
        self.floor = 0  # for not recording the lowest dt


        ###########################
        # Basic values assignment
        ############################
        self.sdate = sdate # Starting date
        self.granu = granu  # Time granularity in minutes
        self.hthres = hthres # active threshold, also known as \theta_h
        self.dthres = dthres # detection threshold, also known as \theta_d
        
        self.dt_list = list() # the list of datetime
        self.peerlist = dict() # dt: monitor list
        self.pfx_trie = dict() # every dt has a corresponding trie, deleted periodically
        self.hdvp_count = dict() # dt: active prefix count
        self.ucount = dict() # dt: update count
        self.pfxcount = dict() # dt: prefix (in updates) count
        self.acount = dict() # dt: announcement count
        self.wcount = dict() # dt: withdrawal count
        self.wpctg = dict() # dt: withdrawal percentage 

        spt = Supporter(sdate)
        self.pfx2as = spt.get_pfx2as_trie()  # all prefixes to AS in a trie
        self.as2nation = spt.get_as2nation_dict() # all ASes to origin nation (latest)
        self.all_pcount = cmlib.get_all_pcount(self.sdate) # Get total prefix count
        self.as2cc = spt.get_as2cc_dict()  # all ASes to size of customer cones

        self.as2rank = dict() # AS:rank by customer cone
        pre_value = 999999
        rank = 0
        for item in sorted(self.as2cc.iteritems(), key=operator.itemgetter(1), reverse=True):
            if item[1] < pre_value:
                rank += 1
                pre_value = item[1]
            self.as2rank[item[0]] = rank
        print self.as2rank


        ###########################################
        # Get data about monitors
        ######################################
        self.monitors = cmlib.get_monitors(self.sdate) # monitors ip: AS number
        self.mcount = len(self.monitors.keys())

        self.m_as_m = dict() # AS number: monitor count
        self.m_nation_as = dict() # nation: AS (of monitors) count

        for m in self.monitors.keys():
            asn = self.monitors[m]
            try:
                self.m_as_m[asn] += 1
            except:
                self.m_as_m[asn] = 1

        for asn in self.m_as_m.keys():
            nation = self.as_to_nation(asn)
            if nation == -1:
                continue
            try:
                self.m_nation_as[nation] += 1
            except:
                self.m_nation_as[nation] = 1

        self.m_ascount = len(self.m_as_m.keys())
        print 'monitor AS count:', self.m_ascount
        self.m_nationcount = len(self.m_nation_as.keys())
        print 'monitor nation count:', self.m_nationcount
        print 'nations:', self.m_nation_as.keys()


        ############################################
        # Dynamic Visibillity Index
        #######################################
        self.dvi = []  # DVI No.: dt: value
        self.dvi_desc = {} # DVI No.: describe
        for i in xrange(0, 5): # control total number of DVIs here
            self.dvi.append({})
        self.dvi_desc[0] = 'dvi(ratio-threshold)' # div No.: describe
        self.dvi_desc[1] = 'dvi(2^(ratio-0.9)_10)'
        self.dvi_desc[2] = 'dvi(5^(ratio-0.9)_10)'
        self.dvi_desc[3] = 'dvi(ratio)'
        self.dvi_desc[4] = 'dvi(1)'


        ####################################################
        # Values according to diffrent Dynamic Visibilities
        ################################################
        self.dv_level = [0, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3]
        # depicts prefix-length for different ratio levels (>0, >5, >10~50)
        self.dv_len_pfx = dict() # DV levels: prefix length: existence
        self.dvrange_len_pfx = dict() # DV levels range: prefix length: existence
        self.dv_dt_hdvp = dict() # DV levels: dt: hdvp count
        self.dv_pfx = dict() # DV levels: DV: prefix count
        self.dv_asn_hdvp = dict() # DV levels (threshold_h): AS: hdvp count
        self.dup_trie = dict() # DV levels: trie (node store pfx existence times)
        self.dup_as = dict() # DV levels: AS: prefix existence times
        for dl in self.dv_level:
            self.dv_len_pfx[dl] = dict()
            self.dvrange_len_pfx[dl] = dict()
            self.dv_dt_hdvp[dl] = dict()
            self.dv_pfx[dl] = dict()
            self.dv_asn_hdvp[dl] = dict()
            self.dup_trie[dl] = patricia.trie(None)
            self.dup_as[dl] = dict()


        ###################################################
        # CDFs for the slot before and after the cdfbound (HDVP peak)
        ##################################################
        self.compare = False
        if cdfbound != None:
            self.compare = True

            self.cdfbfr = dict()
            self.cdfaft = dict()
            ## cdfbound must be like xy:z0 (multiply of self.granu minutes)
            ## cdfbound should be the start time of the HDVP peak
            ## 2003 Slammer Worm: 2003-01-25 05:30:00
            ## 2008 second cable cut: 2008-12-19 07:30:00
            ## 2013 spamhaus DDoS attack: 2013-03-20 09:00:00
            self.cdfbound = datetime.datetime.strptime(cdfbound, '%Y-%m-%d %H:%M:%S')
            self.bfr_start = time_lib.mktime((self.cdfbound +\
                    datetime.timedelta(minutes=-self.granu)).timetuple())
            self.aft_end = time_lib.mktime((self.cdfbound +\
                    datetime.timedelta(minutes=self.granu)).timetuple())
            self.cdfbound = time_lib.mktime(self.cdfbound.timetuple())


        ##### For naming all the figures.
        self.describe_add = self.sdate+'_'+str(self.granu)+'_'+str(self.hthres)+'_'

    def check_memo(self, is_end):
        if self.ceiling == 0:  # not everybody is ready
            return 0
    
        # We are now sure that all collectors exist and any info that is 
        # too early to be combined are deleted

        new_ceil = 9999999999
        for cl in self.cl_list:
            if self.cl_dt[cl][1] < new_ceil:
                new_ceil = self.cl_dt[cl][1]

        if is_end == False:
            if new_ceil - self.ceiling >= 2 * 60 * self.granu:  # frequent
                # e.g., aggregate everything <= 10:50 only when now > 11:00
                self.ceiling = new_ceil - 60 * self.granu
                self.release_memo()
        else:
            self.ceiling = new_ceil - 60 * self.granu
            self.release_memo()

        return 0

    # aggregate everything before ceiling and remove garbage
    def release_memo(self):
        print 'Releasing memory...'
        rel_dt = []  # target dt list
        for dt in self.pfx_trie.keys():
            if self.floor <= dt <= self.ceiling:
                rel_dt.append(dt)

        self.aggregate(rel_dt)

        self.del_garbage()
        return 0

    # delete the tires that have already been used
    def del_garbage(self):
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                del self.pfx_trie[dt]
        return 0

    # add a new line of update to our monitoring system
    def add(self, update):
        attr = update.split('|')[0:6]  # no need for other attributes now

        if ':' in attr[5]: # IPv6 # TODO: should put in analyzer
            return -1

        if len(attr[5]) == 1: # I don't know why they exist
            return -1

        intdt = int(attr[1])
        objdt = datetime.datetime.fromtimestamp(intdt).\
                replace(second = 0, microsecond = 0) +\
                datetime.timedelta(hours=-8) # note the 8H shift

        # Reset date time to fit granularity
        mi = self.xia_qu_zheng(objdt.minute, 'm')
        objdt = objdt.replace(minute = mi)
        dt = time_lib.mktime(objdt.timetuple())  # Change into seconds int

        # meet a brand new dt for sure!
        if dt not in self.dt_list:
            self.dt_list.append(dt)
            self.peerlist[dt] = []
            self.pfx_trie[dt] = patricia.trie(None)
            self.ucount[dt] = 0
            self.acount[dt] = 0
            self.wcount[dt] = 0


        # record update type and number
        if attr[2] == 'A':  # announcement
            self.acount[dt] += 1
        else:  # withdrawal
            self.wcount[dt] += 1
        self.ucount[dt] += 1

        peer = attr[3]
        if peer not in self.peerlist[dt]:
            self.peerlist[dt].append(peer)

        # deal with the prefix -- the core mission!
        pfx = cmlib.ip_to_binary(attr[5], peer)
        try:
            try:  # Test whether the trie has the node
                pfx_peer = self.pfx_trie[dt][pfx]
            except:  # Node does not exist, then we create a new node
                self.pfx_trie[dt][pfx] = [peer]
                pfx_peer = self.pfx_trie[dt][pfx]
            if peer not in pfx_peer:
                self.pfx_trie[dt][pfx].append(peer)
        except:  # self.pfx_trie[dt] has already been deleted
            pass

        return 0


    # get/calculate the infomation we need from the designated tries
    def aggregate(self, rel_dt):

        for dt in rel_dt:
            trie = self.pfx_trie[dt]
            hdvp_count = 0
            for i in xrange(0, len(self.dvi)):
                self.dvi[i][dt] = 0

            dv_asn_hdvp_tmp = dict()
            for dl in self.dv_level:
                dv_asn_hdvp_tmp[dl] = {} # used only at first detection point

            for p in trie:
                if p == '': # the root node (the source of a potential bug)
                    continue

                ratio = float(len(trie[p]))/float(self.mcount)

                plen = len(p) # get prefix length
                asn = self.pfx_to_as(p) # get origin AS number

                for i in xrange(0, len(self.dv_level)):
                    dv_now = self.dv_level[i]
                    if ratio > dv_now:
                        try:
                            self.dv_pfx[dv_now][ratio] += 1
                        except:
                            self.dv_pfx[dv_now][ratio] = 1

                        try:
                            self.dv_len_pfx[dv_now][plen] += 1
                        except:
                            self.dv_len_pfx[dv_now][plen] = 1
                            
                        if asn != -1:
                            try:
                                dv_asn_hdvp_tmp[dv_now][asn] += 1
                            except:
                                dv_asn_hdvp_tmp[dv_now][asn] = 1

                        try:
                            self.dv_dt_hdvp[dv_now][dt] += 1 
                        except:
                            self.dv_dt_hdvp[dv_now][dt] = 1

                        try:
                            self.dup_trie[dv_now][p] += 1
                        except:  # Node does not exist, then we create a new node
                            self.dup_trie[dv_now][p] = 1

                        if i != len(self.dv_level)-1: # not the last one
                            if ratio <= self.dv_level[i+1]:
                                try:
                                    self.dvrange_len_pfx[dv_now][plen] += 1
                                except:
                                    self.dvrange_len_pfx[dv_now][plen] = 1
                        else: # the last one
                            try:
                                self.dvrange_len_pfx[dv_now][plen] += 1
                            except:
                                self.dvrange_len_pfx[dv_now][plen] = 1

                if self.compare == True:
                    ### Only for CDF comparison
                    if dt >= self.bfr_start and dt < self.cdfbound:
                        try:
                            self.cdfbfr[ratio] += 1
                        except:
                            self.cdfbfr[ratio] = 1
                    elif dt >= self.cdfbound and dt < self.aft_end:
                        try:
                            self.cdfaft[ratio] += 1
                        except:
                            self.cdfaft[ratio] = 1
                    else:
                        pass


                # only count HDVPs from now on
                if ratio <= self.hthres: # not active pfx
                    continue
                hdvp_count += 1

                # a bunch of DVIs
                self.dvi[0][dt] += ratio - self.hthres
                self.dvi[1][dt] += np.power(2, (ratio-0.9)*10)
                self.dvi[2][dt] += np.power(5, (ratio-0.9)*10)
                self.dvi[3][dt] += ratio
                self.dvi[4][dt] += 1

             
            if float(hdvp_count)/float(self.all_pcount) >= self.dthres:
                if self.dv_asn_hdvp[0] == {}: # hasn't been filled
                    self.dv_asn_hdvp = dv_asn_hdvp_tmp 

            self.hdvp_count[dt] = hdvp_count
            self.pfxcount[dt] = len(trie)

            # get withdrawal/(W+A) value
            self.wpctg[dt] = float(self.wcount[dt]) / float(self.acount[dt] + self.wcount[dt])

        return 0

    def direct_plot(self, dthres, soccur, eoccur, desc): # this is called before everybody!
        # polt from intial data directly
        # get name of the interested data and divide into categories according
        # to diffrent polting needs
        array1 = []
        array2 = []
        array3 = []
        array1.append(self.dvi_desc[4]) # the DVI we've decided
        array2.append('update_count')
        array2.append('prefix_count')
        array3.append('CDF')

        # Now, let's rock and roll
        for name in array1:
            cmlib.direct_ts_plot(self.hthres, self.granu,\
                    self.describe_add+name, dthres,\
                    soccur, eoccur, desc)
        for name in array2:
            cmlib.direct_ts_plot(self.hthres, self.granu,\
                    self.describe_add+name, dthres,\
                    soccur, eoccur, desc)
        for name in array3:
            cmlib.direct_cdf_plot(self.hthres, self.granu,\
                    self.describe_add+name)

    def plot(self): # plot everything here!
        print 'Plotting everything...'

        # devide DVIs by total prefix count
        for i in xrange(0, len(self.dvi)):
            for key_dt in self.dvi[i].keys():
                value = self.dvi[i][key_dt]
                self.dvi[i][key_dt] = float(value)/float(self.all_pcount)

            cmlib.time_series_plot(self.hthres, self.granu, self.dvi[i], self.describe_add+self.dvi_desc[i])


        cmlib.time_series_plot(self.hthres, self.granu, self.hdvp_count, self.describe_add+'act_pfx_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.acount, self.describe_add+'announce_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wcount, self.describe_add+'withdraw_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wpctg, self.describe_add+'withdraw_percentage')
        cmlib.time_series_plot(self.hthres, self.granu, self.ucount, self.describe_add+'update_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.pfxcount, self.describe_add+'prefix_count')

        # different DV levels
        for dl in self.dv_level:
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.dv_pfx[dl]),\
                    self.describe_add+'CDF-DV-pfx-'+str(dl))
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.dv_len_pfx[dl]),\
                    self.describe_add+'CDF-len-pfx-'+str(dl))
            cmlib.cdf_plot(self.hthres, self.granu, self.symbol_count2cdf(self.dv_asn_hdvp[dl]),\
                    self.describe_add+'CDF-AS-HDVP-'+str(dl))
            cmlib.store_symbol_count(self.hthres, self.granu, self.dv_asn_hdvp[dl],\
                    self.describe_add+'CDF-AS-HDVP-'+str(dl))

            for dt in self.dt_list:
                try:
                    test = self.dv_dt_hdvp[dl][dt]
                except:
                    self.dv_dt_hdvp[dl][dt] = 0

            cmlib.time_series_plot(self.hthres, self.granu, self.dv_dt_hdvp[dl], self.describe_add+'='+str(dl))

        #cmlib.box_plot_grouped(self.hthres, self.granu, self.dvrange_len_pfx[dl],\
                #self.describe_add+'box-dv-len-'+str(dl))

        if self.compare:
            # plot 2 CDFs: before event and after event
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.cdfbfr),\
                   self.describe_add+'CDFbfr')
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.cdfaft),\
                   self.describe_add+'CDFaft')


        # NOTE: no plotting from now on
        # the ASs that generate many prefixes who exist for multiple times
        for dl in self.dv_level:
            for pfx in self.dup_trie[dl]:
                if self.dup_trie[dl][pfx] > 1:
                    asn = self.pfx_to_as(pfx)
                    try:
                        self.dup_as[dl][asn] += 1
                    except:
                        self.dup_as[dl][asn] = 1
                else: # not interested in existnce == 1
                    del self.dup_trie[dl][pfx]

        f = open(hdname+'output/'+self.sdate+'_'+str(self.granu)+'_'+str(self.hthres)+\
                 '/'+self.describe_add+'_dup_AS.txt', 'w')
        for dl in self.dup_as.keys():  # all dt that exists
            f.write(str(dl)+':')
            for asn in self.dup_as[dl]:
                if self.dup_as[dl][asn] > 1:
                    f.write(str(asn)+'|'+str(self.dup_as[dl][asn])+',')
            f.write('\n')
        f.close()

        # show multiple existence of prefixes whose DV > x, y, ...
        f = open(hdname+'output/'+self.sdate+'_'+str(self.granu)+'_'+str(self.hthres)+\
                 '/'+self.describe_add+'_dup_pfx.txt', 'w')
        for dl in self.dup_trie.keys():  # all dt that exists
            f.write(str(dl)+':')
            for pfx in self.dup_trie[dl]:
                if self.dup_trie[dl][pfx] > 1:
                    f.write(pfx+'|'+str(self.dup_trie[dl][pfx])+',')
            f.write('\n')
        f.close()

        return 0

    def value_count2cdf(self, vc_dict): # dict keys are contable values
        cdf = dict()
        xlist = [0]
        ylist = [0]

        for key in sorted(vc_dict): # sort by key
            xlist.append(key)
            ylist.append(vc_dict[key])

        ## y is the number of values that <= x
        for i in xrange(1, len(ylist)):
            ylist[i] += ylist[i-1]

        ## change y into percentage
        giant = ylist[-1] # the largest y value
        if giant == 0: # no value actually
            return {1:1}
        for i in xrange(0, len(ylist)):
            cdf[xlist[i]] = ylist[i]

        return cdf

    def symbol_count2cdf(self, sc_dict): # dict keys are just symbols
        cdf = dict()
        xlist = [0]
        ylist = [0]

        j = 0
        # large to small
        for item in sorted(sc_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
            j += 1
            xlist.append(j)
            ylist.append(item[1])

        for i in xrange(1, len(ylist)):
            ylist[i] += ylist[i-1]

        for i in xrange(0, len(ylist)):
            cdf[xlist[i]] = ylist[i]

        return cdf

    def pfx_to_as(self, mypfx):
        try:
            asn = self.pfx2as[mypfx]
            return asn
        except:
            return -1

    def as_to_nation(self, myasn):
        try:
            return self.as2nation[myasn]
        except:
            return -1

    def as_to_rank(self, myasn):
        try:
            return self.as2rank[myasn]
        except:
            return -1

    def shang_qu_zheng(self, value, tp):  # 'm': minute, 's': second
        if tp == 's':
            return (value + 60 * self.granu) / (60 * self.granu) * (60 *\
                        self.granu)
        elif tp == 'm':
            return (value + self.granu) / self.granu * self.granu
        else:
             return False 

    def xia_qu_zheng(self, value, tp):
        if tp == 's':
            return value / (60 * self.granu) * (60 *\
                        self.granu)
        elif tp == 'm':
            return value / self.granu * self.granu
        else:
            return False

    def set_now(self, cl, line):
        self.cl_dt[cl][1] = int(line.split('|')[1]) - 28800 # must -8 Hours
        return 0
    
    def set_start(self, cl, first_line):
        self.cl_dt[cl][0] = int(first_line.split('|')[1]) - 28800
        non_zero = True # True if all collectors have started
        for cl in self.cl_list:
            if self.cl_dt[cl][0] == 0:
                non_zero = False
        if non_zero == True:
            for cl in self.cl_list:
                if self.cl_dt[cl][0] > self.ceiling:
                    self.ceiling = self.cl_dt[cl][0]
                    self.floor = self.shang_qu_zheng(self.ceiling, 's')
            # delete everything before floor
            self.del_garbage()
        return 0

