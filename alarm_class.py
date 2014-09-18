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


        #####################################
        # DV distribution in every time slot
        ####################################
        self.dv_distribution = dict() # dt: DV: count
        self.dv_cdf = dict() # dt: DV: cumulative count


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
        self.acount = dict() # dt: announcement count
        self.wcount = dict() # dt: withdrawal count
        self.wpctg = dict() # dt: withdrawal percentage 

        spt = Supporter(sdate)
        self.pfx2as = spt.get_pfx2as_trie()  # all prefixes to AS in a trie
        self.as2nation = spt.get_as2nation_dict() # all ASes to origin nation (latest)
        # TODO: write basic info into a seperate output file
        self.all_pcount = cmlib.get_all_pcount(self.sdate) # Get total prefix count
        self.all_ascount = cmlib.get_all_ascount(self.sdate) # Get total prefix count
        self.as2cc = spt.get_as2cc_dict()  # all ASes to size of customer cones

        self.as2rank = dict() # AS:rank by customer cone
        pre_value = 999999
        rank = 0 # number (of ASes whose CC is larger) + 1
        buffer = 0 # number (of ASes having the same CC size) - 1
        for item in sorted(self.as2cc.iteritems(), key=operator.itemgetter(1), reverse=True):
            if item[1] < pre_value:
                rank = rank + buffer + 1
                pre_value = item[1]
                self.as2rank[item[0]] = rank
                buffer = 0
            else: # item[1] == pre_value
                buffer += 1
                self.as2rank[item[0]] = rank


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
        self.dvi = {}  # dt: value
        self.dvi_desc = 'DVI'


        self.dv_level = [0, 0.02, 0.05, 0.1, 0.15, 0.2, 0.25]
        ####################################################
        # Values according to diffrent Dynamic Visibilities
        ################################################
        self.dv_dt_hdvp = dict() # DV levels: dt: hdvp count
        self.dv_pfx = dict() # DV levels: DV: prefix count
        for dl in self.dv_level:
            self.dv_dt_hdvp[dl] = dict()
            self.dv_pfx[dl] = dict()


        self.dv_level2 = [0, 0.05, 0.1, 0.15, 0.2]
        ###################################################
        # Coarser DV values
        ######################################################
        self.dv_dt_pfx = dict() # DV range: dt: pfx count
        self.dup_trie = dict() # DV levels: trie (node store pfx existence times)
        self.dvrange_len_pfx = dict() # DV levels range: prefix length: existence
        self.dv_dt_asn_pfx = dict() # DV levels: dt: AS: prefix count
        self.pfxcount = dict() # dv: dt: prefix (in updates) count
        for dl in self.dv_level2:
            self.dv_dt_pfx[dl] = dict()
            self.dup_trie[dl] = patricia.trie(None) # Enough memory for this?
            self.dvrange_len_pfx[dl] = dict()
            self.dv_dt_asn_pfx[dl] = dict()
            self.pfxcount[dl] = dict()


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
            self.dvi[dt] = 0
            self.dv_distribution[dt] = dict() # DV: count

            for dl in self.dv_level2:
                self.dv_dt_asn_pfx[dl][dt] = dict()

            for pfx in trie:
                if pfx == '': # the root node (the source of a potential bug)
                    continue

                ratio = float(len(trie[pfx]))/float(self.mcount)
                try:
                    self.dv_distribution[dt][ratio] += 1
                except:
                    self.dv_distribution[dt][ratio] = 1

                plen = len(pfx) # get prefix length
                asn = self.pfx_to_as(pfx) # get origin AS number

                for i in xrange(0, len(self.dv_level)):
                    dv_now = self.dv_level[i]
                    if ratio > dv_now:
                        try:
                            self.dv_pfx[dv_now][ratio] += 1
                        except:
                            self.dv_pfx[dv_now][ratio] = 1

                        try:
                            self.dv_dt_hdvp[dv_now][dt] += 1 
                        except:
                            self.dv_dt_hdvp[dv_now][dt] = 1

                for j in xrange(0, len(self.dv_level2)):
                    dv_now = self.dv_level2[j]
                    if ratio > dv_now:
                        try:
                            self.pfxcount[dv_now][dt] += 1
                        except:
                            self.pfxcount[dv_now][dt] = 1

                        if j != len(self.dv_level2)-1: # not the last one
                            if ratio <= self.dv_level2[j+1]:
                                try:
                                    self.dv_dt_pfx[dv_now][dt] += 1  # DV range: dt: pfx count
                                except:
                                    self.dv_dt_pfx[dv_now][dt] = 1  # DV range: dt: pfx count
                                try:
                                    self.dvrange_len_pfx[dv_now][plen] += 1
                                except:
                                    self.dvrange_len_pfx[dv_now][plen] = 1
                        else: # the last one
                            try:
                                self.dv_dt_pfx[dv_now][dt] += 1  # DV range: dt: pfx count
                            except:
                                self.dv_dt_pfx[dv_now][dt] = 1  # DV range: dt: pfx count
                            try:
                                self.dvrange_len_pfx[dv_now][plen] += 1
                            except:
                                self.dvrange_len_pfx[dv_now][plen] = 1

                        try:
                            self.dup_trie[dv_now][pfx] += 1
                        except:  # Node does not exist, then we create a new node
                            self.dup_trie[dv_now][pfx] = 1

                        if asn != -1:
                            try:
                                self.dv_dt_asn_pfx[dv_now][dt][asn] += 1
                            except:
                                self.dv_dt_asn_pfx[dv_now][dt][asn] = 1


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

                # DVI
                self.dvi[dt] += ratio

            self.hdvp_count[dt] = hdvp_count

            # Only get top 1000 AS from AS distribution in this dt 
            for dl in self.dv_level2:
                tmp_dict = self.dv_dt_asn_pfx[dl][dt]
                tmp_list = sorted(tmp_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
                try:
                    tmp_list = tmp_list[0:1000]
                except: # item number < 1000 or empty
                    pass
                self.dv_dt_asn_pfx[dl][dt] = {}
                for item in tmp_list:
                    # value is the ratio of "prefixes in updates"
                    self.dv_dt_asn_pfx[dl][dt][item[0]] =\
                            float(item[1])/float(self.pfxcount[dl][dt])

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
        array1.append(self.dvi_desc) # the DVI we've decided
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
        ###################################################
        # Plot DV distribution: mean and standard deviation TODO: plot 1 fig
        #####################################################
        for dt in self.dv_distribution.keys(): # dt: DV value: prefix count 
            for dv in self.dv_distribution[dt].keys():
                self.dv_distribution[dt][dv] = \
                        float(self.dv_distribution[dt][dv]) / float(self.pfxcount[0][dt])
            # convert count to cumulative count
            self.dv_cdf[dt] = self.value_count2cdf(self.dv_distribution[dt])

        all_dv_list = [0] # all possible DVs: from 1/mcount to 1
        for i in xrange(1, self.mcount):
            all_dv_list.append(float(i)/float(self.mcount))

        dv_mean_dev = dict() # dv: [mean, deviation] for all dt's
        for dv in all_dv_list:
            values = [] # values of all dt's
            for dt in self.dv_cdf.keys():
                try:
                    my_value = self.dv_cdf[dt][dv]
                except: # no such DV as key
                    latest_dv = 0 
                    for key in self.dv_cdf[dt].keys():
                        if key > latest_dv and key < dv:
                            latest_dv = key
                    my_value = self.dv_cdf[dt][latest_dv]
                values.append(my_value)

            mean = sum(values)/len(values)
            dev = np.std(values)
            dv_mean_dev[dv] = [mean, dev]

        ###################################################
        # Plot AS distribution: mean and standard deviation TODO: plot many fig
        #####################################################
        as_mean_dev = dict() # dv: as count: [mean, dev]
        for dl in self.dv_level2:
            as_mean_dev[dl] = dict()
            as_cdf = dict() # dt: as count: prefix count
            for dt in self.dv_dt_asn_pfx[dl].keys():
                # dv_dt_asn_pfx[dl][dt] should have 1000 keys
                for a in xrange(1, 1001):
                    try:
                        test = self.dv_dt_asn_pfx[dl][dt][a]
                    except:
                        self.dv_dt_asn_pfx[dl][dt][a] = 0
                as_cdf[dt] = self.symbol_count2cdf(self.dv_dt_asn_pfx[dl][dt])
            for j in xrange(1, 1001): # AS count from 1 to 1000
                values = [] # values of all dt's
                for dt in as_cdf.keys():
                    values.append(as_cdf[dt][j])
                mean = sum(values)/len(values)
                dev = np.std(values)
                as_mean_dev[dl][j] = [mean, dev]
                
        print as_mean_dev

        #################################################
        # Box ploting prefixes of high DV ranges TODO
        #######################################################
        #cmlib.box_plot...self.dv_dt_pfx # ignore 0<x<0.05

        #################################################
        # CDF ploting prefix lengthes
        #######################################################
        #cmlib.box_plot_grouped(self.hthres, self.granu, self.dvrange_len_pfx[dl],\
                #self.describe_add+'box-dv-len-'+str(dl))
        for dl in self.dv_level2:
            for i in xrange(1, 33):
                try:
                    test = self.dvrange_len_pfx[dl][i]
                except:
                    self.dvrange_len_pfx[dl][i] = 0
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.dvrange_len_pfx[dl]),\
                    self.describe_add+'CDF-length-pfx-'+str(dl))

        all_length = cmlib.get_all_length(self.sdate) # length: prefix count (all)
        for i in xrange(1, 33):
            try:
                test = all_length[i]
            except:
                all_length[i] = 0
        cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(all_length),\
                self.describe_add+'CDF-length-all')

        ###################################
        # Plot DVI
        ######################################
        for key_dt in self.dvi.keys():
            value = self.dvi[key_dt]
            self.dvi[key_dt] = float(value)/float(self.all_pcount)
        cmlib.time_series_plot(self.hthres, self.granu, self.dvi, self.describe_add+self.dvi_desc)

        
        cmlib.time_series_plot(self.hthres, self.granu, self.hdvp_count, self.describe_add+'act_pfx_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.acount, self.describe_add+'announce_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wcount, self.describe_add+'withdraw_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wpctg, self.describe_add+'withdraw_percentage')
        cmlib.time_series_plot(self.hthres, self.granu, self.ucount, self.describe_add+'update_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.pfxcount[0], self.describe_add+'prefix_count')

        # different DV levels
        for dl in self.dv_level:
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.dv_pfx[dl]),\
                    self.describe_add+'CDF-DV-pfx-'+str(dl))

            for dt in self.dt_list:
                try:
                    test = self.dv_dt_hdvp[dl][dt]
                except:
                    self.dv_dt_hdvp[dl][dt] = 0

            cmlib.time_series_plot(self.hthres, self.granu, self.dv_dt_hdvp[dl], self.describe_add+'='+str(dl))

        if self.compare:
            # plot 2 CDFs: before event and after event
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.cdfbfr),\
                   self.describe_add+'CDFbfr')
            cmlib.cdf_plot(self.hthres, self.granu, self.value_count2cdf(self.cdfaft),\
                   self.describe_add+'CDFaft')

        ###########################################
        # Record active prefixes
        #############################################
        f = open(hdname+'output/'+self.sdate+'_'+str(self.granu)+'_'+str(self.hthres)+\
                 '/'+self.describe_add+'_dup_pfx.txt', 'w')
        for dl in self.dup_trie.keys():  # all dt that exists
            f.write(str(dl)+':')
            my_trie = self.dup_trie[dl]
            my_dict = {}
            for key in sorted(my_trie.iter('')):
                if key != '':
                    my_dict[key] = my_trie[key]
            del my_trie

            stop = 0
            for item in sorted(my_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
                stop += 1
                if stop > 10:
                    break
                pfx = item[0]
                asn = self.pfx_to_as(pfx)
                asrank = self.as_to_rank(asn)
                value = item[1]
                f.write(pfx+'|'+str(value)+'|'+str(asn)+'|'+str(asrank)+',')
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

        return cdf # NOTE: dict are not ordered. Order by key when plotting


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

        return cdf # NOTE: dict are not ordered. Order by key when plotting


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

