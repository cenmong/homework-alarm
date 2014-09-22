import patricia
import datetime
import time as time_lib
import numpy as np
import cmlib
import myplot
import operator

from netaddr import *
from env import *
from supporter_class import *

def alarmplot(sdate, granu):
    print 'Plotting...'

    output_dir = hdname+'output/'+sdate+'_'+str(granu)+'/'
    # Good! DV > 0
    myplot.mean_cdf(output_dir+'dv_distribution.txt', 'Dynamic Visibility',\
            'prefix ratio (DV > 1)')
    # Good! Not range!
    myplot.mean_cdfs_multi(output_dir+'as_distribution.txt',\
            'AS count', 'prefix ratio (DV > x)') # in multiple figures
    # Good! Range!
    myplot.cdfs_one(output_dir+'prefix_length_cdf.txt', 'prefix length',\
            'prefix ratio (DV in ranges)') # CDF curves in one figure
    # Good! DV > 0
    myplot.cdfs_one(output_dir+'dv_cdf_bfr_aft.txt', 'Dynamic Visibililty',\
            'prefix ratio (DV > 1)')
    '''
    for dl in self.dv_level:
        # Number!(?)
        myplot.cdfs_one(output_dir+'event_as_cdfs_'+str(dl)+'.txt',\
                'AS count', 'prefix ratio')
    # Number!
    myplot.boxes(output_dir+'high_dv.txt', 'DV ranges', 'prefix number') # boxes in one figure (range)
    myplot.time_values_one(output_dir+'high_dv.txt')
    myplot.time_value(output_dir+'announce_count.txt')
    myplot.time_value(output_dir+'withdraw_count.txt')
    myplot.time_value(output_dir+'update_count.txt')
    myplot.time_value(output_dir+'prefix_count.txt')
    # Number!
    myplot.time_values_one(output_dir+'HDVP.txt')
    '''
    return 0

class Alarm():

    def __init__(self, granu, sdate, cl_list, dthres, cdfbound):
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
        self.dthres = dthres # detection threshold, also known as \theta_d
        
        self.dt_list = list() # the list of datetime
        self.peerlist = dict() # dt: monitor list
        self.pfx_trie = dict() # every dt has a corresponding trie, deleted periodically
        self.ucount = dict() # dt: update count
        self.acount = dict() # dt: announcement count
        self.wcount = dict() # dt: withdrawal count
        self.wpctg = dict() # dt: withdrawal percentage 

        spt = Supporter(sdate)
        self.pfx2as = spt.get_pfx2as_trie()  # all prefixes to AS in a trie
        self.as2nation = spt.get_as2nation_dict() # all ASes to origin nation (latest)
        self.all_ascount = cmlib.get_all_ascount(self.sdate) # Get total AS count
        self.all_pcount = cmlib.get_all_pcount(self.sdate) # DV >= 0
        self.all_pcount_lzero = 0 # DV > 0
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


        self.dv_level = [0, 0.05, 0.1, 0.15, 0.2]
        ###################################################
        # Coarser DV values
        ######################################################
        self.dvrange_dt_pfx = dict() # DV level range: dt: pfx count
        self.dvrange_len_pfx = dict() # DV level range: prefix length: existence
        self.dv_dt_asn_pfx = dict() # DV levels: dt: AS: prefix count
        self.pfxcount = dict() # dv: dt: prefix (in updates) count
        self.pfxcount_range = dict() # dv range: dt: prefix (in updates) count
        self.dv_dt_hdvp = dict() # DV levels: dt: hdvp count
        for dl in self.dv_level:
            self.dvrange_dt_pfx[dl] = dict()
            self.dvrange_len_pfx[dl] = dict()
            self.dv_dt_asn_pfx[dl] = dict()
            self.pfxcount[dl] = dict()
            self.pfxcount_range[dl] = dict()
            self.dv_dt_hdvp[dl] = dict()

        
        # only record DV > 0.15
        self.dup_trie = patricia.trie(None) # Enough memory for this?

        ###################################################
        # CDFs for the slot before and after the cdfbound (HDVP peak)
        ##################################################
        self.compare = False
        if cdfbound != None:
            self.compare = True

            self.cdfbfr = dict()
            self.cdfaft = dict()
            self.as_bfr = dict()
            self.as_aft = dict()
            self.cdfbound = datetime.datetime.strptime(cdfbound, '%Y-%m-%d %H:%M:%S')
            self.bfr_start = time_lib.mktime((self.cdfbound +\
                    datetime.timedelta(minutes=-(self.granu*2))).timetuple())
            self.cdfbound = time_lib.mktime(self.cdfbound.timetuple())

            for dl in self.dv_level:
                self.as_bfr[dl] = dict() # dv: ASN: count
                self.as_aft[dl] = dict()

        ###########################################
        # Output
        #############################################
        self.output_dir = hdname+'output/'+self.sdate+'_'+str(self.granu)+'/'
        cmlib.make_dir(self.output_dir)


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
        attr = update.split('|')[0:6]  # Get 0~5

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
            self.dv_distribution[dt] = dict() # DV: count

            for dl in self.dv_level:
                self.dv_dt_asn_pfx[dl][dt] = dict()

            for pfx in trie:
                if pfx == '': # the root node (the source of a potential bug)
                    continue

                plen = len(pfx) # get prefix length
                asn = self.pfx_to_as(pfx) # get origin AS number
                ratio = float(len(trie[pfx]))/float(self.mcount)

                try:
                    self.dv_distribution[dt][ratio] += 1
                except:
                    self.dv_distribution[dt][ratio] = 1

                if ratio > 0.15:
                    try:
                        self.dup_trie[pfx] += 1
                    except:  # Node does not exist, then we create a new node
                        self.dup_trie[pfx] = 1

                for j in xrange(0, len(self.dv_level)):
                    dv_now = self.dv_level[j]
                    if ratio > dv_now:
                        try:
                            self.pfxcount[dv_now][dt] += 1
                        except:
                            self.pfxcount[dv_now][dt] = 1

                        if j != len(self.dv_level)-1: # not the last one
                            if ratio <= self.dv_level[j+1]:
                                try:
                                    self.dvrange_dt_pfx[dv_now][dt] += 1  # DV range: dt: pfx count
                                except:
                                    self.dvrange_dt_pfx[dv_now][dt] = 1  # DV range: dt: pfx count
                                try:
                                    self.dvrange_len_pfx[dv_now][plen] += 1
                                except:
                                    self.dvrange_len_pfx[dv_now][plen] = 1
                                try:
                                    self.pfxcount_range[dv_now][dt] += 1
                                except:
                                    self.pfxcount_range[dv_now][dt] = 1
                        else: # the last one
                            try:
                                self.dvrange_dt_pfx[dv_now][dt] += 1  # DV range: dt: pfx count
                            except:
                                self.dvrange_dt_pfx[dv_now][dt] = 1  # DV range: dt: pfx count
                            try:
                                self.dvrange_len_pfx[dv_now][plen] += 1
                            except:
                                self.dvrange_len_pfx[dv_now][plen] = 1
                            try:
                                self.pfxcount_range[dv_now][dt] += 1
                            except:
                                self.pfxcount_range[dv_now][dt] = 1

                        try:
                            self.dv_dt_hdvp[dv_now][dt] += 1 
                        except:
                            self.dv_dt_hdvp[dv_now][dt] = 1

                        if asn != -1:
                            try:
                                self.dv_dt_asn_pfx[dv_now][dt][asn] += 1
                            except:
                                self.dv_dt_asn_pfx[dv_now][dt][asn] = 1

                            if self.compare == True and dt == self.bfr_start:
                                try:
                                    self.as_bfr[dv_now][asn] += 1
                                except:
                                    self.as_bfr[dv_now][asn] = 1

                            if self.compare == True and dt == self.cdfbound:
                                try:
                                    self.as_aft[dv_now][asn] += 1
                                except:
                                    self.as_aft[dv_now][asn] = 1


                if self.compare == True:
                    ### CDF comparison before and after event
                    if dt == self.bfr_start:
                        try:
                            self.cdfbfr[ratio] += 1
                        except:
                            self.cdfbfr[ratio] = 1
                    if dt == self.cdfbound:
                        try:
                            self.cdfaft[ratio] += 1
                        except:
                            self.cdfaft[ratio] = 1


            # Only get top 500 ASes
            for dl in self.dv_level:
                tmp_dict = self.dv_dt_asn_pfx[dl][dt]
                tmp_list = sorted(tmp_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
                try:
                    tmp_list = tmp_list[0:500]
                except: # item number < 500 or empty
                    pass
                self.dv_dt_asn_pfx[dl][dt] = {}
                for item in tmp_list:
                    # value is the ratio of "prefixes in updates"
                    self.dv_dt_asn_pfx[dl][dt][item[0]] =\
                            float(item[1])/float(self.pfxcount[dl][dt])

            # get withdrawal/(W+A) value
            self.wpctg[dt] = float(self.wcount[dt]) / float(self.ucount[dt])

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
            myplot.direct_ts_plot(self.granu,\
                    name, dthres,\
                    soccur, eoccur, desc)
        for name in array2:
            myplot.direct_ts_plot(self.granu,\
                    name, dthres,\
                    soccur, eoccur, desc)
        for name in array3:
            myplot.direct_cdf_plot(self.granu,\
                    name)

    def output(self):
        for dt in self.pfxcount[0].keys():
            self.all_pcount_lzero += self.pfxcount[0][dt]

        ##################################################
        # Record the most basic information
        #####################################################
        f = open(self.output_dir+'basic.txt', 'w')
        f.write('Monitor #: '+str(self.mcount))
        f.write('Monitor AS: '+str(self.m_as_m))
        f.write('Monitor nation: '+str(self.m_nation_as))
        f.write('# of AS: '+str(self.all_ascount))
        f.write('# of prefix: '+str(self.all_pcount))
        f.close()

        ###################################################
        # DV distribution: mean and standard deviation
        # dv,mean,deviation|dv ...
        #####################################################
        print 'Recording DV distribution...'
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

        f = open(self.output_dir+'dv_distribution.txt', 'w')
        for dv in all_dv_list:
            mean = dv_mean_dev[dv][0]
            dev = dv_mean_dev[dv][1]
            f.write(str(dv)+','+str(mean)+','+str(dev)+'|')
        f.close()

        ###################################################
        # AS distribution: mean and standard deviation
        # NOTE: it's possible that y cannot reach 100% (we only use top 500 ASes)
        # Or because of the failure of mapping prefix to ASN
        # dv level:AS count,mean,deviation|AS count...\n dv level:...
        #####################################################
        print 'Recording AS distribution...'
        f = open(self.output_dir+'as_distribution.txt', 'w')
        fa = open(self.output_dir+'active_as.txt', 'w')

        for dl in self.dv_level:
            f.write(str(dl)+':')
            fa.write(str(dl)+':')

            active_as = dict() # dt: [[as1,count1],[as2,count2]]
            as_cdf = dict() # dt: as count: prefix count
            for dt in self.dv_dt_asn_pfx[dl].keys():
                # dv_dt_asn_pfx[dl][dt] should have 500 keys
                for a in xrange(1, 501):
                    try:
                        test = self.dv_dt_asn_pfx[dl][dt][a]
                    except:
                        self.dv_dt_asn_pfx[dl][dt][a] = 0
                as_cdf[dt] = self.symbol_count2cdf(self.dv_dt_asn_pfx[dl][dt])
                # get top 5 ASes of each time slot
                tmp_list = sorted(self.dv_dt_asn_pfx[dl][dt].iteritems(),\
                        key=operator.itemgetter(1), reverse=True)
                active_as[dt] = tmp_list[0:5] # [[ASN, pfx count ratio],...]
            for j in xrange(1, 501): # AS count from 1 to 500
                values = [] # values of all dt's
                for dt in as_cdf.keys():
                    values.append(as_cdf[dt][j])
                mean = sum(values)/len(values)
                dev = np.std(values)
                f.write(str(j)+','+str(mean)+','+str(dev)+'|')

            f.write('\n')
            # Record top 10 ASes of this DV level (all time slots)
            as_count = dict() # AS: count
            for dt in active_as.keys():
                for item in active_as[dt]:
                    try:
                        as_count[item[0]] += 1
                    except:
                        as_count[item[0]] = 1

            tmp_list = sorted(as_count.iteritems(),\
                    key=operator.itemgetter(1), reverse=True)
            tmp_list = tmp_list[0:20]
            for item in tmp_list:
                asn = item[0]
                count = item[1]
                asrank = self.as_to_rank(asn)
                nation = self.as_to_nation(asn)
                fa.write(str(asn)+','+str(count)+','+str(asrank)+','+str(nation)+'|')
            fa.write('\n') 
        f.close()
        fa.close()
                
        #################################################
        # quantity of prefixes of high DV ranges in different dt
        # dv level:dt,count|dt,count|...\n dv level...
        #######################################################
        print 'Recording quantity of HDVPs'
        f = open(self.output_dir+'high_dv.txt', 'w')
        for dl in self.dvrange_dt_pfx.keys():
            f.write(str(dl)+':')
            for dt in self.dvrange_dt_pfx[dl].keys():
                f.write(str(dt)+','+str(self.dvrange_dt_pfx[dl][dt])+'|')
            f.write('\n')

        f.close()

        #################################################
        # prefix lengthes
        # dv range:length,count|length...\n dv range...
        #######################################################
        print 'Recording length distribution'
        f = open(self.output_dir+'prefix_length_cdf.txt', 'w')
        for dl in self.dv_level:
            f.write(str(dl)+':')
            for i in xrange(1, 33):
                try:
                    test = self.dvrange_len_pfx[dl][i]
                except:
                    self.dvrange_len_pfx[dl][i] = 0
            mydict = self.value_count2cdf(self.dvrange_len_pfx[dl])
            for k in mydict.keys():
                value = mydict[k]
                total_pfx = 0
                for dt in self.pfxcount_range[dl].keys():
                    total_pfx += self.pfxcount_range[dl][dt]
                value = float(value) / total_pfx
                f.write(str(k)+','+str(value)+'|')
            f.write('\n')

        tmp_result = cmlib.get_all_length(self.sdate) # length: prefix count (all)
        all_length = tmp_result[0] # length: prefix count (all)
        tmp_pcount = tmp_result[1] # Number of all prefixes from RIB
        f.write('all:')
        for i in xrange(1, 33):
            try:
                test = all_length[i]
            except:
                all_length[i] = 0

        mydict = self.value_count2cdf(all_length)
        for k in mydict.keys():
            value = mydict[k]
            value = float(value) / float(tmp_pcount)
            f.write(str(k)+','+str(value)+'|')
        f.write('\n')

        f.close()

        ###################################
        # Plot everything about update quantity
        # dt,value|dt,value...
        ######################################
        f = open(self.output_dir+'announce_count.txt', 'w')
        for dt in self.acount.keys():
            f.write(str(dt)+','+str(self.acount[dt])+'|')
        f.close()

        f = open(self.output_dir+'withdraw_count.txt', 'w')
        for dt in self.wcount.keys():
            f.write(str(dt)+','+str(self.wcount[dt])+'|')
        f.close()

        f = open(self.output_dir+'update_count.txt', 'w')
        for dt in self.ucount.keys():
            f.write(str(dt)+','+str(self.ucount[dt])+'|')
        f.close()

        f = open(self.output_dir+'prefix_count.txt', 'w')
        for dt in self.pfxcount[0].keys():
            f.write(str(dt)+','+str(self.pfxcount[0][dt])+'|')
        f.close()

        #myplot.time_series_plot(self.granu, self.wpctg, 'withdraw_percentage')

        ###################################
        # Plot prefix count of different DV level ranges
        # dv level:dt,value|dt,value|...\n dv level...
        ######################################
        f = open(self.output_dir+'HDVP.txt', 'w')
        for dl in self.dv_level:
            f.write(str(dl)+':')
            for dt in self.dt_list:
                try:
                    value = self.dv_dt_hdvp[dl][dt]
                except:
                    value = 0
                f.write(str(dt)+','+str(value)+'|')
            f.write('\n')
        f.close()
                
        ###################################
        # Plot before and after event
        # before:dv,count|dv...\n after:dv,count|dv...
        # before:AS count,value|AS count...\n after:AS count,value|...
        ######################################
        if self.compare:
            #DV distribution
            f = open(self.output_dir+'dv_cdf_bfr_aft.txt', 'w')
            mydict_b = self.value_count2cdf(self.cdfbfr)
            f.write('before:')
            for k in mydict_b.keys():
                value = mydict_b[k]
                value = float(value) / float(self.pfxcount[0][self.bfr_start])
                f.write(str(k)+','+str(value)+'|')
            f.write('\n')

            mydict_a = self.value_count2cdf(self.cdfaft)
            f.write('after:')
            for k in mydict_a.keys():
                value = mydict_a[k]
                value = float(value) / float(self.pfxcount[0][self.cdfbound])
                f.write(str(k)+','+str(value)+'|')
            f.close()

            #AS distribution: top AS record
            for dl in self.dv_level:
                f = open(self.output_dir+'event_top_as_'+str(dl)+'.txt', 'w')
                f.write('before:')
                for item in sorted(self.as_bfr[dl].iteritems(),\
                        key=operator.itemgetter(1), reverse=True):
                    asrank = self.as_to_rank(item[0])
                    nation = self.as_to_nation(item[0])
                    f.write(str(item[0])+','+str(item[1])+','+str(asrank)+','+str(nation)+'\n')
                f.write('\n')

                f.write('after:')
                for item in sorted(self.as_aft[dl].iteritems(),\
                        key=operator.itemgetter(1), reverse=True):
                    asrank = self.as_to_rank(item[0])
                    nation = self.as_to_nation(item[0])
                    f.write(str(item[0])+','+str(item[1])+','+str(asrank)+','+str(nation)+'\n')
                f.close()

            #AS distribution: CDF record
            for dl in self.dv_level:
                dict_bfr = self.symbol_count2cdf(self.as_bfr[dl])
                dict_aft = self.symbol_count2cdf(self.as_aft[dl])
                f = open(self.output_dir+'event_as_cdfs_'+str(dl)+'.txt', 'w')
                f.write('before:')
                for k in dict_bfr.keys():
                    f.write(str(k)+','+str(dict_bfr[k])+'|')
                f.write('\n')
                f.write('after:')
                for k in dict_aft.keys():
                    f.write(str(k)+','+str(dict_aft[k])+'|')

                f.close()

        ###########################################
        # prefix DV > 15% for miltiple times ranking
        #############################################
        f = open(self.output_dir + 'dup_pfx.txt', 'w')
        f.write('0.15:\n')
        my_trie = self.dup_trie
        my_dict = {}
        for key in sorted(my_trie.iter('')):
            if key != '':
                my_dict[key] = my_trie[key]
        del my_trie

        stop = 0 # only get top 20
        for item in sorted(my_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
            stop += 1
            if stop > 20:
                break
            pfx = item[0]
            asn = self.pfx_to_as(pfx)
            asrank = self.as_to_rank(asn)
            nation = self.as_to_nation(asn)
            value = item[1]
            f.write(pfx+','+str(value)+','+str(asn)+','+str(asrank)+','+str(nation)+'\n')
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

