import patricia
import datetime
import time as time_lib
import numpy as np
import cmlib

from netaddr import *
from env import *
from supporter_class import *

class Alarm():

    def __init__(self, granu, sdate, hthres, cl_list, dthres, soccur,\
                eoccur, desc):

        # for scheduling date time order
        self.cl_list = cl_list
        self.cl_dt = {}  # collector: [from_dt, now_dt] 
        for cl in self.cl_list:
            self.cl_dt[cl] = [0, 0]  # start dt, now dt
        self.ceiling = 0  # we aggregate everything before ceiling
        self.floor = 0  # for not recording the lowest dt

        # most basic infomation about this event        
        self.sdate = sdate # Starting date
        self.granu = granu  # Time granularity in minutes
        self.hthres = hthres # active threshold, also known as \theta_h
        self.dthres = dthres # detection threshold, also known as \theta_d
        self.soccur = soccur # Event occurrence start
        self.eoccur = eoccur # Event occurrence end
        self.desc = desc # description

        # (global) related information
        supporter = supporter() # TODO: fill
        self.pfx2as = supporter.get_pfx2as_trie(self.sdate)  # all prefixes to AS in a trie
        self.as2nation = supporter.get_as2nation_dict(self.sdate)
        self.as2type = supporter.get_as2type_dict(self.sdate)
        self.as2rank = supporter.get_as2rank_dict(self.sdate)
        self.nation2cont = supporter.get_nation2cont_dict(self.sdate)  # nation: continent

        # (local) origin AS and nation information
        self.actas_c = dict() # dt: origin ASes (of HDVPs) count
        self.actnation_c = dict() # dt: origin nations (of HDVPs) count
        
        # peer, also known as monitor
        self.peerlist = dict()  # dt: peer list
        self.peeraslist = dict() # dt: peer AS list
        self.mcount = 0
        self.get_monitor()

        # the list of datetime, for better control
        self.dt_list = list()

        # every dt has a trie and some other values
        self.pfx_trie = dict()  # dt: trie. (costs memory, deleted periodically)
        self.hdvp_count = dict() # dt: active prefix count

        # Values related to the number of updates
        self.ucount = dict() # dt: update count
        self.pfxcount = dict() # dt: prefix (in updates) count
        self.acount = dict() # dt: announcement count
        self.wcount = dict() # dt: withdrawal count
        self.wpctg = dict() # dt: withdrawal percentage 

        # information about continent
        #self.busy_cont_bypfx = dict() # dt: the busiest continent by DAP
        #self.busy_cont_byas = dict() # dt: the busiest continent by AS
        #self.cont2num = {'EU':1,'NA':2,'AS':3,'SA':4,'OC':5,'AF':6}

        # TODO: CDF will be sufficient for origin as and nation
        self.as_cdf = dict()
        self.nation_cdf = dict()
        '''
        self.pfx_as_top10 = dict() # dt: top 10 ASes
        self.pfx_nation_top10 = dict()
        self.pfx_as_top10pctg = dict() # dt: top 10% ASes
        self.pfx_nation_top10pctg = dict()
        '''

        # TODO: decide better method here
        # get origin AS rank levels (among several pre-setted levels)
        self.as_rank_thres = [100, 1000] # threshold for classifying ASes
        self.rank_count = [] # class(0~): {dt: count}
        for i in xrange(0, len(self.as_rank_thres)+1):
            self.rank_count.append(dict())

        # TODO: CDF will be OK?
        # diff levels of visibility, from 0~10 to 90~100 and 100
        self.level = dict() # level(e.g.,>=0,>=10,>=20,...): dt: value
        for i in xrange(0, 101):
            if i % 10 == 0:
                self.level[i] = dict()

        # Dynamic Visibility Index
        self.dvi = []  # DVI No.: dt: value
        for i in xrange(0, 5): # control total number of DVIs here
            self.dvi.append({})
        self.dvi_desc = {} # DVI No.: describe
        self.dvi_desc[0] = 'dvi(ratio-threshold)' # div No.: describe
        self.dvi_desc[1] = 'dvi(2^(ratio-0.9)_10)' # div No.: describe
        self.dvi_desc[2] = 'dvi(5^(ratio-0.9)_10)' # div No.: describe
        self.dvi_desc[3] = 'dvi(ratio)' # div No.: describe
        self.dvi_desc[4] = 'dvi(1)' # div No.: describe

        self.all_pcount = cmlib.get_all_pcount(self.sdate)

        # For the CDF figure in introduction
        self.cdf = dict()
        ''' CDFs for 15 hours before and after the event
        self.cdfbfr = dict()
        self.cdfaft = dict()
        self.occur_dt = datetime.datetime.strptime(soccur,\
                '%Y-%m-%d %H:%M:%S')
        self.bfr_start = time_lib.mktime((self.occur_dt +\
                datetime.timedelta(hours=-15)).timetuple())
        self.aft_end = time_lib.mktime((self.occur_dt +\
                datetime.timedelta(hours=15)).timetuple())
        self.occur_dt = time_lib.mktime(self.occur_dt.timetuple())
        '''
        # for naming all the plots 
        self.describe_add = self.sdate+'_'+str(self.granu)+'_'+str(self.hthres)+'_'

    def get_monitor(self):
        # try to get the value from env.py directly
        for dr in daterange:
            if dr[0] == self.sdate:
                self.mcount = dr[2]
        # no record
        if self.mcount == -1:
            self.mcount = cmlib.get_monitor_c(self.sdate) # monitor count

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
        print 'deciding the dt list to  get info and release memory'
        rel_dt = []  # target dt list
        for dt in self.pfx_trie.keys():
            if self.floor <= dt <= self.ceiling:
                rel_dt.append(dt)

        self.aggregate(rel_dt)

        self.del_garbage()
        return 0

    # delete the tires that have already been used
    def del_garbage(self):
        print 'Deleting garbage...'
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                del self.pfx_trie[dt]
        return 0

    # add a new line of update to our monitoring system
    def add(self, update):
        attr = update.split('|')[0:6]  # no need for other attributes now

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
            self.peeraslist[dt] = []
            self.ucount[dt] = 0
            self.pfx_trie[dt] = patricia.trie(None)
            self.acount[dt] = 0
            self.wcount[dt] = 0
            for i in xrange(0, len(self.as_rank_thres)+1):
                self.rank_count[i][dt] = 0

        # record update type and number
        if attr[2] == 'A':  # announcement
            self.acount[dt] += 1
        else:  # withdrawal
            self.wcount[dt] += 1
        self.ucount[dt] += 1

        # fullfill the peerlist and peer as list
        peer = attr[3]
        if peer not in self.peerlist[dt]:
            self.peerlist[dt].append(peer)

        peeras = int(attr[4])
        if peeras not in self.peeraslist[dt]:
            self.peeraslist[dt].append(peeras)

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
        print 'aggregating...'
        for dt in rel_dt:
            trie = self.pfx_trie[dt]
            pcount = 0
            as_list = [] # list of origin ASes in this dt
            nation_list = [] # list of origin nations in this dt
            for i in xrange(0, len(self.dvi)):
                self.dvi[i][dt] = 0

            pfx_as_distri = {} # ASN: pfx list
            pfx_nation_distri = {} # nation: pfx list
            for p in trie:
                if p == '': # the root node (the source of a potential bug)
                    continue

                # TODO: modify these 7 lines. we need CDF now.
                ratio = len(trie[p])*10 / self.mcount * 10 # 0,10,...,90,100
                for lv in self.level.keys():
                    if ratio >= lv: # e.g., 20 >= 0, 10, 20
                        try:
                            self.level[lv][dt] += 1 
                        except:
                            self.level[lv][dt] = 1

                # only count active prefixes from now on
                ratio = float(len(trie[p]))/float(self.mcount)
                # For CDF plot only
                try:
                    self.cdf[ratio] += 1
                except:
                    self.cdf[ratio] = 1
                '''
                # for CDF (in introduction) comparison only
                if dt >= self.bfr_start and dt < self.occur_dt:
                    try:
                        self.cdfbfr[ratio] += 1
                    except:
                        self.cdfbfr[ratio] = 1
                elif dt >= self.occur_dt and dt < self.aft_end:
                    try:
                        self.cdfaft[ratio] += 1
                    except:
                        self.cdfaft[ratio] = 1
                else:
                    pass
                '''
                if ratio <= self.hthres: # not active pfx
                    continue
                pcount += 1

                asn = self.pfx_to_as(p)
                if asn not in as_list:
                    as_list.append(asn)
                nation = self.as_to_nation(asn)
                if nation not in nation_list:
                    nation_list.append(nation)

                # active prefix to origin AS distribution
                ori_as = self.pfx_to_as(p)
                if ori_as != -1: #really found
                    try:
                        pfxlist = pfx_as_distri[ori_as]
                    except:
                        pfx_as_distri[ori_as] = [p]
                    if p not in pfx_as_distri[ori_as]:
                        pfx_as_distri[ori_as].append(p)

                # active prefix to origin nation distribution
                nation = self.as_to_nation(ori_as)
                if nation != -1:
                    try:
                        pfxlist = pfx_nation_distri[nation]
                    except:
                        pfx_nation_distri[nation] = [p]
                        pfxlist = pfx_nation_distri[nation]
                    if p not in pfxlist:
                        pfx_nation_distri[nation].append(p)

                # a bunch of DVIs
                self.dvi[0][dt] += ratio - self.hthres
                self.dvi[1][dt] += np.power(2, (ratio-0.9)*10)
                self.dvi[2][dt] += np.power(5, (ratio-0.9)*10)
                self.dvi[3][dt] += ratio
                self.dvi[4][dt] += 1

            self.hdvp_count[dt] = pcount
            self.pfxcount[dt] = len(trie)

            self.actas_c[dt] = len(as_list)
            self.actnation_c[dt] = len(nation_list)

            # get rank levels of origin ASes
            for item in as_list:
                rank = self.as_to_rank(item)
                if rank == -1: # no rank found
                    continue
                for i in xrange(0, len(self.as_rank_thres)):
                    if rank <= self.as_rank_thres[i]: # find rank!
                        self.rank_count[i][dt] += 1
                        break
                # in last rank
                self.rank_count[len(self.as_rank_thres)][dt] += 1
            
            # TODO: get features of origin nations here!
            #self.busy_cont_byas
            #self.busy_cont_bypfx

            # get active pfx count ratio of top 10 ASes and States
            top10as_ratio = 0 
            top10nation_ratio = 0 
            try:
                for k in sorted(pfx_as_distri, key=lambda k:\
                        len(pfx_as_distri[k]), reverse=True)[:10]:
                    top10as_ratio += len(pfx_as_distri[k])
                top10as_ratio = float(top10as_ratio) / pcount
            except: # < 10
                top10as_ratio = 1
            self.pfx_as_top10[dt] = top10as_ratio

            try:
                for k in sorted(pfx_nation_distri, key=lambda k:\
                        len(pfx_nation_distri[k]), reverse=True)[:10]:
                    top10nation_ratio += len(pfx_nation_distri[k])
                top10nation_ratio = float(top10nation_ratio) / pcount
            except: # < 10
                top10nation_ratio = 1
            self.pfx_nation_top10[dt] = top10nation_ratio

            # get active pfx count ratio of top 10% ASes and States
            top10as_pctg_ratio = 0 
            top10nation_pctg_ratio = 0 
            tmp_len = len(pfx_as_distri.keys()) / 10
            if tmp_len > 0:
                for k in sorted(pfx_as_distri, key=lambda k:\
                        len(pfx_as_distri[k]), reverse=True)[:tmp_len]:
                    top10as_pctg_ratio += len(pfx_as_distri[k])
                top10as_pctg_ratio = float(top10as_pctg_ratio) / pcount
            else: # tmp_len < 0
                top10as_pctg_ratio = 0 
            self.pfx_as_top10pctg[dt] = top10as_pctg_ratio

            tmp_len = len(pfx_nation_distri.keys()) / 10
            if tmp_len > 0:
                for k in sorted(pfx_nation_distri, key=lambda k:\
                        len(pfx_nation_distri[k]), reverse=True)[:tmp_len]:
                    top10nation_pctg_ratio += len(pfx_nation_distri[k])
                top10nation_pctg_ratio = float(top10nation_pctg_ratio) / pcount
            else: # tmp_len < 0
                top10nation_pctg_ratio = 0 
            self.pfx_nation_top10pctg[dt] = top10nation_pctg_ratio

            # get withdrawal/(W+A) value
            self.wpctg[dt] = float(self.wcount[dt]) / float(self.acount[dt] + self.wcount[dt])

        return 0

    def direct_plot(self): # this is called before everybody!
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
                    self.describe_add+name, self.dthres,\
                    self.soccur, self.eoccur, self.desc)
        for name in array2:
            cmlib.direct_ts_plot(self.hthres, self.granu,\
                    self.describe_add+name, self.dthres,\
                    self.soccur, self.eoccur, self.desc)
        for name in array3:
            cmlib.direct_cdf_plot(self.hthres, self.granu,\
                    self.describe_add+name)

    def plot(self): # plot everything here!

        # devide DVIs by total prefix count
        for i in xrange(0, len(self.dvi)):
            for key_dt in self.dvi[i].keys():
                value = self.dvi[i][key_dt]
                # get the percentage
                self.dvi[i][key_dt] = float(value)/float(self.all_pcount) * 100

        # plot all DVIs
        for i in xrange(0, len(self.dvi)):
            cmlib.time_series_plot(self.hthres, self.granu, self.dvi[i], self.describe_add+self.dvi_desc[i])

        # plot peer count and peer AS count
        peercount = {}
        for key in self.peerlist.keys():
            peercount[key] = len(self.peerlist[key])
        cmlib.time_series_plot(self.hthres, self.granu, peercount, self.describe_add+'peercount')
        
        peerascount = {}
        for key in self.peeraslist.keys():
            peerascount[key] = len(self.peeraslist[key])
        cmlib.time_series_plot(self.hthres, self.granu, peerascount, self.describe_add+'peerAScount')

        # active pfx count
        cmlib.time_series_plot(self.hthres, self.granu, self.hdvp_count, self.describe_add+'act_pfx_count')

        # plot interested levels
        self.plot_level(10, 80)

        cmlib.time_series_plot(self.hthres, self.granu, self.actas_c, self.describe_add+'originAS(act_pfx)count')
        cmlib.time_series_plot(self.hthres, self.granu, self.actnation_c, self.describe_add+'State(active_pfx)count')

        # top 10 AS and State
        cmlib.time_series_plot(self.hthres, self.granu, self.pfx_as_top10,\
                self.describe_add+'pfx_ratio_of_top10_originAS(active)')
        cmlib.time_series_plot(self.hthres, self.granu, self.pfx_nation_top10,\
                self.describe_add+'pfx_ratio_of_top10_originState(active)')

        # top 10% AS and State
        cmlib.time_series_plot(self.hthres, self.granu, self.pfx_as_top10pctg,\
                self.describe_add+'pfx_ratio_of_top10%_originAS(active)')
        cmlib.time_series_plot(self.hthres, self.granu, self.pfx_nation_top10pctg,\
                self.describe_add+'pfx_ratio_of_top10%_originState(active)')

        # different levels of origin AS ranks
        sign = 'rank_level_'
        for item in self.as_rank_thres:
            sign = sign + str(item) + '_'
        sign += '_'
        for i in xrange(0, len(self.as_rank_thres)+1):
            cmlib.time_series_plot(self.hthres, self.granu, self.rank_count[i], self.describe_add+sign+str(i+1))

        # announcement withdrawal update prefix count
        cmlib.time_series_plot(self.hthres, self.granu, self.acount, self.describe_add+'announce_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wcount, self.describe_add+'withdraw_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.wpctg, self.describe_add+'withdraw_percentage')

        # total update and prefix count
        cmlib.time_series_plot(self.hthres, self.granu, self.ucount, self.describe_add+'update_count')
        cmlib.time_series_plot(self.hthres, self.granu, self.pfxcount, self.describe_add+'prefix_count')
        
        # CDF in introduction
        cmlib.cdf_plot(self.hthres, self.granu, self.cdf, self.describe_add+'CDF')

        # plot 2 CDFs: before event and after event
        cmlib.cdf_plot(self.hthres, self.granu, self.cdfbfr,\
                self.describe_add+'CDFbfr')
        cmlib.cdf_plot(self.hthres, self.granu, self.cdfaft,\
                self.describe_add+'CDFaft')

    def plot_level(self, low, high):
        # fill the empty values with 0
        for key in self.level.keys():
            for dt in self.dt_list:
                try:
                    test = self.level[key][dt]
                except:
                    self.level[key][dt] = 0
        
        for key in self.level.keys():
            if key < low or key > high:
                continue
            cmlib.time_series_plot(self.hthres, self.granu, self.level[key], self.describe_add+'='+str(key))

    def pfx_to_as(self, mypfx):
        try:
            asn = self.pfx2as[mypfx]
            return asn
        except:  # no corresponding ASN
            return -1

    def as_to_nation(self, myasn):
        try:
            return self.as2nation[myasn]
        except:
            return -1

    def as_to_type(self, myasn): # TODO: this is based on old data (2004) :(
        try:
            return self.as2type[myasn]
        except:
            return -1

    def as_to_rank(self, myasn):
        try:
            return self.as2rank[myasn]
        except:
            return -1

    def nation_to_cont(self, mynation):
        try:
            return self.nation2cont[mynation]
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

