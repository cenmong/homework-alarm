import patricia
import datetime
import time as time_lib
import numpy as np
import cmlib

from netaddr import *
from env import *

class Alarm():

    def __init__(self, granu, sdate, active_t, cl_list, thres, soccur,\
                eoccur, des):
        # for scheduling date time order
        self.cl_list = cl_list
        self.cl_dt = {}  # collector: [from_dt, now_dt] 
        for cl in self.cl_list:
            self.cl_dt[cl] = [0, 0]  # start dt, now dt
        self.ceiling = 0  # we aggregate everything before ceiling
        self.floor = 0  # for not recording the lowest dt

        self.sdate = sdate
        self.granu = granu  # Time granularity in minutes
        self.active_t = active_t # active threshold

        self.thres = thres
        self.soccur = soccur
        self.eoccur = eoccur
        self.des = des

        '''
        self.pfx2as = None  # all pfxes in the globe
        # here stores info about global ASes
        self.as2nation = dict() # asn: nation
        self.as2type = dict() # asn: type
        self.as2rank = dict() # asn: rank (2012 datasource)
        # here stores info about nations
        self.nation2cont = dict() # nation: continent

        self.peerlist = dict()  # dt: peer list
        self.peeraslist = dict() # dt: peer AS list

        self.actas_c = dict() # dt: origin AS(of DAPs) count
        self.actnation_c = dict() # dt: origin nation(od DAPs) count
        
        self.acount = dict() # dt: announcement count
        self.wcount = dict() # dt: withdrawl count
        self.wpctg = dict() # dt: withdrawl percentage 
        '''

        # from now on is what we are really interested in, or what we can plot
        # figures from.

        self.pfx_trie = dict()  # dt: trie. (garbage deletion target)
        self.act_c = dict() # dt: active prefix count

        self.dt_list = list()

        self.ucount = dict() # dt: update count
        self.pfxcount = dict() # dt: prefix (in updates) count

        for dr in daterange:
            if dr[0] == self.sdate:
                self.mcount = dr[2]
        if self.mcount == -1:
            self.mcount = cmlib.get_monitor_c(self.sdate) # monitor count

        #self.busy_cont_bypfx = dict() # dt: the busiest continent by DAP
        #self.busy_cont_byas = dict() # dt: the busiest continent by AS
        #self.cont2num = {'EU':1,'NA':2,'AS':3,'SA':4,'OC':5,'AF':6}

        '''
        # distribution skewness 
        # TODO: more statistics needed
        self.pfx_as_top10 = dict() # dt: top 10 ASes
        self.pfx_nation_top10 = dict()
        self.pfx_as_top10pctg = dict() # dt: top 10% ASes
        self.pfx_nation_top10pctg = dict()

        # TODO: nation and continent

        # get origin AS rank levels (among several pre-setted levels)
        self.rank_thsd = [100, 1000] # threshold for classifying ASes
        self.rank_count = [] # class(0~): {dt: count}
        for i in xrange(0, len(self.rank_thsd)+1):
            self.rank_count.append(dict())
        '''

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

        # For CDF
        self.cdf = dict()

        # for naming the plots 
        self.describe_add = self.sdate+'_'+str(self.granu)+'_'+str(self.active_t)+'_'

    def check_memo(self, is_end):
        if self.ceiling == 0:  # not everybofy is ready
            return 0
    
        # We are now sure that all collectors exist and any info that is 
        # too early to be combined are deleted

        new_ceil = 9999999999
        for cl in self.cl_list:
            if self.cl_dt[cl][1] < new_ceil:
                new_ceil = self.cl_dt[cl][1]

        if is_end == False:
            if new_ceil - self.ceiling >= 4 * 60 * self.granu:  # not so frequent
                # e.g., aggregate 10:50 only when now > 11:00
                self.ceiling = new_ceil - 60 * self.granu
                self.release_memo()
        else:
            self.ceiling = new_ceil - 60 * self.granu
            self.release_memo()

        return 0

    # aggregate everything before ceiling and remove garbage
    def release_memo(self):
        print 'deciding dt to release...'
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if self.floor <= dt <= self.ceiling:
                #cmlib.print_dt(dt)
                rel_dt.append(dt)
        # Put major businesses here
        self.aggregate(rel_dt)

        self.del_garbage()
        return 0

    # delete large and unaggregated memory usage
    def del_garbage(self):
        print 'Deleting garbage...'
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                #cmlib.print_dt(dt)
                del self.pfx_trie[dt]
        return 0

    def add(self, update):
        attr = update.split('|')[0:6]  # no need for other attrs now

        intdt = int(attr[1])
        objdt = datetime.datetime.fromtimestamp(intdt).\
                replace(second = 0, microsecond = 0) +\
                datetime.timedelta(hours=-8) # note the 8H shift

        # Reset time to fit granularity
        mi = self.xia_qu_zheng(objdt.minute, 'm')
        objdt = objdt.replace(minute = mi)
        dt = time_lib.mktime(objdt.timetuple())  # Change into seconds int


        if dt not in self.dt_list: # a brand new dt for sure!
            self.dt_list.append(dt)
            # initialization
            #self.peerlist[dt] = []
            #self.peeraslist[dt] = []
            self.ucount[dt] = 0
            self.pfx_trie[dt] = patricia.trie(None)
    
            '''
            self.acount[dt] = 0
            self.wcount[dt] = 0
            for i in xrange(0, len(self.rank_thsd)+1):
                self.rank_count[i][dt] = 0

        # get and record update type and number
        ty = attr[2]
        if ty == 'A':
            self.acount[dt] += 1
        else: # 'W'
            self.wcount[dt] += 1
        '''
        self.ucount[dt] += 1

        peer = attr[3]
        '''
        # fullfill the peerlist
        if peer not in self.peerlist[dt]:
            self.peerlist[dt].append(peer)

        # fullfill the peeraslist
        peeras = int(attr[4])
        if peeras not in self.peeraslist[dt]:
            self.peeraslist[dt].append(peeras)
        '''    

        # now let's deal with the prefix -- the core mission!
        pfx = cmlib.ip_to_binary(attr[5], peer)
        try:
            try:  # Test whether the trie has the node
                test = self.pfx_trie[dt][pfx]
            except:  # Node does not exist
                self.pfx_trie[dt][pfx] = [peer]
            if peer not in self.pfx_trie[dt][pfx]:
                self.pfx_trie[dt][pfx].append(peer)
        except: # this self.pfx_trie[dt] has been deleted
            pass

        return 0

    def aggregate(self, rel_dt):
        print 'aggregating...'
        for dt in rel_dt:
            # TODO
            trie = self.pfx_trie[dt]
            pcount = 0
            #as_list = [] # list of origin ASes in this dt
            #nation_list = [] # list of origin nations in this dt
            for i in xrange(0, len(self.dvi)):
                self.dvi[i][dt] = 0

            #pfx_as_distri = {} # ASN: pfx list
            #pfx_nation_distri = {} # nation: pfx list
            for p in trie:
                if p == '':
                    continue
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
                if ratio <= self.active_t: # not active pfx
                    continue
                pcount += 1

                '''
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
                    if p not in pfx_nation_distri[nation]:
                        pfx_nation_distri[nation].append(p)
                '''

                # a bunch of DVIs
                self.dvi[0][dt] += ratio - self.active_t
                self.dvi[1][dt] += np.power(2, (ratio-0.9)*10)
                self.dvi[2][dt] += np.power(5, (ratio-0.9)*10)
                self.dvi[3][dt] += ratio
                self.dvi[4][dt] += 1

            self.act_c[dt] = pcount
            self.pfxcount[dt] = len(trie)

            '''
            self.actas_c[dt] = len(as_list)
            self.actnation_c[dt] = len(nation_list)

            # get rank levels of origin ASes
            for item in as_list:
                rank = self.as_to_rank(item)
                if rank == -1: # no rank found
                    continue
                for i in xrange(0, len(self.rank_thsd)):
                    if rank <= self.rank_thsd[i]: # find rank!
                        self.rank_count[i][dt] += 1
                        break
                # in last rank
                self.rank_count[len(self.rank_thsd)][dt] += 1
            
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
            '''

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
            cmlib.direct_simple_plot(self.active_t, self.granu,\
                    self.describe_add+name, self.thres,\
                    self.soccur, self.eoccur, self.des)
        for name in array2:
            cmlib.direct_simple_plot(self.active_t, self.granu,\
                    self.describe_add+name, self.thres,\
                    self.soccur, self.eoccur, self.des)
        for name in array3:
            cmlib.direct_cdf_plot(self.active_t, self.granu,\
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
            cmlib.simple_plot(self.active_t, self.granu, self.dvi[i], self.describe_add+self.dvi_desc[i])

        '''
        # plot peer count
        peercount = {}
        for key in self.peerlist.keys():
            peercount[key] = len(self.peerlist[key])
        cmlib.simple_plot(self.active_t, self.granu, peercount, self.describe_add+'peercount')

        # plot peer AS count
        peerascount = {}
        for key in self.peeraslist.keys():
            peerascount[key] = len(self.peeraslist[key])
        cmlib.simple_plot(self.active_t, self.granu, peerascount, self.describe_add+'peerAScount')
        '''

        # active pfx count
        cmlib.simple_plot(self.active_t, self.granu, self.act_c, self.describe_add+'act_pfx_count')

        # plot interested levels
        self.plot_level(10, 80)

        '''
        cmlib.simple_plot(self.active_t, self.granu, self.actas_c, self.describe_add+'originAS(act_pfx)count')
        cmlib.simple_plot(self.active_t, self.granu, self.actnation_c, self.describe_add+'State(active_pfx)count')

        # top 10 AS and State
        cmlib.simple_plot(self.active_t, self.granu, self.pfx_as_top10,\
                self.describe_add+'pfx_ratio_of_top10_originAS(active)')
        cmlib.simple_plot(self.active_t, self.granu, self.pfx_nation_top10,\
                self.describe_add+'pfx_ratio_of_top10_originState(active)')

        # top 10% AS and State
        cmlib.simple_plot(self.active_t, self.granu, self.pfx_as_top10pctg,\
                self.describe_add+'pfx_ratio_of_top10%_originAS(active)')
        cmlib.simple_plot(self.active_t, self.granu, self.pfx_nation_top10pctg,\
                self.describe_add+'pfx_ratio_of_top10%_originState(active)')

        # different levels of origin AS ranks
        sign = 'rank_level_'
        for item in self.rank_thsd:
            sign = sign + str(item) + '_'
        sign += '_'
        for i in xrange(0, len(self.rank_thsd)+1):
            cmlib.simple_plot(self.active_t, self.granu, self.rank_count[i], self.describe_add+sign+str(i+1))

        # announcement withdrawal update prefix count
        cmlib.simple_plot(self.active_t, self.granu, self.acount, self.describe_add+'announce_count')
        cmlib.simple_plot(self.active_t, self.granu, self.wcount, self.describe_add+'withdraw_count')
        cmlib.simple_plot(self.active_t, self.granu, self.wpctg, self.describe_add+'withdraw_percentage')
        '''

        # total update and prefix count
        cmlib.simple_plot(self.active_t, self.granu, self.ucount, self.describe_add+'update_count')
        cmlib.simple_plot(self.active_t, self.granu, self.pfxcount, self.describe_add+'prefix_count')

        # CDF in introduction
        cmlib.cdf_plot(self.active_t, self.granu, self.cdf, self.describe_add+'CDF')

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
            cmlib.simple_plot(self.active_t, self.granu, self.level[key], self.describe_add+'='+str(key))

    '''
    def pfx_to_as(self, mypfx):
        if self.pfx2as == None:
            self.pfx2as = patricia.trie(None)

            pfx2as_file = ''
            tmp = os.listdir(hdname+'topofile/'+self.sdate+'/')
            for line in tmp:
                if 'pfx2as' in line:
                    pfx2as_file = line
                    break

            f = open(hdname+'topofile/'+self.sdate+'/'+pfx2as_file)
            for line in f:
                print line
                line = line.rstrip('\n')
                attr = line.split()
                if '_' in attr[2] or ',' in attr[2]:
                    continue
                pfx = cmlib.ip_to_binary(attr[0]+'/'+attr[1], '0.0.0.0')
                try:
                    self.pfx2as[pfx] = int(attr[2]) # pfx: origin AS
                except:
                    self.pfx2as[pfx] = -1
            f.close()
        # We already have a global trie
        try:
            asn = self.pfx2as[mypfx]
            return asn
        except:  # no corresponding ASN
            return -1

    def as_to_nation(self, myasn):
        if self.as2nation == {}:
            f = open(hdname+'topofile/as2nation.txt')
            for line in f:
                self.as2nation[int(line.split()[0])] = line.split()[1]
            f.close()
   
        # We already have as2nation database
        try:
            return self.as2nation[myasn]
        except:
            return -1

    def as_to_type(self, myasn): # TODO: this is based on old data (2004) :(
        if self.as2type == {}:
            f = open(hdname+'topofile/as2attr.txt')
            for line in f:
                line = line.strip('\n')
                self.as2type[int(line.split()[0])] = line.split()[-1]
            f.close()
   
        # We already have as2type database
        try:
            return self.as2type[myasn]
        except:
            return -1

    def as_to_rank(self, myasn):
        if self.as2rank == {}:
            f = open(hdname+'topofile/asrank_20121102.txt')
            for line in f:
                line = line.strip('\n')
                self.as2rank[int(line.split()[1])] = int(line.split()[0])
            f.close()
   
        # We already have as2rank database
        try:
            return self.as2rank[myasn]
        except:
            return -1

    def nation_to_cont(self, mynation):
        if self.nation2cont == {}:
            f = open(hdname+'topofile/continents.txt')
            for line in f:
                line = line.strip('\n')
                self.nation2cont[line.split(',')[0]] = line.split(',')[1]
            f.close()
   
        # We already have as2rank database
        try:
            return self.nation2cont[mynation]
        except:
            return -1
    '''    

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
        self.cl_dt[cl][1] = int(line.split('|')[1]) - 28800 # -8 Hours
        return 0
    
    def set_first(self, cl, first_line):
        self.cl_dt[cl][0] = int(first_line.split('|')[1]) - 28800
        non_zero = True
        for cl in self.cl_list:
            if self.cl_dt[cl][0] == 0:
                non_zero = False
        if non_zero == True:  # all cl has file exist
            for cl in self.cl_list:
                if self.cl_dt[cl][0] > self.ceiling:
                    self.ceiling = self.cl_dt[cl][0]
                    self.floor = self.shang_qu_zheng(self.ceiling, 's')
            # delete everything before floor
            self.del_garbage()
        return 0

