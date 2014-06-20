import patricia
import datetime
import time as time_lib
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
import cmlib

from netaddr import *
from env import *
from matplotlib.dates import HourLocator

class Alarm():

    def __init__(self, granu, sdate, active_t, cl_list):
        # for scheduling date time order
        self.cl_list = cl_list
        self.cl_dt = {}  # collector: [from_dt, now_dt] 
        for cl in self.cl_list:
            self.cl_dt[cl] = [0, 0]  # start dt, now dt
        self.ceiling = 0  # we aggregate everything before ceiling
        self.floor = 0  # for not recording the lowest dt

        self.pfx2as = None  # all pfxes in the globe
        self.as2state = dict() # asn: state
        self.as2type = dict() # asn: type
        self.as2rank = dict() # asn: rank (2012 datasource)

        self.sdate = sdate
        self.granu = granu  # Time granularity in minutes
        self.active_t = active_t # active threshold

        self.pfx_trie = dict()  # dt: trie
        self.peerlist = dict()  # dt: peer list
        self.act_c = dict() # dt: DAP count
        self.actas_c = dict() # dt: AS(of DAPs) count
        self.actstate_c = dict() # dt: state(od DAPs) count

        # TODO: some detailed info
        # self.as_detail = dict() # dt: {AS: [state, No. of pfx]}

        # Dynamic Visibility Index
        self.dvi = []  # DVI No.: dt: value
        for i in xrange(0, 5):
            self.dvi.append({})
        self.dvi_desc = {} # DVI No.: describe
        self.dvi_desc[0] = 'dvi(ratio-threshold)' # div No.: describe
        self.dvi_desc[1] = 'dvi(2^(ratio-0.9)*10)' # div No.: describe
        self.dvi_desc[2] = 'dvi(5^(ratio-0.9)*10)' # div No.: describe
        self.dvi_desc[3] = 'dvi(1)' # div No.: describe
        self.dvi_desc[4] = 'dvi(ratio)' # div No.: describe

        # diff levels of visibility, from 0~10 to 90~100 and 100
        self.level = dict() # level(e.g.,>=0,>=10,>=20,...): dt: value
        for i in xrange(0, 101):
            if i % 10 == 0:
                self.level[i] = dict()

    def add(self, update):
        attr = update.split('|')[0:6]  # no need for other attrs now

        intdt = int(attr[1])
        objdt = datetime.datetime.fromtimestamp(intdt).\
                replace(second = 0, microsecond = 0) +\
                datetime.timedelta(hours=-8) # note the 8H shift

        # Reset time to fit granularity
        mi = self.xia_qu_zheng(objdt.minute, 'm')
        objdt = objdt.replace(minute = mi)
        intdt = time_lib.mktime(objdt.timetuple())  # Change into seconds int

        # fullfill the peerlist
        peer = attr[3]
        if intdt not in self.peerlist.keys():
            self.peerlist[intdt] = []
        if peer not in self.peerlist[intdt]:
            self.peerlist[intdt].append(peer)

        # now let's deal with the prefix
        pfx = cmlib.ip_to_binary(attr[5], peer)
        if intdt not in self.pfx_trie.keys():
            self.pfx_trie[intdt] = patricia.trie(None)
        try:  # Test whether the trie has the node
            test = self.pfx_trie[intdt][pfx]
        except:  # Node does not exist
            self.pfx_trie[intdt][pfx] = [peer]
            return 0

        if peer not in test:
            self.pfx_trie[intdt][pfx].append(peer)

        return 0

    def aggregate(self, rel_dt):
        for dt in rel_dt:
            len_all_peer = len(self.peerlist[dt])
            trie = self.pfx_trie[dt]
            pcount = 0
            as_list = []
            state_list = []
            for i in xrange(0, 5):
                self.dvi[i][dt] = 0

            for p in trie:
                if p == '':
                    continue
                ratio = len(trie[p])*10 / len_all_peer * 10 # 0,10,...,90,100
                for lv in self.level.keys():
                    if ratio >= lv: # e.g., 20 >= 0, 10, 20
                        try:
                            self.level[lv][dt] += 1 
                        except:
                            self.level[lv][dt] = 1

                ratio = float(len(trie[p]))/float(len_all_peer)
                # only count active ones from now on
                if ratio <= self.active_t: # not active pfx
                    continue
                pcount += 1
                asn = self.pfx_to_as(p)
                if asn not in as_list:
                    as_list.append(asn)
                state = self.as_to_state(asn)
                if state not in state_list:
                    state_list.append(state)

                # a bunch of shit
                    self.dvi[0][dt] += ratio - self.active_t
                    self.dvi[1][dt] += np.power(2, (ratio-0.9)*10)
                    self.dvi[2][dt] += np.power(5, (ratio-0.9)*10)
                    self.dvi[3][dt] += 1
                    self.dvi[4][dt] += ratio

            self.act_c[dt] = pcount
            self.actas_c[dt] = len(as_list)
            self.actstate_c[dt] = len(state_list)

        return 0

    def plot(self): # plot everything here!
        # plot all DVIs
        describe_add = self.sdate+'_'+str(self.granu)+'_'+str(self.active_t)+'_'
        for i in xrange(0, len(self.dvi)):
            cmlib.simple_plot(self.dvi[i], describe_add+self.dvi_desc[i])

        # plot interested levels
        self.plot_level(10, 90, describe_add) # plot 20 ~ 70

        # plot peer count
        peercount = {}
        for key in self.peerlist.keys():
            peercount[key] = len(self.peerlist[key])
        cmlib.simple_plot(peercount, describe_add+'peercount')

        # active pfx count
        cmlib.simple_plot(self.act_c, describe_add+'act_pfx_count')
        cmlib.simple_plot(self.actas_c, describe_add+'originAS(act_pfx)count')
        cmlib.simple_plot(self.actstate_c, describe_add+'State(active_pfx)count')

    def plot_level(self, low, high, describe_add):
        # fill the empty values with 0
        dtlist = self.peerlist.keys()
        for key in self.level.keys():
            for dt in dtlist:
                try:
                    test = self.level[key][dt]
                except:
                    self.level[key][dt] = 0
        
        for key in self.level.keys():
            if key < low or key > high:
                continue
            cmlib.simple_plot(self.level[key], describe_add+'>='+str(key))

    def pfx_to_as(self, my_pfx):
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
                line = line.rstrip('\n')
                attr = line.split()
                if '_' in attr[2] or ',' in attr[2]:
                    continue
                pfx = cmlib.ip_to_binary(attr[0]+'/'+attr[1], '0.0.0.0')
                self.pfx2as[pfx] = int(attr[2]) # pfx: origin AS
            f.close()

        # We already have a global trie
        try:
            asn = self.pfx2as[my_pfx]
            return asn
        except:  # no corresponding ASN
            return -1

    def as_to_state(self, my_asn):
        if self.as2state == {}:
            f = open(hdname+'topofile/as2state.txt')
            for line in f:
                self.as2state[int(line.split()[0])] = line.split()[1]
            f.close()
   
        # We already have as2state database
        try:
            return self.as2state[my_asn]
        except:
            return -1

    def as_to_type(self, myasn): # old data (2004)
        if self.as2type == {}:
            f = open(hdname+'topofile/as2attr.txt')
            for line in f:
                line = line.strip('\n')
                self.as2type[int(line.split()[0])] = line.split()[-1]
            f.close()
   
        # We already have as2type database
        try:
            return self.as2state[my_asn]
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
            return self.as2rank[my_asn]
        except:
            return -1
        

    # aggregate everything before ceiling and remove garbage
    def release_memo(self):
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if self.floor <= dt <= self.ceiling:
                #cmlib.print_dt(dt)
                rel_dt.append(dt)

        # Put major businesses here
        self.aggregate(rel_dt)

        self.del_garbage()
        return 0

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
            if new_ceil - self.ceiling >= 1 * 60 * self.granu:  # not so frequent
                # e.g., aggregate 10:50 only when now > 11:00
                self.ceiling = new_ceil - 60 * self.granu
                self.release_memo()
        else:
            self.ceiling = new_ceil - 60 * self.granu
            self.release_memo()

        return 0

    def del_garbage(self):
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                #cmlib.print_dt(dt)
                del self.pfx_trie[dt]
        return 0
