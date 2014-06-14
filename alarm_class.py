import patricia
import datetime
import time as time_lib
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates

from netaddr import *
from getfile import collectors
from matplotlib.dates import HourLocator

class Alarm():

    def __init__(self, granu, sdate):
        # schedule date time order
        self.cl_dt = {}  # collector: [from_dt, now_dt] 
        for cl in collectors:
            self.cl_dt[cl[0]] = [0, 0]  # start dt, now dt
        self.ceiling = 0  # ceiling is a place we can reach, not over
        self.floor = 0  # for not recording the lowest dt

        self.sdate = sdate  # For saving figures
        self.granu = granu  # Time granularity
        self.pfx_trie = dict()  # dt: pfx, from ip list
        self.fip_list = dict()  # dt: from ip list

        # aggregated info
        self.dvi1 = dict()  #  time: value
        self.dvi2 = dict()  #  time: value
        self.dvi3 = dict()  #  time: value
        self.dvi4 = dict()  #  time: value
        self.dvi5 = dict()  #  time: value

    
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

    
    def print_dt(self, dt):
        try:
            print datetime.datetime.fromtimestamp(dt)
        except:
            print dt
        return 0

    def set_now(self, cl, line):
        self.cl_dt[cl][1] = int(line.split('|')[1])
        return 0
    
    def set_first(self, cl, first_line):
        self.cl_dt[cl][0] = int(first_line.split('|')[1])
        non_zero = True
        for cl in collectors:
            if self.cl_dt[cl[0]][0] == 0:
                non_zero = False
        if non_zero == True:  # all cl has file exist
            for cl in collectors:
                if self.cl_dt[cl[0]][0] > self.ceiling:
                    self.ceiling = self.cl_dt[cl[0]][0]
                    self.floor = self.shang_qu_zheng(self.ceiling, 's')
            # del all var that are before ceiling
            # TODO: unable to completely delete
            self.del_garbage()
        return 0

    def check_memo(self, is_end):
        if self.ceiling == 0:  # not everybofy is ready
            return 0
    
        # We are now sure that all collectors exist and any info that is 
        # too early to be combined are deleted

        new_ceil = 9999999999
        # get smallest CEILING value
        for cl in collectors:
            if self.cl_dt[cl[0]][1] < new_ceil:
                new_ceil = self.cl_dt[cl[0]][1]

        print 'checking...'
        print 'self.ceiling: '
        self.print_dt(self.ceiling)
        print 'new_ceil: '
        self.print_dt(new_ceil)

        if is_end == False:
            if new_ceil - self.ceiling >= 1 * 60 * self.granu:  # not so frequent
                # e.g., aggregate 10:50 only when now > 11:00
                self.ceiling = new_ceil - 60 * self.granu
                self.release_memo()
        else:
            'cleaning in the end'
            self.ceiling = new_ceil - 60 * self.granu
            self.release_memo()

        return 0

    # aggregate any info that is <= ceiling
    def release_memo(self):
        print '\nreleasing...'
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if self.floor <= dt <= self.ceiling:
                self.print_dt(dt)
                rel_dt.append(dt)
        self.get_index(rel_dt)
        self.del_garbage()
        return 0

    def del_garbage(self):
        print '\ndeleting...'
        rel_dt = []  # dt for processing
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                self.print_dt(dt)
                del self.pfx_trie[dt]
                del self.fip_list[dt]
        return 0

    def add(self, update):
        updt_attr = update.split('|')[0:6]  # no need for attrs now

        intdt = int(updt_attr[1])
        objdt = datetime.datetime.fromtimestamp(intdt).\
                replace(second = 0, microsecond = 0)
        # Set granularity
        mi = self.xia_qu_zheng(objdt.minute, 'm')
        objdt = objdt.replace(minute = mi)
        intdt = time_lib.mktime(objdt.timetuple())  # Change into seconds int

        from_ip = updt_attr[3]
        if intdt not in self.fip_list.keys():
            self.fip_list[intdt] = []
        if from_ip not in self.fip_list[intdt]:
            self.fip_list[intdt].append(from_ip)

        pfx = self.ip_to_binary(updt_attr[5], from_ip)
        if intdt not in self.pfx_trie.keys():
            self.pfx_trie[intdt] = patricia.trie(None)
        try:  # Test whether the trie has the node
            pfx_fip = self.pfx_trie[intdt][pfx]
        except:  # Node does not exist
            self.pfx_trie[intdt][pfx] = [from_ip]
            return 0

        if from_ip not in pfx_fip:
            self.pfx_trie[intdt][pfx].append(from_ip)

        return 0

    def ip_to_binary(self, content, from_ip):  # can deal with ip addr and pfx
        length = None
        pfx = content.split('/')[0]
        try:
            length = int(content.split('/')[1])
        except:  # an addr, not a pfx
            pass
        if '.' in from_ip:  # IPv4
            addr = IPAddress(pfx).bits()
            addr = addr.replace('.', '')
            if length:
                addr = addr[:length]
            return addr
        elif ':' in from_ip:
            addr = IPAddress(pfx).bits()
            addr = addr.replace(':', '')
            if length:
                addr = addr[:length]
            return addr
        else:
            print 'protocol false!'
            return 0

    def get_index(self, rel_dt):
        for dt in rel_dt:
            len_all_fi = len(self.fip_list[dt])
            trie = self.pfx_trie[dt]
            for p in trie:
                if p == '':
                    continue
                ratio = float(len(trie[p]))/float(len_all_fi)
                if ratio <= 0.5:
                    continue
                try:
                    self.dvi1[dt] += ratio - 0.5
                except:
                    self.dvi1[dt] = ratio - 0.5
                try:
                    self.dvi2[dt] += np.power(2, (ratio-0.9)*10)
                except:
                    self.dvi2[dt] = np.power(2, (ratio-0.9)*10)
                try:
                    self.dvi3[dt] += np.power(5, (ratio-0.9)*10)
                except:
                    self.dvi3[dt] = np.power(5, (ratio-0.9)*10)
                try:
                    self.dvi4[dt] += 1
                except:
                    self.dvi4[dt] = 1
                try:
                    self.dvi5[dt] += ratio
                except:
                    self.dvi5[dt] = ratio

        return 0

    def plot_index(self):
        dvi1 = []  # list, only for plot
        dvi2 = []
        dvi3 = []
        dvi4 = []
        dvi5 = []

        dt = self.dvi1.keys()
        dt.sort()
        for key in dt:
            dvi1.append(self.dvi1[key])
            dvi2.append(self.dvi2[key])
            dvi3.append(self.dvi3[key])
            dvi4.append(self.dvi4[key])
            dvi5.append(self.dvi5[key])
        dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj

        fig = plt.figure(figsize=(16, 20))
        fig.suptitle('DVI '+self.sdate)

        ax1 = fig.add_subplot(511)
        ax1.plot(dt, dvi1, 'b-')
        ax1.xaxis.set_visible(False)
        ax1.set_ylabel('dvi1: ratio-0.5')

        ax2 = fig.add_subplot(512)
        ax2.plot(dt, dvi2, 'b-')
        ax2.xaxis.set_visible(False)
        ax2.set_ylabel('dvi2: power(2,(ratio-0.9)*10)')

        ax3 = fig.add_subplot(513)
        ax3.plot(dt, dvi3, 'b-')
        ax3.xaxis.set_visible(False)
        ax3.set_ylabel('dvi3: power(5,(ratio-0.9)*10)')

        ax4 = fig.add_subplot(514)
        ax4.plot(dt, dvi4, 'b-')
        ax4.xaxis.set_visible(False)
        ax4.set_ylabel('dvi4:1')

        ax5 = fig.add_subplot(515)
        ax5.plot(dt, dvi5, 'b-')
        ax5.set_ylabel('dvi5:ratio')

        ax5.set_xlabel('Datetime')
        myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
        ax5.xaxis.set_major_formatter(myFmt)

        plt.xticks(rotation=45)
        plt.plot()
        plt.savefig(self.sdate+'_dvi.pdf')
        return 0

    def get_50_90(self):
        len_all_fi = len(self.fip_list)
        self.ct_monitor[self.lastdt] = len_all_fi

        for p in self.trie:
            if p == '':
                continue
           
            if len(self.trie[p].keys()) >= 0.5 * len_all_fi:  # TODO: not dict any more
                try:
                    self.actv_pfx50[self.lastdt].append(p)
                except:
                    self.actv_pfx50[self.lastdt] = []
                    self.actv_pfx50[self.lastdt].append(p)
           
                if len(self.trie[p].keys()) >= 0.6 * len_all_fi:
                    try:
                        self.actv_pfx60[self.lastdt].append(p)
                    except:
                        self.actv_pfx60[self.lastdt] = []
                        self.actv_pfx60[self.lastdt].append(p)
               
                    if len(self.trie[p].keys()) >= 0.7 * len_all_fi:
                        try:
                            self.actv_pfx70[self.lastdt].append(p)
                        except:
                            self.actv_pfx70[self.lastdt] = []
                            self.actv_pfx70[self.lastdt].append(p)
                   
                        if len(self.trie[p].keys()) >= 0.8 * len_all_fi:
                            try:
                                self.actv_pfx80[self.lastdt].append(p)
                            except:
                                self.actv_pfx80[self.lastdt] = []
                                self.actv_pfx80[self.lastdt].append(p)
                       
                            if len(self.trie[p].keys()) >= 0.9 * len_all_fi:
                                try:
                                    self.actv_pfx90[self.lastdt].append(p)
                                except:
                                    self.actv_pfx90[self.lastdt] = []
                                    self.actv_pfx90[self.lastdt].append(p)

            else:
                continue

        try:
            self.ct90[self.lastdt] = len(self.actv_pfx90[self.lastdt])
        except:  # No active pfx at all
            self.ct90[self.lastdt] = 0
        try:
            self.ct80[self.lastdt] = len(self.actv_pfx80[self.lastdt])
        except:  # No active pfx at all
            self.ct80[self.lastdt] = 0
        try:
            self.ct70[self.lastdt] = len(self.actv_pfx70[self.lastdt])
        except:  # No active pfx at all
            self.ct70[self.lastdt] = 0
        try:
            self.ct60[self.lastdt] = len(self.actv_pfx60[self.lastdt])
        except:  # No active pfx at all
            self.ct60[self.lastdt] = 0
        try:
            self.ct50[self.lastdt] = len(self.actv_pfx50[self.lastdt])
        except:  # No active pfx at all
            self.ct50[self.lastdt] = 0

    def plot_50_90(self): 
        count90 = []
        count80 = []
        count70 = []
        count60 = []
        count50 = []
        count_m = []
        count_p = []
        count_u = []

        dt = self.ct90.keys()
        dt.sort()
        for key in dt:
            count90.append(self.ct90[key])
            count80.append(self.ct80[key])
            count70.append(self.ct70[key])
            count60.append(self.ct60[key])
            count50.append(self.ct50[key])
            count_m.append(self.ct_monitor[key])
            count_p.append(self.ct_p[key])
            count_u.append(self.ct_u[key])

        dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj

        # Plot all var in one figure
        fig = plt.figure(figsize=(16, 30))
        fig.suptitle('I-Seismometer '+self.sdate)

        ax1 = fig.add_subplot(711)
        ax1.plot(dt, count_u, 'b-', label='updates')
        ax1.xaxis.set_visible(False)
        ax1.set_ylabel('update count', color='b')

        ax11 = ax1.twinx()
        ax11.plot(dt, count_m, 'g-', label='monitors')
        ax11.xaxis.set_visible(False)
        ax11.set_ylabel('monitor count', color='g')

        ax1.legend(loc='best')
        ax11.legend(loc='best')

        ax2 = fig.add_subplot(712)
        ax2.plot(dt, count_p, 'b-')
        ax2.xaxis.set_visible(False)
        ax2.set_ylabel('pfx number', color='b')
        for t in ax2.get_yticklabels():
            t.set_color('b')

        ax3 = fig.add_subplot(713)
        ax3.plot(dt, count50, 'b-')
        ax3.xaxis.set_visible(False)
        ax3.set_ylabel('active 50')

        ax4 = fig.add_subplot(714)
        ax4.plot(dt, count60, 'b-')
        ax4.xaxis.set_visible(False)
        ax4.set_ylabel('active 60')

        ax5 = fig.add_subplot(715)
        ax5.plot(dt, count70, 'b-')
        ax5.xaxis.set_visible(False)
        ax5.set_ylabel('active 70')

        ax6 = fig.add_subplot(716)
        ax6.plot(dt, count80, 'b-')
        ax6.xaxis.set_visible(False)
        ax6.set_ylabel('active 80')

        ax7 = fig.add_subplot(717)
        ax7.plot(dt, count90, 'b-')
        ax7.set_ylabel('active 90')

        ax7.set_xlabel('Datetime')
        myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
        ax7.xaxis.set_major_formatter(myFmt)

        plt.xticks(rotation=45)
        plt.plot()
        plt.savefig(self.sdate+'_50_90.pdf')
