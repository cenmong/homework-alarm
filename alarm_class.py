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
        self.from_dt = {}  # collector: dt 
        self.to_dt = {}  # collector: dt 
        for cl in collectors:
            self.from_dt[cl[0]] = 0
            self.to_dt[cl[0]] = 0

        self.granu = granu  # Time granularity
        self.trie = patricia.trie(None)  # pfx: AS list
        '''
        self.actv_pfx90 = dict()  # {time: pfx list}
        self.actv_pfx80 = dict()  # {time: pfx list}
        self.actv_pfx70 = dict()  # {time: pfx list}
        self.actv_pfx60 = dict()  # {time: pfx list}
        self.actv_pfx50 = dict()  # {time: pfx list}
        '''
        self.ucount = 0  # update count in a period
        self.pcount = 0  # pfx count in a period
        self.lastdt = 0  # detect time period changes
        self.fip_list = []  # For detecting new from IP
        self.dvi1 = dict()  # {time: index value}
        self.dvi2 = dict()
        self.dvi3 = dict()
        self.dvi4 = dict()
        self.dvi5 = dict()
        '''
        self.ct90 = dict()  # {time: active pfx count}. For plot.
        self.ct80 = dict()  # {time: active pfx count}. For plot.
        self.ct70 = dict()  # {time: active pfx count}. For plot.
        self.ct60 = dict()  # {time: active pfx count}. For plot.
        self.ct50 = dict()  # {time: active pfx count}. For plot.
        self.ct_monitor = dict()  # {time: monitor count}. For plot.
        self.ct_p = dict()  # {time: all pfx count}. For plot.
        self.ct_u = dict()  # {time: all update count}. For plot.
        '''

        self.sdate = sdate  # For saving figures

    
    def set_from(self, collector, update):
        self.from_dt[collector] = int(update.split('|')[1])
        return 0

    def set_to(self, collector, update):
        self.to_dt[collector] = int(update.split('|')[1])
        return 0


    def add(self, update):
        updt_attr = update.split('|')[0:6]
        intdt = int(updt_attr[1])
        objdt = datetime.datetime.fromtimestamp(intdt)

        # Set granularity
        objdt = objdt.replace(second = 0, microsecond = 0)
        mi = (dt.minute / self.granu) * self.granu
        dt = dt.replace(minute = mi)
        intdt = time_lib.mktime(dt.timetuple())  # Change datetime into seconds

        from_ip = updt_attr[3]
        if from_ip not in self.fip_list:
            self.fip_list.append(from_ip)

        pfx = self.ip_to_binary(from_ip, updt_attr[5])

        if intdt != self.lastdt:
            if self.lastdt != 0:  # Not the first run
                print datetime.datetime.fromtimestamp(intdt)
                #self.get_50_90()
                self.get_index()
                self.trie = patricia.trie(None)
                self.pcount = 0
                self.ucount = 0
            
            self.lastdt = time

        try:  # Test whether the trie has the node
            test = self.trie[pfx]
        except:  # Node does not exist
            self.trie[pfx] = []
            self.pcount += 1

        if from_ip not in self.trie[pfx]:
            self.trie[pfx].append(from_ip)

        self.ucount += 1

    def get_index(self):
        len_all_fi = len(self.fip_list)

        for p in self.trie:
            if p == '':
                continue
            ratio = float(len(self.trie[p]))/float(len_all_fi)
            if ratio > 0.5:
                try:
                    self.dvi1[self.lastdt] += ratio - 0.5
                except:
                    self.dvi1[self.lastdt] = ratio - 0.5
                try:
                    self.dvi2[self.lastdt] += np.power(2, (ratio-0.9)*10)
                except:
                    self.dvi2[self.lastdt] = np.power(2, (ratio-0.9)*10)
                try:
                    self.dvi3[self.lastdt] += np.power(5, (ratio-0.9)*10)
                except:
                    self.dvi3[self.lastdt] = np.power(5, (ratio-0.9)*10)
                try:
                    self.dvi4[self.lastdt] += 1
                except:
                    self.dvi4[self.lastdt] = 1
                try:
                    self.dvi5[self.lastdt] += ratio
                except:
                    self.dvi5[self.lastdt] = ratio

        return 0

    def plot_index(self):
        dvi1 = []
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

        self.ct_p[self.lastdt] = self.pcount
        self.ct_u[self.lastdt] = self.ucount

    def ip_to_binary(self, from_ip, content):  # can deal with ip addr and pfx
        length = None
        pfx = content.split('/')[0]
        try:
            length = int(content.split('/')[1])
        except:
            pass
        if '.' in from_ip:
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
