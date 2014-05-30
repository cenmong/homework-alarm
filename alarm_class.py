from update_class import *

from netaddr import *
import patricia
import datetime
import time as time_lib

import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
from matplotlib.dates import HourLocator
import numpy as np

class Alarm():

    def __init__(self, granu, ymd):
        self.granu = granu  # Time granularity
        self.trie = patricia.trie(None)  # prefix: AS list
        '''
        self.actv_pfx90 = dict()  # {time: prefix list}
        self.actv_pfx80 = dict()  # {time: prefix list}
        self.actv_pfx70 = dict()  # {time: prefix list}
        self.actv_pfx60 = dict()  # {time: prefix list}
        self.actv_pfx50 = dict()  # {time: prefix list}
        '''
        self.ucount = 0  # update count in a period
        self.pcount = 0  # prefix count in a period
        self.lasttime = 0  # detect time period changes
        self.from_ip_list = []  # For detecting new from IP
        self.dvi1 = dict()  # {time: index value}
        self.dvi2 = dict()
        self.dvi3 = dict()
        '''
        self.ct90 = dict()  # {time: active prefix count}. For plot.
        self.ct80 = dict()  # {time: active prefix count}. For plot.
        self.ct70 = dict()  # {time: active prefix count}. For plot.
        self.ct60 = dict()  # {time: active prefix count}. For plot.
        self.ct50 = dict()  # {time: active prefix count}. For plot.
        self.ct_monitor = dict()  # {time: monitor count}. For plot.
        self.ct_p = dict()  # {time: all prefix count}. For plot.
        self.ct_u = dict()  # {time: all update count}. For plot.
        '''

        self.ymd = ymd  # For saving figures

    def add(self, update):
        dt = update.get_time()
        dt = datetime.datetime.strptime(dt, '%m/%d/%y %H:%M:%S')  # Format to obj 

        # Set granularity
        dt = dt.replace(second = 0, microsecond = 0)
        mi = (dt.minute / self.granu) * self.granu
        dt = dt.replace(minute = mi)
        time = time_lib.mktime(dt.timetuple())  # Change datetime into seconds

        from_ip = update.get_from_ip()
        if from_ip not in self.from_ip_list:
            self.from_ip_list.append(from_ip)

        prefix = update.get_announce() + update.get_withdrawn()

        if time != self.lasttime:
            if self.lasttime != 0:  # Not the first run
                print datetime.datetime.fromtimestamp(time)
                #self.get_50_90()
                self.get_index()
                self.trie = patricia.trie(None)
                self.pcount = 0
                self.ucount = 0
            
            self.lasttime = time

        for p in prefix:
            try:  # Test whether the trie has the node
                test = self.trie[p]
            except:  # Node does not exist
                self.trie[p] = []
                self.pcount += 1

            if from_ip not in self.trie[p]:
                self.trie[p].append(from_ip)

            self.ucount += 1

    def get_index(self):
        len_all_fi = len(self.from_ip_list)

        for p in self.trie:
            if p == '':
                continue
            ratio = float(len(self.trie[p]))/float(len_all_fi)
            if ratio > 0.5:
                try:
                    self.dvi1[self.lasttime] += ratio - 0.5
                except:
                    self.dvi1[self.lasttime] = ratio - 0.5
                try:
                    self.dvi2[self.lasttime] += np.power(2, (ratio-0.9)*10)
                except:
                    self.dvi2[self.lasttime] = np.power(2, (ratio-0.9)*10)
                try:
                    self.dvi3[self.lasttime] += np.power(5, (ratio-0.9)*10)
                except:
                    self.dvi3[self.lasttime] = np.power(5, (ratio-0.9)*10)

        return 0

    def plot_index(self):
        dvi1 = []
        dvi2 = []
        dvi3 = []

        dt = self.dvi1.keys()
        dt.sort()
        for key in dt:
            dvi1.append(self.dvi1[key])
            dvi2.append(self.dvi2[key])
            dvi3.append(self.dvi3[key])
        dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj

        fig = plt.figure(figsize=(16, 16))
        fig.suptitle('DVI '+self.ymd)

        ax1 = fig.add_subplot(311)
        ax1.plot(dt, dvi1, 'b-')
        ax1.xaxis.set_visible(False)
        ax1.set_ylabel('dvi1')

        ax2 = fig.add_subplot(312)
        ax2.plot(dt, dvi2, 'b-')
        ax2.xaxis.set_visible(False)
        ax2.set_ylabel('dvi2')

        ax3 = fig.add_subplot(313)
        ax3.plot(dt, dvi3, 'b-')
        ax3.set_ylabel('dvi3')
        ax3.set_xlabel('Datetime')
        myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
        ax3.xaxis.set_major_formatter(myFmt)

        plt.xticks(rotation=45)
        plt.plot()
        plt.savefig(self.ymd+'_dvi.pdf')
        return 0

    def get_50_90(self):
        len_all_fi = len(self.from_ip_list)
        self.ct_monitor[self.lasttime] = len_all_fi

        for p in self.trie:
            if p == '':
                continue
           
            if len(self.trie[p].keys()) >= 0.5 * len_all_fi:  # TODO: not dict any more
                try:
                    self.actv_pfx50[self.lasttime].append(p)
                except:
                    self.actv_pfx50[self.lasttime] = []
                    self.actv_pfx50[self.lasttime].append(p)
           
                if len(self.trie[p].keys()) >= 0.6 * len_all_fi:
                    try:
                        self.actv_pfx60[self.lasttime].append(p)
                    except:
                        self.actv_pfx60[self.lasttime] = []
                        self.actv_pfx60[self.lasttime].append(p)
               
                    if len(self.trie[p].keys()) >= 0.7 * len_all_fi:
                        try:
                            self.actv_pfx70[self.lasttime].append(p)
                        except:
                            self.actv_pfx70[self.lasttime] = []
                            self.actv_pfx70[self.lasttime].append(p)
                   
                        if len(self.trie[p].keys()) >= 0.8 * len_all_fi:
                            try:
                                self.actv_pfx80[self.lasttime].append(p)
                            except:
                                self.actv_pfx80[self.lasttime] = []
                                self.actv_pfx80[self.lasttime].append(p)
                       
                            if len(self.trie[p].keys()) >= 0.9 * len_all_fi:
                                try:
                                    self.actv_pfx90[self.lasttime].append(p)
                                except:
                                    self.actv_pfx90[self.lasttime] = []
                                    self.actv_pfx90[self.lasttime].append(p)

            else:
                continue

        try:
            self.ct90[self.lasttime] = len(self.actv_pfx90[self.lasttime])
        except:  # No active prefix at all
            self.ct90[self.lasttime] = 0
        try:
            self.ct80[self.lasttime] = len(self.actv_pfx80[self.lasttime])
        except:  # No active prefix at all
            self.ct80[self.lasttime] = 0
        try:
            self.ct70[self.lasttime] = len(self.actv_pfx70[self.lasttime])
        except:  # No active prefix at all
            self.ct70[self.lasttime] = 0
        try:
            self.ct60[self.lasttime] = len(self.actv_pfx60[self.lasttime])
        except:  # No active prefix at all
            self.ct60[self.lasttime] = 0
        try:
            self.ct50[self.lasttime] = len(self.actv_pfx50[self.lasttime])
        except:  # No active prefix at all
            self.ct50[self.lasttime] = 0

        self.ct_p[self.lasttime] = self.pcount
        self.ct_u[self.lasttime] = self.ucount

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
        fig.suptitle('I-Seismometer '+self.ymd)

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
        plt.savefig(self.ymd+'_50_90.pdf')
