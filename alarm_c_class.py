from update_class import *

from netaddr import *
import patricia
import datetime
import time as time_lib

import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
from matplotlib.dates import HourLocator
import numpy as np

class Alarm_c():

    def __init__(self, granu):
        self.granu = granu  # Time granularity
        self.trie = patricia.trie(None)  # prefix: AS list
        self.dvi1 = dict()  # {time: index value}
        self.dvi2 = dict()
        self.dvi4 = dict()
        self.dvi5 = dict()

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
                    self.dvi4[self.lasttime] += 1
                except:
                    self.dvi4[self.lasttime] = 1
                try:
                    self.dvi5[self.lasttime] += ratio
                except:
                    self.dvi5[self.lasttime] = ratio

        return 0

    def plot(self):
        dvi1 = []
        dvi2 = []
        dvi4 = []
        dvi5 = []

        dt = self.dvi1.keys()
        dt.sort()
        for key in dt:
            dvi1.append(self.dvi1[key])
            dvi2.append(self.dvi2[key])
            dvi4.append(self.dvi4[key])
            dvi5.append(self.dvi5[key])
        dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj

        fig = plt.figure(figsize=(16, 20))
        fig.suptitle('DVI '+self.ymd)

        ax1 = fig.add_subplot(411)
        ax1.plot(dt, dvi1, 'b-')
        ax1.xaxis.set_visible(False)
        ax1.set_ylabel('dvi1: ratio-0.5')

        ax2 = fig.add_subplot(412)
        ax2.plot(dt, dvi2, 'b-')
        ax2.xaxis.set_visible(False)
        ax2.set_ylabel('dvi2: power(2,(ratio-0.9)*10)')

        ax4 = fig.add_subplot(413)
        ax4.plot(dt, dvi4, 'b-')
        ax4.xaxis.set_visible(False)
        ax4.set_ylabel('dvi4:1')

        ax5 = fig.add_subplot(414)
        ax5.plot(dt, dvi5, 'b-')
        ax5.set_ylabel('dvi5:ratio')

        ax5.set_xlabel('Datetime')
        myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
        ax5.xaxis.set_major_formatter(myFmt)

        plt.xticks(rotation=45)
        plt.plot()
        plt.savefig('chronology.pdf')
        return 0
