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
        self.from_ip_list = []  # temprorily store monitor
        self.lasttime = 0
        self.dvi1 = dict()  # {time: index value}
        self.dvi2 = dict()
        self.dvi4 = dict()
        self.dvi5 = dict()
        self.monitor_dict = dict()  # {time: number of monitors}

        self.dvi1_avg = []  # {day: average value}
        self.dvi1_med = []
        self.dvi2_avg = []  # {day: average value}
        self.dvi2_med = []
        self.dvi4_avg = []  # {day: average value}
        self.dvi4_med = []
        self.dvi5_avg = []  # {day: average value}
        self.dvi5_med = []

        self.monitor = []  # {day: number of monitors}

        self.days = []  # strings of days
        #for i in range(2003, 2005):
        for i in range(2003, 2014):
            for j in ['01', '04', '07', '10']:
                if i == 2012 and j == '07':  # TODO: temp: to avoid bug
                    continue
                self.days.append(str(i)+j)
                self.dvi1_avg.append(0)  # initialization 
                self.dvi2_avg.append(0)
                self.dvi4_avg.append(0)
                self.dvi5_avg.append(0)

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
                self.get_index()  # TODO: should consider the last run
                self.trie = patricia.trie(None)
            
            self.lasttime = time

        for p in prefix:
            try:  # Test whether the trie has the node
                test = self.trie[p]
            except:  # Node does not exist
                self.trie[p] = []

            if from_ip not in self.trie[p]:
                self.trie[p].append(from_ip)


    def get_index(self):
        len_all_fi = len(self.from_ip_list)  # get number of from ip

        for p in self.trie:
            if p == '':  # root node
                continue
            ratio = float(len(self.trie[p]))/float(len_all_fi)
            if ratio > 0.5:  # this prefix is highly visible
                # TODO: if no one goes in, all assign zero
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

        self.monitor_dict[self.lasttime] = len_all_fi
        self.from_ip_list = []
        return 0

    def get_avg_med(self):
        dt = self.dvi1.keys()
        dt.sort()
        order = 0  # order of season
        count = 0  # number of data in a season
        temp1 = []
        temp2 = []
        temp4 = []
        temp5 = []
        last_key = dt[0]
        for key in dt:
            if key - last_key < 100000:  # in the same season
                self.dvi1_avg[order] += self.dvi1[key]
                self.dvi2_avg[order] += self.dvi2[key]
                self.dvi4_avg[order] += self.dvi4[key]
                self.dvi5_avg[order] += self.dvi5[key]
                temp1.append(self.dvi1[key])
                temp2.append(self.dvi2[key])
                temp4.append(self.dvi4[key])
                temp5.append(self.dvi5[key])
                count += 1
            else:  # in different seasons
                self.monitor.append(self.monitor_dict[last_key])
                self.dvi1_avg[order] = float(self.dvi1_avg[order])/float(count)
                self.dvi2_avg[order] = float(self.dvi2_avg[order])/float(count)
                self.dvi4_avg[order] = float(self.dvi4_avg[order])/float(count)
                self.dvi5_avg[order] = float(self.dvi5_avg[order])/float(count)
                self.dvi1_med.append(sorted(temp1)[len(temp1)/2])
                self.dvi2_med.append(sorted(temp2)[len(temp2)/2])
                self.dvi4_med.append(sorted(temp4)[len(temp4)/2])
                self.dvi5_med.append(sorted(temp5)[len(temp5)/2])
                temp1 = [self.dvi1[key]]
                temp2 = [self.dvi2[key]]
                temp4 = [self.dvi4[key]]
                temp5 = [self.dvi5[key]]
                self.dvi1_avg[order] += self.dvi1[key]  # index out of range
                self.dvi2_avg[order] += self.dvi2[key]
                self.dvi4_avg[order] += self.dvi4[key]
                self.dvi5_avg[order] += self.dvi5[key]
                count = 1
                order += 1
            last_key = key

        self.monitor.append(self.monitor_dict[key])
        self.dvi1_avg[order] = float(self.dvi1_avg[order])/float(count)
        self.dvi2_avg[order] = float(self.dvi2_avg[order])/float(count)
        self.dvi4_avg[order] = float(self.dvi4_avg[order])/float(count)
        self.dvi5_avg[order] = float(self.dvi5_avg[order])/float(count)
        self.dvi1_med.append(sorted(temp1)[len(temp1)/2])
        self.dvi2_med.append(sorted(temp2)[len(temp2)/2])
        self.dvi4_med.append(sorted(temp4)[len(temp4)/2])
        self.dvi5_med.append(sorted(temp5)[len(temp5)/2])

    def plot(self):
        fig = plt.figure(figsize=(16, 20))
        fig.suptitle('Chronology')

        x_int = range(0, len(self.days))

        print x_int
        print self.monitor

        ax0 = fig.add_subplot(511)
        ax0.plot(x_int, self.monitor, 'b-')
        ax0.xaxis.set_visible(False)
        ax0.set_ylabel('monitors')

        ax1 = fig.add_subplot(512)
        ax1.plot(x_int, self.dvi1_avg, 'b-', label='average')
        ax1.xaxis.set_visible(False)
        ax1.set_ylabel('dvi1: ratio-0.5')

        ax11 = ax1.twinx()
        ax11.plot(x_int, self.dvi1_med, 'g-', label='median')
        ax11.xaxis.set_visible(False)
        ax1.set_ylabel('dvi1: ratio-0.5')

        ax1.legend(loc='best')
        ax11.legend(loc='best')

        ax2 = fig.add_subplot(513)
        ax2.plot(x_int, self.dvi2_avg, 'b-', label='average')
        ax2.xaxis.set_visible(False)
        ax2.set_ylabel('dvi2:np.power(2, (ratio-0.9)*10)')

        ax22 = ax2.twinx()
        ax22.plot(x_int, self.dvi2_med, 'g-', label='median')
        ax22.xaxis.set_visible(False)
        ax22.set_ylabel('dvi2')

        ax2.legend(loc='best')
        ax22.legend(loc='best')

        ax4 = fig.add_subplot(514)
        ax4.plot(x_int, self.dvi4_avg, 'b-', label='average')
        ax4.xaxis.set_visible(False)
        ax4.set_ylabel('dvi4:1')

        ax44 = ax4.twinx()
        ax44.plot(x_int, self.dvi4_med, 'g-', label='median')
        ax44.xaxis.set_visible(False)
        ax4.set_ylabel('dvi4')

        ax4.legend(loc='best')
        ax44.legend(loc='best')

        ax5 = fig.add_subplot(515)
        ax5.plot(x_int, self.dvi5_avg, 'b-', label='average')
        ax5.set_ylabel('dvi5:ratio')

        ax55 = ax5.twinx()
        ax55.plot(x_int, self.dvi1_med, 'g-', label='median')
        ax55.xaxis.set_visible(False)
        ax55.set_ylabel('dvi5')

        ax5.legend(loc='best')
        ax55.legend(loc='best')
        
        #plt.xticks(range(len(x_int)), self.days, rotation=45)
        plt.plot()
        plt.savefig('chronology.pdf')
        return 0
