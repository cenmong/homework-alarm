import datetime
import numpy as np
import calendar # do not use the time module
import cmlib
import operator
import string
import traceback
import logging
import subprocess
import re
import os
import ast

from operator import add
from cStringIO import StringIO
from netaddr import *
from env import *
#from supporter_class import *

import matplotlib
# This is useful. I can render figures thourgh ssh. VNC viewer in unnecessary.
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates
from matplotlib.dates import HourLocator
from matplotlib.dates import DayLocator
from matplotlib.patches import Ellipse
from matplotlib.patches import Rectangle
from matplotlib.lines import Line2D

line_type = ['k--', 'k-', 'k^-'] # line type (hard code)
font = {'size': 38,}

matplotlib.rc('font', **font)
plt.rc('legend',**{'fontsize':28})

spamhaus_s = 1363564800
spamhaus_e = 1364860800

dot_size = 60

default_color = 'k'
colors = ['r', 'b', 'g', 'm', 'cyan', 'darkorange',\
          'mediumpurple', 'salmon', 'lime', 'hotpink', 'yellow', '',\
          'firebrick', 'sienna', 'sandybrown', 'y', 'teal']
shapes = ['^', '*', 'D', 'd']
cluster_labels = ['Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4']
month_labels = ['Jan.','Feb.','Mar.','Apr','May','June','July','Aug.','Sept.','Oct.','Nov.','Dec.']
linestyles = ['-', '--', '_', ':']
markers = []
for m in Line2D.markers:
    try:
        if len(m) == 1 and m != ' ':
            markers.append(m)
    except TypeError:
        pass
styles = markers + [
        r'$\lambda$',
        r'$\bowtie$',
        r'$\circlearrowleft$',
        r'$\clubsuit$',
        r'$\checkmark$']


class Plotter():

    def __init__(self, reaper):
        self.mr = None
        self.reaper = reaper
        self.pfx_input_dir = reaper.get_output_dir_pfx()
        self.event_input_dir = reaper.get_output_dir_event()
        self.pfx_plot_dir = reaper.get_output_dir_pfx() + 'plot/'
        self.event_plot_dir = reaper.get_output_dir_event() + 'plot/'
        cmlib.make_dir(self.pfx_plot_dir)
        cmlib.make_dir(self.event_plot_dir)
        self.TS_events_dot_dir = pub_plot_dir + 'TS_events_dot/'
        cmlib.make_dir(self.TS_events_dot_dir)
        self.events_tpattern_dir = pub_plot_dir + 'events_time_pattern/'
        cmlib.make_dir(self.events_tpattern_dir)

    def set_multi_reaper(self, mr):
        self.mr = mr

    def all_events_tpattern_curve(self):
        index = self.reaper.period.index
        edict = self.reaper.get_events_list()

        pattern_lol = list() # list of lists
        try:
            pattern_f = open(self.reaper.events_tpattern_path(), 'r')
        except:
            return
        for line in pattern_f:
            line = line.rstrip('\n')
            unix_dt = int(line.split(':')[0])
            if edict[unix_dt][0] < global_rsize_threshold:
                continue
            plist = ast.literal_eval(line.split(':')[1])
            pattern_lol.append(plist)
            print plist
        pattern_f.close()

        n = 4
        total = 2 * n + 1
        xlist = list()
        for i in xrange(1, total + 1):
            xlist.append(i)
            
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        for ylist in pattern_lol:
            ax.plot(xlist,ylist,'k-')
        ax.set_ylabel('Density')
        ax.set_xlabel('slots')

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        #--------------------------------------------------------------
        output_loc = self.events_tpattern_dir + str(self.reaper.period.index) + '_tpattern.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf()
        plt.close()


    def TS_all_event_curve(self, rlist):
        #labels = [r'$\theta_d=0.75$',r'$\theta_d=0.8$',r'$\theta_d=0.85$']
        labels = ['10 min','20 min','30 min']

        xlists = list() # list of lists
        ylists = list()

        for r in rlist:
            dt = list()
            value = list()

            event_input_dir = r.get_output_dir_event()
            file = event_input_dir + 'events_new.txt'
            f = open(file, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix_dt = float(line.split(':')[0])
                the_dt = datetime.datetime.utcfromtimestamp(unix_dt)
                dt.append(the_dt)
                the_list = ast.literal_eval(line.split(':')[1])
                rsize = the_list[0]
                value.append(rsize)
            f.close()

            xlists.append(dt)
            ylists.append(value)


        for i in xrange(0, len(xlists)):
            ylists[i] = [y for (x,y) in sorted(zip(xlists[i],ylists[i]))]
            xlists[i] = sorted(xlists[i])

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        for i in xrange(0, len(xlists)):
            ax.plot(xlists[i],ylists[i],colors[i]+'-',label=labels[i])

        legend = ax.legend(loc='upper right',shadow=False)
        ax.set_ylabel('Relative size')
        ax.set_xlabel('Date')
        myFmt = mpldates.DateFormatter('%b\n%d')
        ax.xaxis.set_major_formatter(myFmt)

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        plt.savefig('TS_combined.pdf', bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def TS_event_dot(self):
        index = self.reaper.period.index
        try:
            occur_unix = occur_unix_dt[index]
            occur_dt = datetime.datetime.utcfromtimestamp(occur_unix)
        except:
            occur_dt = None

        #if index in ([0,16]):
        #    y_high = 0.1
        #else:
        #    y_high = 0.03 # Be careful! Setting this may miss some points!
        
        y_high = 0.09

        value = list()
        dt = list()

        file = self.event_input_dir + 'events_plusminus.txt'
        f = open(file, 'r')
        for line in f:
            line = line.rstrip('\n')
            unix_dt = float(line.split(':')[0])
            the_dt = datetime.datetime.utcfromtimestamp(unix_dt)
            dt.append(the_dt)
            the_list = ast.literal_eval(line.split(':')[1])
            rsize = the_list[0]
            value.append(rsize)
        f.close()

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        plt.scatter(dt, value, s=150, facecolor='r', edgecolors='none')
        ax.set_ylabel('Relative size')
        ax.set_xlabel('Date')
        myFmt = mpldates.DateFormatter('%b\n%d')
        ax.xaxis.set_major_formatter(myFmt)

        sdate = self.reaper.period.sdate
        year = int(sdate[0:4])
        month = int(sdate[4:6])
        day = int(sdate[6:8])
        sdate = datetime.datetime(year, month, day)
        edate = self.reaper.period.edate
        year = int(edate[0:4])
        month = int(edate[4:6])
        day = int(edate[6:8])
        edate = datetime.datetime(year, month, day)
        ax.set_xlim([mpldates.date2num(sdate), mpldates.date2num(edate)])
        ax.set_ylim([0,y_high]) # Be careful!

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        # polt a line
        if occur_dt is not None:
            plt.plot((occur_dt, occur_dt), (0, y_high), 'k--', lw=4)

        output_loc = self.TS_events_dot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
        #output_loc = self.event_plot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def TS_event_cluster_dot_mr(self):
        cluster_list = list()
        cluster_f = open(self.mr.events_cluster_path(), 'r')
        for line in cluster_f:
            print line
            line = line.rstrip('\n')
            int_list = line.split('|')
            for i in int_list:
                if i:
                    cluster_list.append(int(i))

        print 'clusters: ', cluster_list

        #if index in ([0,16]):
        #    y_high = 0.1
        #else:
        #    y_high = 0.03 # Be careful! Setting this may miss some points!
        
        y_high = 0.08

        value = list()
        dt = list()
        dt2value = dict()

        fig = plt.figure(figsize=(50, 9.2))
        ax = fig.add_subplot(111)
        
        for reaper in self.mr.rlist:
            file = reaper.get_output_dir_event() + 'events_plusminus.txt'
            f = open(file, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix_dt = float(line.split(':')[0])
                the_list = ast.literal_eval(line.split(':')[1])
                rsize = the_list[0]
                if rsize < global_rsize_threshold:
                    continue
                the_dt = datetime.datetime.utcfromtimestamp(unix_dt)
                dt.append(the_dt)
                value.append(rsize)
                dt2value[the_dt] = rsize
            f.close()

        sorted_dt = sorted(dt2value.keys())
        cluster2dtset = dict()
        assert len(cluster_list) == len(sorted_dt)
        for i in xrange(0, len(sorted_dt)):
            try:
                cluster2dtset[cluster_list[i]].add(sorted_dt[i])
            except:
                cluster2dtset[cluster_list[i]] = set([sorted_dt[i]])

        #print 'cluster2dtset:', cluster2dtset
        #print 'dt2value:', dt2value
        #print 'dt:', dt2value.keys()
        #----------------------------------------------------
        # different clusters are assigned different colors
        if -1 in cluster2dtset.keys():
            dt_list = list()
            value_list = list()
            for tmp_dt in cluster2dtset[-1]:
                dt_list.append(tmp_dt)
                value_list.append(dt2value[tmp_dt])
            print dt_list
            print value_list
            assert len(dt_list) == len(value_list)
            plt.scatter(dt_list, value_list, s=250, facecolor=default_color, edgecolors='none', label='Others')

        color_index = 0
        for c in cluster2dtset:
            dt_list = list()
            value_list = list()
            if c == -1:
                continue
            for tmp_dt in cluster2dtset[c]:
                dt_list.append(tmp_dt)
                value_list.append(dt2value[tmp_dt])
            print dt_list
            print value_list
            assert len(dt_list) == len(value_list)
            print colors[color_index]
            plt.scatter(dt_list, value_list, s=350, facecolor=colors[color_index], edgecolors='none', marker=shapes[color_index], label = cluster_labels[color_index])
            color_index += 1


        the_dt = datetime.datetime.utcfromtimestamp(1363564800)
        plt.plot((the_dt, the_dt), (0, y_high), 'k--', lw=3)
        the_dt = datetime.datetime.utcfromtimestamp(1364601600)
        plt.plot((the_dt, the_dt), (0, y_high), 'k--', lw=3)

        dt1 = datetime.datetime.utcfromtimestamp(1356998400)
        dt2 = datetime.datetime.utcfromtimestamp(1383264000)
        ax.set_xlim([dt1, dt2])

        ax.set_ylabel('Relative size')
        #ax.set_xlabel('Date')
        myFmt = mpldates.DateFormatter('%b\n%d')
        ax.xaxis.set_major_formatter(myFmt)

        #sdate = self.reaper.period.sdate
        #year = int(sdate[0:4])
        #month = int(sdate[4:6])
        #day = int(sdate[6:8])
        #sdate = datetime.datetime(year, month, day)
        #edate = self.reaper.period.edate
        #year = int(edate[0:4])
        #month = int(edate[4:6])
        #day = int(edate[6:8])
        #edate = datetime.datetime(year, month, day)
        #ax.set_xlim([mpldates.date2num(sdate), mpldates.date2num(edate)])
        ax.set_ylim([0,y_high]) # Be careful!

        legend = ax.legend(loc='upper right',shadow=False)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        # polt a line
        #if occur_dt is not None:
        #    plt.plot((occur_dt, occur_dt), (0, y_high), 'k--', lw=4)

        output_loc = pub_plot_dir + 'all_TS_event_dot.pdf'
        #output_loc = self.event_plot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def TS_event_cluster_dot(self):
        index = self.reaper.period.index
        try:
            occur_unix = occur_unix_dt[index]
            occur_dt = datetime.datetime.utcfromtimestamp(occur_unix)
        except:
            occur_dt = None

        cluster_list = list()
        cluster_f = open(self.reaper.events_cluster_path(), 'r')
        for line in cluster_f:
            print line
            line = line.rstrip('\n')
            int_list = line.split('|')
            for i in int_list:
                if i:
                    cluster_list.append(int(i))

        print 'clusters: ', cluster_list

        #if index in ([0,16]):
        #    y_high = 0.1
        #else:
        #    y_high = 0.03 # Be careful! Setting this may miss some points!
        
        y_high = 0.09

        value = list()
        dt = list()
        dt2value = dict()

        file = self.event_input_dir + 'events_plusminus.txt'
        f = open(file, 'r')
        for line in f:
            line = line.rstrip('\n')
            unix_dt = float(line.split(':')[0])
            the_dt = datetime.datetime.utcfromtimestamp(unix_dt)
            dt.append(the_dt)
            the_list = ast.literal_eval(line.split(':')[1])
            rsize = the_list[0]
            value.append(rsize)
            dt2value[the_dt] = rsize
        f.close()

        cluster2dtset = dict()
        sorted_dt = sorted(dt2value.keys())
        assert len(cluster_list) == len(sorted_dt)
        for i in xrange(0, len(sorted_dt)):
            try:
                cluster2dtset[cluster_list[i]].add(sorted_dt[i])
            except:
                cluster2dtset[cluster_list[i]] = set([sorted_dt[i]])


        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        
        #----------------------------------------------------
        # different clusters are assigned different colors
        if -1 in cluster2dtset.keys():
            dt_list = list()
            value_list = list()
            for tmp_dt in cluster2dtset[-1]:
                dt_list.append(tmp_dt)
                value_list.append(dt2value[tmp_dt])
            plt.scatter(dt_list, value_list, s=150, facecolor=default_color, edgecolors='none')

        color_index = 0
        for c in cluster2dtset:
            dt_list = list()
            value_list = list()
            if c == -1:
                continue
            for tmp_dt in cluster2dtset[c]:
                dt_list.append(tmp_dt)
                value_list.append(dt2value[tmp_dt])
            plt.scatter(dt_list, value_list, s=150, facecolor=colors[color_index], edgecolors='none')
            color_index += 1


        ax.set_ylabel('Relative size')
        ax.set_xlabel('Date')
        myFmt = mpldates.DateFormatter('%b\n%d')
        ax.xaxis.set_major_formatter(myFmt)

        sdate = self.reaper.period.sdate
        year = int(sdate[0:4])
        month = int(sdate[4:6])
        day = int(sdate[6:8])
        sdate = datetime.datetime(year, month, day)
        edate = self.reaper.period.edate
        year = int(edate[0:4])
        month = int(edate[4:6])
        day = int(edate[6:8])
        edate = datetime.datetime(year, month, day)
        ax.set_xlim([mpldates.date2num(sdate), mpldates.date2num(edate)])
        ax.set_ylim([0,y_high]) # Be careful!

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        # polt a line
        if occur_dt is not None:
            plt.plot((occur_dt, occur_dt), (0, y_high), 'k--', lw=4)

        output_loc = self.TS_events_dot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
        #output_loc = self.event_plot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def plot_pfx_all(self):
        files = os.listdir(self.pfx_input_dir)
        print files
        for f in files:
            if os.path.isfile(self.pfx_input_dir+f):
                files.remove(f)
                files.insert(0, self.pfx_input_dir+f)
            else:
                files.remove(f)

        for f in files:
            if '_ts' in f:
                pdfname = f.split('/')[-1].split('.')[0] + '.pdf'
                print 'Plotting ', self.pfx_plot_dir+pdfname
                self.basic_ts(f, self.pfx_plot_dir+pdfname)
            elif '_distr' in f:
                pdfname = f.split('/')[-1].split('.')[0] + '.pdf'
                print 'Plotting ', self.pfx_plot_dir+pdfname
                self.basic_distr(f, self.pfx_plot_dir+pdfname)
            else:
                print 'Did not plot ', f

    def basic_ts(self, input_loc, output_loc):
        the_dict = {}
        f = open(input_loc, 'r')
        for line in f:
            line = line.rstrip('\n')
            attrs = line.split(':')
            dt = int(attrs[0])
            value = float(attrs[1])
            the_dict[dt] = value
        f.close()
    
        # dict => two lists
        valuelist = []
        dtlist = the_dict.keys()
        dtlist.sort()
        for x in dtlist:
            valuelist.append(the_dict[x])
        counter = range(len(valuelist))

        # plot
        fig = plt.figure(figsize=(16,10))
        ax = fig.add_subplot(111)

        ax.plot(counter, valuelist, 'k--')

        # set the parameters
        ax.set_xlabel('time slots since ...')
        ax.set_ylabel('value')

        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def basic_distr(self, input_loc, output_loc):
        the_dict = {}
        f = open(input_loc, 'r')
        for line in f:
            line = line.rstrip('\n')
            attrs = line.split(':')
            value = float(attrs[0])
            count = float(attrs[1])
            the_dict[value] = count
        f.close()

        cdf_dict = self.value_count2cdf(the_dict)

        xlist = [0]
        ylist = [0]
        for key in sorted(cdf_dict):
            xlist.append(key)
            ylist.append(cdf_dict[key])

        xmax = max(xlist)
        ymax = max(ylist)

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        ax.plot(xlist, ylist, 'k-')
        ax.set_ylim([-0.1*ymax, 1.1*ymax])
        ax.set_xlim([-0.1*xmax, 1.1*xmax])
        ax.set_ylabel('cumulative distribution')
        ax.set_xlabel('value')

        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

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

        for i in xrange(0, len(ylist)):
            cdf[xlist[i]] = ylist[i]

        return cdf 

    def ratios_dot_mr(self):
        target = ['UPDATE','ONE','UPerO']

        for tar in target:
            fig = plt.figure(figsize=(16, 16))
            ax = fig.add_subplot(111)

            xlist = list()
            ylist = list()
            for reaper in self.mr.rlist:
                file = reaper.get_output_dir_event() + 'ratios.txt'
                f = open(file, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    linetype = line.split('|')[0]
                    if linetype != tar:
                        continue

                    rsize = float(line.split('|')[2])
                    if rsize < 0.007:
                        continue

                    if linetype in ('UPDATE', 'ONE'):
                        total = float(line.split('|')[3])
                        in_num = float(line.split('|')[4])
                        ratio = in_num / total
                        xlist.append(rsize)
                        ylist.append(ratio)

                    if linetype == 'UPerO':
                        outavg = float(line.split('|')[3])
                        inavg = float(line.split('|')[4])
                        xlist.append(outavg)
                        ylist.append(inavg)
                f.close()

            plt.scatter(xlist, ylist, s=400, facecolor='none', edgecolors='k')

            if tar == 'UPerO':
                ax.set_ylim([0,10]) # Be careful!
                ax.set_xlim([0,10]) # Be careful!
                ax.set_ylabel('Updates per one (out of LBE)')
                ax.set_xlabel('Updates per one (in LBE)')
                plt.plot([0,10],[0,10],'k-')

            if tar in ('UPDATE', 'ONE'):
                ax.set_ylim([0.38, 1])
                ax.set_xlim([0,0.04]) # Be careful!
                plt.xticks([0, 0.01, 0.02, 0.03, 0.04])
                ax.set_ylabel('Ratio of updates captured by LBE')
                ax.set_xlabel('Relative size')

            ax.tick_params(axis='y',pad=10)
            ax.tick_params(axis='x',pad=10)
            output_loc = pub_plot_dir + tar + '.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

    def width_dot_mr(self):
        fig = plt.figure(figsize=(16, 16))
        ax = fig.add_subplot(111)
        xlist = list()
        ylist = list()

        unix2rw = dict()
        f = open(datadir+'final_output/007_LBE_rwidth.txt', 'r')
        for line in f:
            line = line.rstrip('\n')
            line = line.split(':')
            unix = int(line[0])
            rwidth = float(line[1])
            unix2rw[unix] = rwidth
        f.close()

        unix2rs = dict()
        for reaper in self.mr.rlist:
            file = reaper.get_output_dir_event() + 'events_plusminus.txt'
            f = open(file, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix = int(line.split(':')[0])
                the_list = ast.literal_eval(line.split(':')[1])
                rsize = the_list[0]
                if rsize < 0.007:
                    continue
                unix2rs[unix] = rsize
            f.close()

        for unix in unix2rw:
            xlist.append(unix2rs[unix])
            ylist.append(unix2rw[unix])

        ax.set_ylim([0.38, 1.05])
        ax.set_xlim([0, 0.04])
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('Relative width')
        ax.set_xlabel('Relative size')
        plt.scatter(xlist, ylist, s=400, facecolor='none', edgecolors='k')
        plt.xticks([0, 0.01, 0.02, 0.03, 0.04])
        plt.yticks([0.4, 0.5, 0.6, 0.7, 0.8,0.9,1.0])
        output_loc = pub_plot_dir + 'width.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()


    def size_CDFs_mr(self):
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        count = 0
        for reaper in self.mr.rlist:
            vlist = list()
            xlist = [0] # initialized 
            ylist = [0]

            file = reaper.get_output_dir_event() + 'all_slot_size.txt'
            print 'Reading ', file
            f = open(file, 'r')
            for line in f:
                line = line.rstrip('\n')
                size = float(line.split(':')[1])
                vlist.append(size)
            f.close()

            vlist.sort()
            tlen = len(vlist)
            #print tlen
            for i in xrange(0,tlen):
                yindex = i + 1
                ylist.append(float(yindex) / float(tlen))
                xlist.append(vlist[i])

            ax.plot(xlist, ylist, linestyle='None', marker=styles[count],\
                    color=colors[count], markersize=15, label=month_labels[count])
            #ax.plot(xlist, ylist, 'k--')

            count += 1

        legend = ax.legend(loc='lower right',shadow=False)
        plt.plot((0.007, 0.007), (0, 1.1), 'k--', lw=4)
        ax.set_ylim([0.8, 1.01])
        ax.set_xlim([0.003, 0.011])
        plt.xticks([0.004, 0.006, 0.008, 0.010])
        ax.set_ylabel('Cumulative distribution (ratio)')
        ax.set_xlabel('Relative size')
        output_loc = pub_plot_dir + 'all_size_dist.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def LBE_updt_pattern(self, dt_list):
        target_types = ['AADiff','AADup2','AADup1','WW','WADup','WADiff',]
        #target_types = ['WAUnknown','AW','WW','WADup','WADiff','AADiff','AADup1','AADup2']

        numflist = list()
        oneflist = list()
        for dt in dt_list:
            numflist.append(str(dt)+'_updt_pattern.txt')
            oneflist.append(str(dt)+'_updt_pattern_in_ones.txt')

        numtype2list = dict()
        onetype2list = dict()
        for t in target_types:
            numtype2list[t] = list()
            onetype2list[t] = list()
        labels = target_types

        for reaper in self.mr.rlist:
            dir = reaper.get_output_dir_event() 
            for nf in numflist:
                nfpath = dir + nf
                if not os.path.isfile(nfpath):
                    continue
                print nf
                f = open(nfpath, 'r')
                type2num = dict()
                type2ratio = dict()
                for line in f:
                    line = line.rstrip('\n')
                    type = line.split(':')[0]
                    num = int(line.split(':')[1].split()[0])
                    type2num[type] = num
                    ratio = float(line.split(':')[1].split()[1])
                    type2ratio[type] = ratio
                f.close()

                removed_ratio = type2ratio['AW'] + type2ratio['WAUnknown']
                multiply_factor = 1.0 / (1-removed_ratio)
                for t in target_types:
                    type2ratio[t] = type2ratio[t] * multiply_factor
                    print t, type2num[t], type2ratio[t]
                    numtype2list[t].append(type2ratio[t])


            for of in oneflist:
                ofpath = dir + of
                if not os.path.isfile(ofpath):
                    continue
                print of
                type2ratio = dict()
                f = open(ofpath, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    type = line.split(':')[0]
                    ratio = float(line.split(':')[1])
                    type2ratio[type] = ratio
                f.close()
                    
                for t in target_types:
                    print t, type2ratio[t]
                    onetype2list[t].append(type2ratio[t])

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)

        loc = list()
        for i in xrange(1, 19):
            loc.append(i)
    
        width = 0.6
        p1=plt.bar(loc, numtype2list['AADiff'],width,color = colors[0])
        p2=plt.bar(loc, numtype2list['AADup2'],width,color = colors[1],bottom=numtype2list['AADiff'])
        tmp = map(add, numtype2list['AADiff'], numtype2list['AADup2'])
        p3=plt.bar(loc, numtype2list['AADup1'],width,color = colors[2],bottom=tmp)
        tmp = map(add, tmp, numtype2list['AADup1'])
        p4=plt.bar(loc, numtype2list['WW'],width,color = colors[3],bottom=tmp)
        tmp = map(add, tmp, numtype2list['WW'])
        p5=plt.bar(loc, numtype2list['WADup'],width,color = colors[4],bottom=tmp)
        tmp = map(add, tmp, numtype2list['WADup'])
        p6=plt.bar(loc, numtype2list['WADiff'],width,color = colors[5],bottom=tmp)
        tmp = map(add, tmp, numtype2list['WADiff'])


        #legend = ax.legend(loc='lower right',shadow=False)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('Ratio')
        ax.set_xlabel('LBE Sequence')
        plt.xticks([1,9,18],['1','9','18'])
        ax.set_xlim([0, 19])
        ax.set_ylim([0, 1])
        plt.legend((p1[0],p2[0],p3[0],p4[0],p5[0],p6[0]),('AADiff','AADup2','AADup1','WW','WADup','WADiff'),loc='lower left')
        output_loc = pub_plot_dir + 'upattern_num.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()


        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)

        loc = list()
        for i in xrange(1, 19):
            loc.append(i)
    
        width = 0.6
        p1=plt.bar(loc, onetype2list['AADiff'],width,color = colors[0])
        p2=plt.bar(loc, onetype2list['AADup2'],width,color = colors[1],bottom=onetype2list['AADiff'])
        tmp = map(add, onetype2list['AADiff'], onetype2list['AADup2'])
        p3=plt.bar(loc, onetype2list['AADup1'],width,color = colors[2],bottom=tmp)
        tmp = map(add, tmp, onetype2list['AADup1'])
        p4=plt.bar(loc, onetype2list['WW'],width,color = colors[3],bottom=tmp)
        tmp = map(add, tmp, onetype2list['WW'])
        p5=plt.bar(loc, onetype2list['WADup'],width,color = colors[4],bottom=tmp)
        tmp = map(add, tmp, onetype2list['WADup'])
        p6=plt.bar(loc, onetype2list['WADiff'],width,color = colors[5],bottom=tmp)
        tmp = map(add, tmp, onetype2list['WADiff'])


        #legend = ax.legend(loc='lower right',shadow=False)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('Ratio')
        ax.set_xlabel('LBE Sequence')
        plt.xticks([1,9,18],['1','9','18'])
        ax.set_xlim([0, 19])
        plt.legend((p1[0],p2[0],p3[0],p4[0],p5[0],p6[0]),('AADiff','AADup2','AADup1','WW','WADup','WADiff'),loc='lower left')
        output_loc = pub_plot_dir + 'upattern_one.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def pfx_upattern_dist(self, dt_list):
        aadiff_flist = list()
        aadup2_flist = list()
        f2id = dict()
        count = 1
        for dt in dt_list:
            aadiff_flist.append(str(dt)+'_tpfx_aadiff_ratio.txt')
            f2id[str(dt)+'_tpfx_aadiff_ratio.txt'] = count
            aadup2_flist.append(str(dt)+'_tpfx_policy_ratio.txt')
            f2id[str(dt)+'_tpfx_policy_ratio.txt'] = count
            count += 1

        count = 1
        id2plot = dict()
        for reaper in self.mr.rlist:
            dir = reaper.get_output_dir_event() 
            for myf in aadiff_flist:
                ratio2count = dict()
                myfpath = dir + myf
                if not os.path.isfile(myfpath):
                    continue
                print myfpath
                f = open(myfpath, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    value = float(line.split(':')[1].split('|')[0])
                    mcount = float(line.split(':')[1].split('|')[1])
                    ratio = value / mcount
                    try:
                        ratio2count[ratio] += 1
                    except:
                        ratio2count[ratio] = 1
                f.close()
            
                id = f2id[myf]
                id2plot[id] = ratio2count

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        for id in id2plot:
            ratio2count = id2plot[id]
            xlist = [0]
            ylist = [0]
            rlist = sorted(ratio2count.keys())
            prev = 0
            for i in xrange(0, len(rlist)):
                ratio = rlist[i]
                xlist.append(ratio)
                ylist.append(ratio2count[ratio] + prev)
                prev = ratio2count[ratio] + prev


            ax.plot(xlist, ylist, 'k-')

        #ax.set_ylim([-0.1*ymax, 1.1*ymax])
        #ax.set_xlim([-0.1*xmax, 1.1*xmax])
        ax.set_ylabel('Cumulative distribution')
        ax.set_xlabel('Proportion')

        output_loc = pub_plot_dir + 'tpfx_upattern_aadiff.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()



        count = 1
        id2plot = dict()
        for reaper in self.mr.rlist:
            dir = reaper.get_output_dir_event() 
            for myf in aadup2_flist:
                ratio2count = dict()
                myfpath = dir + myf
                if not os.path.isfile(myfpath):
                    continue
                print myfpath
                f = open(myfpath, 'r')
                for line in f:
                    line = line.rstrip('\n')
                    value = float(line.split(':')[1].split('|')[0])
                    mcount = float(line.split(':')[1].split('|')[1])
                    ratio = value / mcount
                    try:
                        ratio2count[ratio] += 1
                    except:
                        ratio2count[ratio] = 1
                f.close()
            
                id = f2id[myf]
                id2plot[id] = ratio2count

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        for id in id2plot:
            for f in f2id:
                if f2id[f] == id:
                    print f
            ratio2count = id2plot[id]
            xlist = [0]
            ylist = [0]
            rlist = sorted(ratio2count.keys())
            for r in rlist:
                print r,':',ratio2count[r]
            prev = 0
            for i in xrange(0, len(rlist)):
                ratio = rlist[i]
                xlist.append(ratio)
                ylist.append(ratio2count[ratio] + prev)
                prev = ratio2count[ratio] + prev

            ax.plot(xlist, ylist, 'k-')

        #ax.set_ylim([-0.1*ymax, 1.1*ymax])
        #ax.set_xlim([-0.1*xmax, 1.1*xmax])
        ax.set_ylabel('Cumulative distribution')
        ax.set_xlabel('Proportion')

        output_loc = pub_plot_dir + 'tpfx_upattern_aadup2.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def uv_uq_distr_mr(self):
        top_ratios = [0.8, 0.9, 0.95, 0.96, 0.97, 0.98, 0.99, 0.999] # change with that in reaper
        top2uq_list = dict()
        top2uv_list = dict()
        for top in top_ratios:
            top2uq_list[top] = list()
            top2uv_list[top] = list()

        for reaper in self.mr.rlist:
            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir + 'uq_uv_top.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                line = line.split(':')[1]
                part1 = line.split('&')[0]
                part2 = line.split('&')[1]
                part1 = part1.split()
                count = 0
                for item in part1:
                    if item == '':
                        continue
                    count += 1
                    top = float(item.split('|')[0])
                    uq = int(item.split('|')[1])
                    top2uq_list[top].append(uq)

                count = 0
                part2 = part2.split()
                for item in part2:
                    if item == '':
                        continue
                    count += 1
                    top = float(item.split('|')[0])
                    uv = float(item.split('|')[1])
                    top2uv_list[top].append(uv)
            f.close()

        #---------------------------------
        # plot UQ
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)

        i = -1
        for top in top2uq_list:
            print 'top=',top
            print 'list length=', len(top2uq_list[top])
            i += 1
            uqlist = sorted(top2uq_list[top])
            #uqlist = [1,1,2,2,2,3,4,5,6,6,7] # for test
            xlist = [0]
            ylist = [0]
            uq_pre = uqlist[0]
            sum = 0
            for uq in uqlist:
                if uq != uq_pre:
                    xlist.append(uq_pre)
                    ylist.append(sum)
                    sum += 1
                    uq_pre = uq
                else:
                    sum += 1
            ylist.append(sum)
            xlist.append(uq)

            ax.plot(xlist,ylist,'k-', color=colors[i], label=str(top))
            ymax = max(ylist)
            print 'ymax=',ymax

        legend = ax.legend(loc='lower right',shadow=False)
        ax.set_ylabel('number of time slots')
        ax.set_xlabel('Value')

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.plot((80, 80), (0, 23000), 'k--', lw=2)
        plt.plot((100, 100), (0, 23000), 'k--', lw=4)
        plt.plot((200, 200), (0, 23000), 'k--', lw=2)
        ax.set_xscale('log')
        plt.xlim(-1,10000)
        #plt.xlim(-1,1000)
        ax.set_ylim([0,ymax * 1.1])

        plt.grid()

        output_loc = pub_plot_dir + 'UQ_distr.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

        #---------------------------------
        # plot UV
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)

        i = -1
        for top in top2uv_list:
            print 'top=',top
            print 'list length=', len(top2uv_list[top])
            i += 1
            uvlist = sorted(top2uv_list[top])
            xlist = [0]
            ylist = [0]
            uv_pre = uvlist[0]
            sum = 0
            for uv in uvlist:
                if uv != uv_pre:
                    xlist.append(uv_pre)
                    ylist.append(sum)
                    sum += 1
                    uv_pre = uv
                else:
                    sum += 1
            ylist.append(sum)
            xlist.append(uv)

            ax.plot(xlist,ylist,'k-', color=colors[i])
            ymax = max(ylist)
            print 'ymax=',ymax

        #legend = ax.legend(loc='upper left',shadow=False)
        ax.set_ylabel('number of time slots')
        ax.set_xlabel('Value')

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_xlim([-0.01,1.01])
        ax.set_ylim([0,ymax * 1.1])
        plt.plot((0.3, 0.3), (0, 23000), 'k--', lw=2)
        plt.plot((0.4, 0.4), (0, 23000), 'k--', lw=4)
        plt.plot((0.5, 0.5), (0, 23000), 'k--', lw=2)

        plt.grid()

        output_loc = pub_plot_dir + 'UV_distr.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()


    def hpfx_compare(self, rlist):
        to_avg = 18 # number of slots to average

        unix2huqp = dict()
        unix2huvp = dict()
        for reaper in rlist:
            Tq = reaper.Tq
            unix2huqp[Tq] = dict()
            Tv = reaper.Tv
            unix2huvp[Tv] = dict()
            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                 line = line.rstrip('\n')
                 unix = int(line.split(':')[0])
                 tmp = line.split(':')[1].split('|')
                 huqp_num = int(tmp[0])
                 unix2huqp[Tq][unix] = huqp_num
                 huvp_num = int(tmp[1])
                 unix2huvp[Tv][unix] = huvp_num
            f.close()


        fig = plt.figure(figsize=(30, 15))
        ax = fig.add_subplot(111)
        count = -1
        for Tq in sorted(unix2huqp.keys()):
            print Tq
            count += 1
            mydict = unix2huqp[Tq]

            dt_list = list()
            value_list = list()
            for unix in sorted(mydict.keys()):
                the_dt = datetime.datetime.utcfromtimestamp(unix)
                dt_list.append(the_dt)
                value_list.append(mydict[unix])

            new_dlist = list()
            new_vlist = list()
            sum = 0
            for index in xrange(0, len(dt_list)): # initialize
                dt = dt_list[index]
                sum += value_list[index]
                if index != 0 and (index+1) % to_avg == 0:
                    new_dlist.append(dt)
                    new_vlist.append(sum)
                    sum = 0

            ax.plot(new_dlist,new_vlist,colors[count]+'-', label='Tq='+str(Tq))

        dt1 = min(dt_list)
        dt2 = max(dt_list)
        ax.set_xlim([dt1, dt2])

        ax.set_ylabel('Quantity')
        myFmt = mpldates.DateFormatter('%b %d')
        ax.xaxis.set_major_formatter(myFmt)

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        legend = ax.legend(loc='best',shadow=False)

        output_loc = pub_plot_dir + 'Tq_compare.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()


        fig = plt.figure(figsize=(30, 15))
        ax = fig.add_subplot(111)
        count = -1
        for Tv in sorted(unix2huvp.keys()):
            print Tv
            count += 1
            mydict = unix2huvp[Tv]

            dt_list = list()
            value_list = list()
            for unix in sorted(mydict.keys()):
                the_dt = datetime.datetime.utcfromtimestamp(unix)
                dt_list.append(the_dt)
                value_list.append(mydict[unix])

            new_dlist = list()
            new_vlist = list()
            sum = 0
            for index in xrange(0, len(dt_list)): # initialize
                dt = dt_list[index]
                sum += value_list[index]
                if index != 0 and (index+1) % to_avg == 0:
                    new_dlist.append(dt)
                    new_vlist.append(sum)
                    sum = 0

            ax.plot(new_dlist,new_vlist,colors[count]+'-', label='Tv='+str(Tv))

        dt1 = min(dt_list)
        dt2 = max(dt_list)
        ax.set_xlim([dt1, dt2])

        ax.set_ylabel('Quantity')
        myFmt = mpldates.DateFormatter('%b %d')
        ax.xaxis.set_major_formatter(myFmt)

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        legend = ax.legend(loc='best',shadow=False)

        output_loc = pub_plot_dir + 'Tv_compare.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def TS_hpfx_mr(self):
        unix2huqp = dict()
        unix2huvp = dict()
        unix2h2 = dict()
        for reaper in self.mr.rlist:
            Tv = reaper.Tv
            Tq = reaper.Tq
            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                 line = line.rstrip('\n')
                 unix = int(line.split(':')[0])
                 tmp = line.split(':')[1].split('|')
                 huqp_num = int(tmp[0])
                 huvp_num = int(tmp[1])
                 h2_num = int(tmp[2])

                 unix2huqp[unix] = huqp_num
                 unix2huvp[unix] = huvp_num
                 unix2h2[unix] = h2_num
            f.close()

        Tv = reaper.Tv
        Tq = reaper.Tq
        dict_list = [unix2huqp, unix2huvp, unix2h2]
        index2name = {1:'huqp',2:'huvp',3:'hap'}
        count = 0
        for mydict in dict_list:
            count += 1

            fig = plt.figure(figsize=(60, 20))
            ax = fig.add_subplot(111)

            dt_list = list()
            value_list = list()
            for unix in mydict:
                the_dt = datetime.datetime.utcfromtimestamp(unix)
                dt_list.append(the_dt)
                value_list.append(mydict[unix])

            plt.scatter(dt_list, value_list, s=60, facecolor='blue', edgecolors='none')

            soccur = datetime.datetime.utcfromtimestamp(spamhaus_s)
            eoccur = datetime.datetime.utcfromtimestamp(spamhaus_e)
            plt.axvspan(mpldates.date2num(soccur),mpldates.date2num(eoccur),facecolor='0.3',alpha=0.3)
            '''
            if count == 1:
                dt_list = list()
                value_list = list()
                for unix in unix2h2:
                    the_dt = datetime.datetime.utcfromtimestamp(unix)
                    dt_list.append(the_dt)
                    value_list.append(unix2h2[unix])

                plt.scatter(dt_list, value_list, s=20, facecolor='red', edgecolors='none')
            '''

            dt1 = min(dt_list)
            dt2 = max(dt_list)
            ax.set_xlim([dt1, dt2])
            ax.set_ylim([100, 10000]) # FIXME for test only

            ymax = max(value_list)
            pre_wd = -1
            for dt in sorted(dt_list):
                wd = dt.weekday()
                if wd == 0 and wd != pre_wd:
                    plt.plot((dt, dt), (0, ymax), 'k--', lw=3)
                pre_wd = wd

            ax.set_yscale('log')
            ax.set_ylabel('Quantity')
            myFmt = mpldates.DateFormatter('%b\n%d')
            ax.xaxis.set_major_formatter(myFmt)

            ax.tick_params(axis='y',pad=10)
            ax.tick_params(axis='x',pad=10)
            #plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

            output_loc = pub_plot_dir + index2name[count]+'_'+str(Tq)+'_'+str(Tv)+'.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

    def box_hpfx_mr(self):
        huqp = list()
        huvp = list()
        hap = list()
        for reaper in self.mr.rlist:
            Tv = reaper.Tv
            Tq = reaper.Tq
            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                 line = line.rstrip('\n')
                 tmp = line.split(':')[1].split('|')
                 huqp_num = int(tmp[0])
                 huvp_num = int(tmp[1])
                 hap_num = int(tmp[2])

                 huqp.append(huqp_num)
                 huvp.append(huvp_num)
                 hap.append(hap_num)

        for n in huqp:
            if n > 10000:
                print '#', n
        for n in huvp:
            if n > 10000:
                print '%', n
        for n in hap:
            if n > 10000:
                print 'A', n


        Tv = reaper.Tv
        Tq = reaper.Tq
        the_lists = list()
        the_lists.append(huqp)
        the_lists.append(huvp)
        the_lists.append(hap)

        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(111)
        ax.boxplot(the_lists, showmeans=True)
        ax.set_yscale('log')
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('quantity')

        output_loc = pub_plot_dir + 'hpfx_number_'+str(Tq)+'_'+str(Tv)+'.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def hratio_box(self):
        the_lists = list() # list of lists

        h2_huqp_list = list()
        h2_huvp_list = list()
        for reaper in self.mr.rlist:
            Tv = reaper.Tv
            Tq = reaper.Tq
            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                tmp = line.split(':')[1].split('|')
                huqp_num = float(tmp[0])
                huvp_num = float(tmp[1])
                h2_num = float(tmp[2])

                try:
                    ratio1 = h2_num / huqp_num
                    h2_huqp_list.append(ratio1)
                except:
                    print 'no huqp'
                
                try:
                    ratio2 = h2_num / huvp_num
                    h2_huvp_list.append(ratio2)
                except:
                    print 'no huvp'
            f.close()

        Tv = reaper.Tv
        Tq = reaper.Tq

        print 'hap/huqp average', float(sum(h2_huqp_list))/float(len(h2_huqp_list))
        c = 0
        for r in h2_huqp_list:
            if r < 0.5:
                c += 1
        print c

        print 'hap/huvp percentile', np.percentile(h2_huvp_list, 85)
        print 'hap/huvp percentile', np.percentile(h2_huvp_list, 15)
        c = 0
        for r in h2_huvp_list:
            if r < 0.5:
                c += 1
        print c

        the_lists.append(h2_huqp_list)
        the_lists.append(h2_huvp_list)

        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(111)
        ax.boxplot(the_lists, showmeans=True)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('ratio')

        output_loc = pub_plot_dir + 'h2_ratio_'+str(Tq)+'_'+str(Tv)+'.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def updt_ratio_box(self):
        the_lists = list() # list of lists

        updt_ratio_huqp = list()
        updt_ratio_huvp = list()
        updt_ratio_h2p = list()
        h2p_huqp = list()
        for reaper in self.mr.rlist:
            Tq = reaper.Tq
            Tv = reaper.Tv

            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'TS_updt_num_'+str(Tv)+'_'+str(Tq)+'.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                tmp = line.split(':')[1].split('|')
                total = float(tmp[0])
                huqp_u = float(tmp[1])
                huvp_u = float(tmp[2])
                h2p_u = float(tmp[3])

                updt_ratio_huqp.append(huqp_u/total)
                updt_ratio_huvp.append(huvp_u/total)
                updt_ratio_h2p.append(h2p_u/total)
                h2p_huqp.append(h2p_u/huqp_u)

        the_lists.append(updt_ratio_huqp)
        the_lists.append(updt_ratio_huvp)
        the_lists.append(updt_ratio_h2p)
        the_lists.append(h2p_huqp)

        print float(sum(updt_ratio_huqp))/float(len(updt_ratio_huqp))
        print float(sum(updt_ratio_huvp))/float(len(updt_ratio_huvp))
        print float(sum(updt_ratio_h2p))/float(len(updt_ratio_h2p))
        print float(sum(h2p_huqp))/float(len(h2p_huqp))

        fig = plt.figure(figsize=(10, 10))
        ax = fig.add_subplot(111)
        ax.boxplot(the_lists, showmeans=True)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('update ratio')

        Tq = reaper.Tq
        Tv = reaper.Tv
        output_loc = pub_plot_dir + 'update_ratio_'+str(Tq)+'_'+str(Tv)+'.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def TS_total_huvp_huqp_updt_mr(self):
        dt2total = dict()
        dt2uq = dict()
        dt2uv = dict()
        dt2h2 = dict()
        for reaper in self.mr.rlist:
            Tq = reaper.Tq
            Tv = reaper.Tv

            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'TS_updt_num_'+str(Tv)+'_'+str(Tq)+'.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix = int(line.split(':')[0])
                tmp = line.split(':')[1].split('|')
                total = int(tmp[0])
                huqp_u = int(tmp[1])
                huvp_u = int(tmp[2])
                h2p_u = int(tmp[3])

                dt2total[unix] = total
                dt2uq[unix] = huqp_u
                dt2uv[unix] = huvp_u
                dt2h2[unix] = h2p_u

        dlist = [dt2total, dt2uq, dt2uv, dt2h2]
        index2name = {0:'total_updt',1:'huqp_updt',2:'huvp_updt',3:'hap_updt'}
        count = 0
        for mydict in dlist:

            fig = plt.figure(figsize=(60, 20))
            ax = fig.add_subplot(111)

            dt_list = list()
            value_list = list()
            for unix in mydict:
                the_dt = datetime.datetime.utcfromtimestamp(unix)
                dt_list.append(the_dt)
                value_list.append(mydict[unix])

            plt.scatter(dt_list, value_list, s=60, facecolor='blue', edgecolors='none')

            dt1 = min(dt_list)
            dt2 = max(dt_list)
            ax.set_xlim([dt1, dt2])
            #ax.set_ylim([100, 10000]) # FIXME for test only

            ymax = max(value_list)
            pre_wd = -1
            for dt in sorted(dt_list):
                wd = dt.weekday()
                if wd == 0 and wd != pre_wd:
                    plt.plot((dt, dt), (0, ymax), 'k--', lw=3)
                pre_wd = wd

            ax.set_yscale('log')
            ax.set_ylabel('Quantity')
            myFmt = mpldates.DateFormatter('%b\n%d')
            ax.xaxis.set_major_formatter(myFmt)

            soccur = datetime.datetime.utcfromtimestamp(spamhaus_s)
            eoccur = datetime.datetime.utcfromtimestamp(spamhaus_e)
            plt.axvspan(mpldates.date2num(soccur),mpldates.date2num(eoccur),facecolor='0.3',alpha=0.3)
            ax.tick_params(axis='y',pad=10)
            ax.tick_params(axis='x',pad=10)
            #plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

            output_loc = pub_plot_dir + index2name[count]+'_'+str(Tq)+'_'+str(Tv)+'.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

            count += 1

    def new_pfx_mr(self):
        unix2nhuqp = dict()
        unix2nhuvp = dict()
        unix2nhap = dict()

        unix2totaln_q = dict()
        unix2totaln_v = dict()
        unix2totaln_a = dict()

        Tq = None
        Tv = None

        for reaper in self.mr.rlist:
            Tq = reaper.Tq
            Tv = reaper.Tv

            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'new_huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix = int(line.split(':')[0])
                tmp1 = line.split(':')[1].split('&')[0]
                unix2totaln_q[unix] = int(tmp1.split('|')[0])
                unix2totaln_v[unix] = int(tmp1.split('|')[1])
                unix2totaln_a[unix] = int(tmp1.split('|')[2])
                tmp2 = line.split(':')[1].split('&')[1]
                unix2nhuqp[unix] = int(tmp2.split('|')[0])
                unix2nhuvp[unix] = int(tmp2.split('|')[1])
                unix2nhap[unix] = int(tmp2.split('|')[2])
            f.close()

        dict_list = [unix2totaln_q, unix2totaln_v, unix2totaln_a,\
                unix2nhuqp, unix2nhuvp, unix2nhap]
        index2name = {0:'total_newhuqp',1:'total_newhuvp',2:'total_newhap',\
                3:'new_huqp',4:'new_huvp',5:'new_hap'}

        index = -1
        for mydict in dict_list:
            index += 1

            fig = plt.figure(figsize=(60, 20))
            ax = fig.add_subplot(111)

            dt_list = list()
            value_list = list()
            for unix in mydict:
                the_dt = datetime.datetime.utcfromtimestamp(unix)
                dt_list.append(the_dt)
                value_list.append(mydict[unix])

            plt.scatter(dt_list, value_list, s=dot_size, facecolor='blue', edgecolors='none')

            dt1 = min(dt_list)
            dt2 = max(dt_list)
            ax.set_xlim([dt1, dt2])
            if index in [0,1,2]:
                ax.set_ylim([1, 10000]) # FIXME for test only

            ymax = max(value_list)
            pre_wd = -1
            for dt in sorted(dt_list):
                wd = dt.weekday()
                if wd == 0 and wd != pre_wd:
                    plt.plot((dt, dt), (0, ymax), 'k--', lw=3)
                pre_wd = wd

            soccur = datetime.datetime.utcfromtimestamp(spamhaus_s)
            eoccur = datetime.datetime.utcfromtimestamp(spamhaus_e)
            plt.axvspan(mpldates.date2num(soccur),mpldates.date2num(eoccur),facecolor='0.3',alpha=0.3)

            ax.set_yscale('log')
            ax.set_ylabel('Quantity')
            myFmt = mpldates.DateFormatter('%b\n%d')
            ax.xaxis.set_major_formatter(myFmt)

            ax.tick_params(axis='y',pad=10)
            ax.tick_params(axis='x',pad=10)
            #plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

            output_loc = pub_plot_dir + index2name[index]+'_'+str(Tq)+'_'+str(Tv)+'.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

    def hpfx_lifetime_distr_mr(self):
        Tq = self.mr.rlist[0].Tq
        Tv = self.mr.rlist[0].Tv
        fpath = datadir + 'final_output_pfx/' + 'lifetime_huvp_'+str(Tv)+'_huqp_'+str(Tq)+'.txt'

        uq_lt = dict()
        uv_lt = dict()
        a_lt = dict()
        uqo_lt = dict()
        uvo_lt = dict()

        f = open(fpath, 'r')
        for line in f:
            line = line.rstrip('\n')
            value = int(line.split(':')[1])
            if line.startswith('#'):
                try:
                    uq_lt[value] += 1
                except:
                    uq_lt[value] = 1
            elif line.startswith('%'):
                try:
                    uv_lt[value] += 1
                except:
                    uv_lt[value] = 1
            elif line.startswith('O#'):
                try:
                    uqo_lt[value] += 1
                except:
                    uqo_lt[value] = 1
            elif line.startswith('O%'):
                try:
                    uvo_lt[value] += 1
                except:
                    uvo_lt[value] = 1
            else:
                try:
                    a_lt[value] += 1
                except:
                    a_lt[value] = 1
        f.close()

        dict_list = [uq_lt, uv_lt, a_lt, uqo_lt, uvo_lt]
        index2name = {0:'UQ_LT',1:'UV_LT',2:'A_LT',3:'UQO_LT',4:'UVO_LT'}

        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)

        count = 0
        for mydict in dict_list:
            cdf_dict = self.value_count2cdf(mydict)

            xlist = [0]
            ylist = [0]
            for key in sorted(cdf_dict):
                xlist.append(key)
                ylist.append(cdf_dict[key])

            ymax = float(max(ylist))
            for i in xrange(0, len(ylist)):
                ratio = float(ylist[i]) / ymax
                if ratio > 0.78:
                    print count, ratio, xlist[i]

            ax.plot(xlist, ylist, 'k-', color=colors[count], label=index2name[count])
            count += 1

        legend = ax.legend(loc='lower right',shadow=False)
        ax.set_ylabel('cumulative distribution')
        ax.set_xlabel('value')
        #ax.set_xlim([-1, 1000])
        ax.set_xscale('log')
        plt.grid()

        output_loc = pub_plot_dir + 'LifeTimeDistribution_'+str(Tq)+'_'+str(Tv)+'.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    def new_pfx_box_mr(self): # discarded: incorrect in concept
        hap_in_huqp_total = list()
        hap_in_huvp_total = list()
        hap_in_huqp = list()
        hap_in_huvp = list()

        Tq = None
        Tv = None

        for reaper in self.mr.rlist:
            Tq = reaper.Tq
            Tv = reaper.Tv

            mydir = reaper.pfx_final_dir + 'default/'
            fpath = mydir+'new_huvp_'+str(Tv)+'_huqp_'+str(Tq)+'_TS.txt'
            f = open(fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                tmp1 = line.split(':')[1].split('&')[0]
                totalnew_hap = float(tmp1.split('|')[2])
                hap_in_huqp_total.append(totalnew_hap / float(tmp1.split('|')[0]))
                hap_in_huvp_total.append(totalnew_hap / float(tmp1.split('|')[1]))
                tmp2 = line.split(':')[1].split('&')[1]
                new_hap = float(tmp2.split('|')[2])
                hap_in_huqp.append(new_hap / float(tmp2.split('|')[0]))
                hap_in_huvp.append(new_hap / float(tmp2.split('|')[1]))
            f.close()

        the_lists = list()
        the_lists.append(hap_in_huqp_total)
        the_lists.append(hap_in_huvp_total)
        the_lists.append(hap_in_huqp)
        the_lists.append(hap_in_huvp)

        fig = plt.figure(figsize=(20, 10))
        ax = fig.add_subplot(111)
        ax.boxplot(the_lists, showmeans=True)
        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        ax.set_ylabel('ratio')

        output_loc = pub_plot_dir + 'new_hap_ratio_'+str(Tq)+'_'+str(Tv)+'.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()
