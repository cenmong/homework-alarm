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

line_type = ['k--', 'k-', 'k^-'] # line type (hard code)
font = {'size': 38,}

matplotlib.rc('font', **font)
plt.rc('legend',**{'fontsize':28})

colors = ['r', 'b', 'g', 'yellow', 'm', 'cyan', 'darkorange',\
          'mediumpurple', 'salmon', 'lime', 'hotpink', '',\
          'firebrick', 'sienna', 'sandybrown', 'y', 'teal']

default_color = 'k'

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

        pattern_lol = list() # list of lists
        try:
            pattern_f = open(self.reaper.events_tpattern_path(), 'r')
        except:
            return
        for line in pattern_f:
            line = line.rstrip('\n')
            unix_dt = int(line.split(':')[0])
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
        
        y_high = 0.04

        value = list()
        dt = list()
        dt2value = dict()

        fig = plt.figure(figsize=(60, 10))
        ax = fig.add_subplot(111)
        
        for reaper in self.mr.rlist:
            file = reaper.get_output_dir_event() + 'events_plusminus.txt'
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
            plt.scatter(dt_list, value_list, s=50, facecolor=default_color, edgecolors='none')

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
            plt.scatter(dt_list, value_list, s=50, facecolor=colors[color_index], edgecolors='none')
            color_index += 1


        ax.set_ylabel('Relative size')
        ax.set_xlabel('Date')
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

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        # polt a line
        #if occur_dt is not None:
        #    plt.plot((occur_dt, occur_dt), (0, y_high), 'k--', lw=4)

        output_loc = datadir + 'final_output/all_TS_event_dot.pdf'
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

                    if linetype in ('UPDATE', 'ONE'):
                        rsize = float(line.split('|')[2])
                        total = float(line.split('|')[3])
                        in_num = float(line.split('|')[4])
                        ratio = in_num / total
                        xlist.append(rsize)
                        ylist.append(ratio)

                    if linetype == 'UPerO':
                        rsize = float(line.split('|')[2])
                        outavg = float(line.split('|')[3])
                        inavg = float(line.split('|')[4])
                        xlist.append(outavg)
                        ylist.append(inavg)
                f.close()

            plt.scatter(xlist, ylist, s=150, facecolor='none', edgecolors='b')

            if tar == 'UPerO':
                print 'XXXXXXXXXXXXXXXXXXXX'
                ax.set_ylim([0,10]) # Be careful!
                ax.set_xlim([0,10]) # Be careful!

            output_loc = pub_plot_dir + tar + '.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

    def width_dot_mr(self):
        xlist = list()
        ylist = list()
        for reaper in self.mr.rlist:
            file = self.event_input_dir + 'events_plusminus.txt'
            f = open(file, 'r')
            for line in f:
                line = line.rstrip('\n')
                the_list = ast.literal_eval(line.split(':')[1])
                rsize = the_list[0]
                width = the_list[4]
                xlist.append(rsize)
                ylist.append(width)
            f.close()

        plt.scatter(xlist, ylist, s=150, facecolor='none', edgecolors='b')
        output_loc = pub_plot_dir + 'width.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

