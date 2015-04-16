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
font = {'size': 28,}

matplotlib.rc('font', **font)
plt.rc('legend',**{'fontsize':18})

colors = ['r', 'b', 'g', 'y', 'm', 'k']

class Plotter():

    def __init__(self, reaper):
        self.reaper = reaper
        self.pfx_input_dir = reaper.get_output_dir_pfx()
        self.event_input_dir = reaper.get_output_dir_event()
        self.pfx_plot_dir = reaper.get_output_dir_pfx() + 'plot/'
        self.event_plot_dir = reaper.get_output_dir_event() + 'plot/'
        cmlib.make_dir(self.pfx_plot_dir)
        cmlib.make_dir(self.event_plot_dir)

    def TS_event_dot(self):
        index = self.reaper.period.index
        try:
            occur_unix = occur_unix_dt[index]
            occur_dt = datetime.datetime.utcfromtimestamp(occur_unix)
        except:
            occur_dt = None

        y_high = 0.03

        value = list()
        dt = list()

        file = self.event_input_dir + 'events_new.txt'
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
        plt.scatter(dt, value, s=100, facecolor='r', edgecolors='none')
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
        ax.set_ylim([0,y_high])

        ax.tick_params(axis='y',pad=10)
        ax.tick_params(axis='x',pad=10)
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

        # polt a line
        if occur_dt is not None:
            plt.plot((occur_dt, occur_dt), (0, y_high), 'k--', lw=4)

        output_loc = self.event_plot_dir + str(self.reaper.period.index) + '_TS_event_dot.pdf'
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
