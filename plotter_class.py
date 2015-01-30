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

class Plotter():

    def __init__(self, reaper):
        input_dir = reaper.get_output_dir()
        plot_dir = reaper.get_output_dir() + 'plot/'
        cmlib.make_dir(plot_dir)

        files = os.listdir(input_dir)
        print files
        for f in files:
            if os.path.isfile(input_dir+f):
                files.remove(f)
                files.insert(0, input_dir+f)
            else:
                files.remove(f)

        for f in files:
            if '_ts' in f:
                pdfname = f.split('/')[-1].split('.')[0] + '.pdf'
                print 'Plotting ', plot_dir+pdfname
                self.basic_ts(f, plot_dir+pdfname)
            elif '_distr' in f:
                pdfname = f.split('.')[0] + '.pdf'
                #self.basic_distr(f, plot_dir+pdfname)
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
        ax.plot(counter, valuelist, 'k-')

        # set the parameters
        ax.set_xlabel('time slots since ...')
        ax.set_ylabel('value')

        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()

    #def basic_distr(self, input_loc, output_loc):
