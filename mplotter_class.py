import datetime
import numpy as np
import cmlib
import operator
import os
import ast

from operator import add
import matplotlib
# This is useful. I can render figures thourgh ssh. VNC viewer in unnecessary.
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates
from matplotlib.lines import Line2D

line_type = ['k--', 'k-', 'k^-'] # line type (hard code)
font = {'size': 38,}
#font = {'size': 80,}

matplotlib.rc('font', **font)
plt.rc('legend',**{'fontsize':34})

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

class Mplotter():

    def __init__(self, uds):
        self.uds = uds
        #self.outdir = 

    def num_features_metrics_TS(self): 
        # get file path
        # open file and read the time series as dicts
        # feature -> metric -> slot -> value
        # plot all the metrics TS for each feature in one figure
        pass
