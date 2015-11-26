import datetime
import numpy as np
import cmlib
import operator
import os
import ast
import calendar # do not use the time module

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

# used when plotting
metric_tag2name = {'1':'CR1', '0.1':'CR0.1',}
feature_num2name = {0:'updates', 1:'A', 2:'W', 3:'WW', 4:'AADup1', 5:'AADup2',\
                    6:'AADiff', 7:'WAUnknown', 8:'WADup', 9:'WADiff', 10:'AW'}

class Mplotter():

    def __init__(self, uds_list):
        self.uds_list = uds_list
        #self.outdir = 

    def num_features_metrics_TS(self): 
        fea2unix2met2v = dict()

        # initialize the huge dict
        print 'Initializing ...'
        tmppath = self.uds_list[0].numf_metrics_fpath()
        with open(tmppath, 'r') as f:
            first_line = f.readline().strip().rstrip('\n')
            print 'first line:', first_line
            thedict = ast.literal_eval(first_line.split('|')[2])
            for fea in thedict:
                fea2unix2met2v[fea] = dict()

        unix_list = list()
        for uds in self.uds_list:
            for dtobj in uds.dtobj_list:
                unix = calendar.timegm(dtobj[0].utctimetuple())
                unix_list.append(unix)

        for f in fea2unix2met2v.keys():
            for unix in unix_list:
                fea2unix2met2v[f][unix] = dict()

        
        # read output file and store information
        for uds in self.uds_list:
            mf_path = uds.numf_metrics_fpath()
            print 'Reading ', mf_path
            f = open(mf_path, 'r')
            for line in f:
                line = line.rstrip('\n')
                splitted = line.split('|')
                unix = int(splitted[0])
                mtype = splitted[1]
                thedict = ast.literal_eval(splitted[2])
                for fea in thedict:
                    value = thedict[fea]
                    fea2unix2met2v[fea][unix][mtype] = value
            f.close()
            # plot all the metrics TS for each feature in ONE figure

        for fea in fea2unix2met2v:
            print 'Plotting ', fea
            fig = plt.figure(figsize=(50, 100))

            m2dt_list = dict()
            m2value_list = dict() # metric type -> value list
            for unix in fea2unix2met2v[fea]:
                for mtype in fea2unix2met2v[fea][unix]:
                    value = fea2unix2met2v[fea][unix][mtype]
                    try:
                        m2dt_list[mtype].append(datetime.datetime.utcfromtimestamp(unix))
                        m2value_list[mtype].append(value)
                    except:
                        m2dt_list[mtype] = [datetime.datetime.utcfromtimestamp(unix)]
                        m2value_list[mtype] = [value]

            subsum = len(m2dt_list.keys())
            count = 1
            for mtype in m2dt_list.keys():
                ax = fig.add_subplot(subsum, 1, count)
                count += 1

                plt.scatter(m2dt_list[mtype], m2value_list[mtype])
                myFmt = mpldates.DateFormatter('%b\n%d')
                ax.xaxis.set_major_formatter(myFmt)
                ax.set_ylabel(str(mtype))

            plt.savefig('test'+feature_num2name[fea]+'.pdf', bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()

