import datetime
import numpy as np
import cmlib
import operator
import os
import ast
import calendar # do not use the time module
import env

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
          'mediumpurple', 'salmon', 'lime', 'hotpink', 'yellow',\
          'firebrick', 'sienna', 'sandybrown', 'y', 'teal']
shapes = ['^', '*', 'D', 'd']
cluster_labels = ['Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4']
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

    def num_features_metrics_TS_met2total(self): 
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


        '''
        # plot all the metrics TS for each feature in ONE figure
        for fea in fea2unix2met2v:
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

            print 'Plotting TS for ', fea
            fig = plt.figure(figsize=(50, 100))
            subsum = len(m2dt_list.keys())
            count = 1
            for mtype in m2dt_list.keys():
                ax = fig.add_subplot(subsum, 1, count)
                count += 1

                plt.scatter(m2dt_list[mtype], m2value_list[mtype])
                myFmt = mpldates.DateFormatter('%b\n%d')
                ax.xaxis.set_major_formatter(myFmt)
                ax.set_ylabel(str(mtype))

            plt.savefig(env.metric_plot_dir+'TS_'+feature_num2name[fea]+'.pdf', bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()
        '''

        # plot all the metrics TS for each feature in ONE figure
        m2unix2v = dict()
        for fea in fea2unix2met2v:
            for unix in fea2unix2met2v[fea]:
                for mtype in fea2unix2met2v[fea][unix]:
                    value = fea2unix2met2v[fea][unix][mtype]
                    try:
                        m2unix2v[mtype][unix] = value
                    except:
                        m2unix2v[mtype] = dict()
                        m2unix2v[mtype][unix] = value

            print 'Plotting total/metric correlation for ', fea
            fig = plt.figure(figsize=(50, 200))
            subsum = len(m2unix2v.keys())
            count = 1
            for mtype in m2unix2v.keys():
                if mtype == 'TOTAL':
                    continue

                ax = fig.add_subplot(subsum-1, 1, count)
                count += 1

                xlist = list()
                ylist = list()
                for unix in m2unix2v[mtype]:
                    xlist.append(m2unix2v['TOTAL'][unix])
                    ylist.append(m2unix2v[mtype][unix])

                plt.scatter(xlist, ylist)
                ax.set_ylabel(str(mtype))
                ax.set_xlabel('TOTAL')
                ax.set_xscale('log')

            plt.savefig(env.metric_plot_dir+'met2total_'+feature_num2name[fea]+'.pdf', bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()


    def num_features_metrics_CDF(self): 
        met2unix2fea2v = dict() # Note: different to fea2unix2met2v

        # initialize the huge dict
        print 'Initializing ...'
        tmppath = self.uds_list[0].numf_metrics_fpath()
        f = open(tmppath, 'r')
        count = 0
        for line in f:
            mtype = line.split('|')[1]
            met2unix2fea2v[mtype] = dict()
            count += 1
            if count == 15: # XXX Note: we assume at most 15 metrics
                break
        f.close()

        unix_list = list()
        for uds in self.uds_list:
            for dtobj in uds.dtobj_list:
                unix = calendar.timegm(dtobj[0].utctimetuple())
                unix_list.append(unix)

        for m in met2unix2fea2v.keys():
            for unix in unix_list:
                met2unix2fea2v[m][unix] = dict()

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
                    met2unix2fea2v[mtype][unix][fea] = value
            f.close()

        # Plot M figures for M metrics. In each figure, N curves for N features.
        for mtype in met2unix2fea2v:
            print 'Plotting metric ', mtype

            fea2vlist = dict()
            for unix in met2unix2fea2v[mtype]:
                for fea in met2unix2fea2v[mtype][unix]:
                    value = met2unix2fea2v[mtype][unix][fea]
                    try:
                        fea2vlist[fea].append(value)
                    except:
                        fea2vlist[fea] = [value]

            fea2xlist = dict()
            fea2ylist = dict()
            for fea in fea2vlist:
                v2count = dict()
                for v in fea2vlist[fea]:
                    try:
                        v2count[v] += 1
                    except:
                        v2count[v] = 1

                mycdf = cmlib.value_count2cdf(v2count)
                for key in sorted(mycdf):
                    try:
                        fea2xlist[fea].append(key)
                        fea2ylist[fea].append(mycdf[key])
                    except:
                        fea2xlist[fea] = [key]
                        fea2ylist[fea] = [mycdf[key]]


            # Start plotting now! 
            fig = plt.figure(figsize=(20, 20))
            ax = fig.add_subplot(111)
            count = 1
            for fea in fea2xlist:
                count += 1
                ax.plot(fea2xlist[fea], fea2ylist[fea],\
                        color=colors[count], label=feature_num2name[fea])

            ax.set_ylabel('Cumulative distribution (slots)')
            ax.set_xlabel(' Metric value')
            legend = ax.legend(loc='upper left',shadow=False)

            cmlib.make_dir(env.metric_plot_dir)
            output_loc = env.metric_plot_dir + 'CDF_' + str(mtype) + '.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()


    def pfx_metrics_CDF_met2total(self):
        uds = self.uds_list[0] # XXX note: in the ISCC paper we only analyze one period!
        metrics = ['UQ', 'DV', 'GINI', 'CR1', 'CR4', 'CR8', 'CR0.1', 'CR0.2', 'CR0.3']
        met2list = dict()
        for met in metrics:
            met2list[met] = list()

        fname = uds.apfx_metrics_fpath()
        f = open(fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            attr = line.split('|')
            met2list['UQ'].append(int(attr[1])) # get the metrics. Hard-coding is bad. But we save time here.
            met2list['DV'].append(float(attr[2]))
            met2list['GINI'].append(float(attr[3]))
            met2list['CR1'].append(float(attr[4]))
            met2list['CR4'].append(float(attr[5]))
            met2list['CR8'].append(float(attr[6]))
            met2list['CR0.1'].append(float(attr[7]))
            met2list['CR0.2'].append(float(attr[8]))
            met2list['CR0.3'].append(float(attr[9]))
        f.close()


        '''
        fig = plt.figure(figsize=(20, 20))
        ax = fig.add_subplot(111)
        count = 0
        for mtype in met2list:
            if mtype == 'UQ':
                continue
            v2count = dict()
            for v in met2list[mtype]:
                try:
                    v2count[v] += 1
                except:
                    v2count[v] = 1
            mycdf = cmlib.value_count2cdf(v2count)
            xlist = list()
            ylist = list()

            for key in sorted(mycdf):
                xlist.append(key)
                ylist.append(mycdf[key])

            ax.plot(xlist, ylist, color=colors[count], label=mtype)
            count += 1

        ax.set_ylabel('Cumulative distribution (slots)')
        ax.set_xlabel(' Metric value')
        legend = ax.legend(loc='upper left',shadow=False)
        
        cmlib.make_dir(env.metric_plot_dir)
        output_loc = env.metric_plot_dir + 'pfx_metrics_CDF.pdf'
        plt.savefig(output_loc, bbox_inches='tight')
        plt.clf() # clear the figure
        plt.close()
        '''


        tlist = met2list['UQ']
        mcount = len(metrics) - 1
        count = 1
        for mtype in met2list:
            if mtype == 'UQ':
                continue
            fig = plt.figure(figsize=(20, 20))
            ax = fig.add_subplot(mcount, 1, count)
            count += 1

            plt.scatter(tlist, met2list[mtype])
            ax.set_ylabel(str(mtype))
            ax.set_xlabel('TOTAL')
            ax.set_xscale('log')

            output_loc = env.metric_plot_dir + 'pfx_met2total_' + mtype + '.pdf'
            plt.savefig(output_loc, bbox_inches='tight')
            plt.clf() # clear the figure
            plt.close()
