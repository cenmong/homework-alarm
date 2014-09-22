import subprocess
import re
import nltk
import numpy as np
import matplotlib
# This is useful. I can render figures thourgh ssh. VNC viewer in unnecessary.
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
import datetime
import gzip
import time as time_lib
import operator
import cmlib

from matplotlib.dates import HourLocator
from matplotlib.dates import DayLocator
from matplotlib.patches import Ellipse
from matplotlib.patches import Rectangle
from netaddr import *
from env import *

line_type = ['k--', 'k-', 'k^-'] # line type (hard code)

font = {
    #'weight': 'bold',
    'size': 38,}
matplotlib.rc('font', **font)
plt.rc('legend',**{'fontsize':38})

el = Ellipse((2,-1),0.5,0.5)

def mean_cdf(fname, xlab, ylab):
    mydict = dict()
    with open(fname, 'r') as f:
        data = f.read()
        items = data.split('|')
        for item in items: 
            if item == '' or item == '\n': # last one
                continue
            item = item.split(',')
            axis = float(item[0])
            mean = float(item[1])
            dev = float(item[2])
            mydict[axis] = [mean, dev]
    f.close()

    xlist = []
    ylist = []
    ylist_low = []
    ylist_high = []
    pre_mean = -1
    pre_dev = -1
    same = 0
    for key in sorted(mydict): # sort by key
        mean = mydict[key][0]
        dev = mydict[key][1]
        if mean == pre_mean and dev == pre_dev:
            same += 1
        else:
            pre_mean = mean
            pre_dev = dev
            same = 0

        if same == 10:
            break

        xlist.append(key)
        ylist.append(mydict[key][0])
        ylist_low.append(mydict[key][0]-mydict[key][1])
        ylist_high.append(mydict[key][0]+mydict[key][1])

    xmax = max(xlist)
    ymax = max(ylist_high)

    fig = plt.figure(figsize=(16, 12))
    ax = fig.add_subplot(111)
    ax.set_ylim([-0.1*ymax, 1.1*ymax])
    ax.set_xlim([-0.1*xmax, 1.02*xmax])
    ax.set_ylabel(ylab)
    ax.set_xlabel(xlab)
    ax.plot(xlist, ylist, 'k-')
    ax.plot(xlist, ylist_low, 'k--')
    ax.plot(xlist, ylist_high, 'k--')

    plotfname = fname.rpartition('/')[0] + '/' +  fname.rpartition('/')[2] + '.pdf'
    plt.savefig(plotfname, bbox_inches='tight')
    plt.close()

def mean_cdfs_multi(fname, xlab, ylab):
    f = open(fname, 'r')
    for line in f:
        mydict = dict()
        new_name = line.split(':')[0]
        items = line.split(':')[1]
        for item in items.split('|'): 
            if item == '' or item == '\n': # last one
                continue
            item = item.split(',')
            axis = float(item[0])
            mean = float(item[1])
            dev = float(item[2])
            mydict[axis] = [mean, dev]

        xlist = []
        ylist = []
        ylist_low = []
        ylist_high = []
        pre_mean = -1
        pre_dev = -1
        same = 0
        for key in sorted(mydict): # sort by key
            mean = mydict[key][0]
            dev = mydict[key][1]
            if mean == pre_mean and dev == pre_dev:
                same += 1
            else:
                pre_mean = mean
                pre_dev = dev
                same = 0

            if same == 10:
                break

            xlist.append(key)
            ylist.append(mydict[key][0])
            ylist_low.append(mydict[key][0]-mydict[key][1])
            ylist_high.append(mydict[key][0]+mydict[key][1])

        xmax = max(xlist)
        ymax = max(ylist_high)

        fig = plt.figure(figsize=(16, 12))
        ax = fig.add_subplot(111)
        ax.set_ylim([-0.1*ymax, 1.1*ymax])
        ax.set_xlim([-0.1*xmax, 1.02*xmax])
        ax.set_ylabel(ylab)
        ax.set_xlabel(xlab)
        ax.plot(xlist, ylist, 'k-')
        ax.plot(xlist, ylist_low, 'k--')
        ax.plot(xlist, ylist_high, 'k--')

        plotfname = fname.rpartition('/')[0] + '/' +\
                fname.rpartition('/')[2] + new_name + '.pdf'
        plt.savefig(plotfname, bbox_inches='tight')
        plt.close()

    f.close()

def cdfs_one(fname, xlab, ylab):
    colors = ['k', 'b', 'r', 'g', 'y', 'm']

    mydicts = dict() # type: dict
    f = open(fname, 'r')
    for line in f:
        my_legend = line.split(':')[0]
        mydicts[my_legend] = dict()
        items = line.split(':')[1]
        for item in items.split('|'): 
            if item == '' or item == '\n': # last one
                continue
            item = item.split(',')
            xvalue = float(item[0])
            yvalue = float(item[1])
            mydicts[my_legend][xvalue] = yvalue
    f.close()

    xlists = dict()
    ylists = dict()
    for my_legend in mydicts.keys():
        xlists[my_legend] = []
        ylists[my_legend] = []
        for key in sorted(mydicts[my_legend]):
            xlists[my_legend].append(key)
            ylists[my_legend].append(mydicts[my_legend][key])

    tmp_legend = xlists.keys()[0]
    xmax = max(xlists[tmp_legend])
    ymax = max(ylists[tmp_legend])

    fig = plt.figure(figsize=(16, 12))
    ax = fig.add_subplot(111)
    ax.set_ylim([-0.1*ymax, 1.1*ymax])
    ax.set_xlim([-0.1*xmax, 1.02*xmax])
    ax.set_ylabel(ylab)
    ax.set_xlabel(xlab)
    color_count = 0
    for key in xlists.keys():
        ax.plot(xlists[key], ylists[key], colors[color_count]+'-')
        color_count += 1

    plotfname = fname.rpartition('/')[0] + '/' +\
            fname.rpartition('/')[2] + '.pdf'
    plt.savefig(plotfname, bbox_inches='tight')
    plt.close()

# TODO hard code bad
def boxes(fname, xlab, ylab):
    test = [[12,2,3,4,5,2,7,6,5,5,5,6],[12,13,11,15,1,25]]
    fig = plt.figure(figsize=(16, 12))
    ax = fig.add_subplot(111)
    
    ax.boxplot(test, showmeans=True)

    plotfname = fname.rpartition('/')[0] + '/' + 'test.pdf'
    plt.savefig(plotfname, bbox_inches='tight')
    plt.close()

def cdf_plot(granu, my_dict, describe):
    # my_dict DV value: exist time
    xlist = [0]
    ylist = [0]
    for key in sorted(my_dict): # must sort by key
        xlist.append(key)
        ylist.append(my_dict[key])

    xmax = max(xlist)
    ymax = max(ylist)

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    ax.plot(xlist, ylist, 'k-')
    ax.set_ylim([-0.1*ymax, 1.1*ymax])
    ax.set_xlim([-0.1*xmax, 1.1*xmax])
    ax.set_ylabel('y')
    ax.set_xlabel('x')

    # make a dir according to datetime, granularity and h threshold
    sdate = describe.split('_')[0]
    cmlib.make_dir(hdname+'output/'+sdate+'_'+str(granu)+'/')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'.pdf')
    plt.close()

    # Record plot data in a separate file for future use
    f = open(hdname+'output/'+sdate+'_'+str(granu)+'/'+\
            describe+'.txt', 'w')
    for i in xrange(0, len(xlist)):
        f.write(str(xlist[i])+','+str(ylist[i])+'\n')
    f.close()

    return 0

def time_series_plot(granu, my_dict, describe): 
    value = []

    dt = my_dict.keys()
    dt.sort()
    for key in dt:
        value.append(my_dict[key])
    dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj. required!
    
    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    ax.plot(dt, value, 'k-')
    ax.set_ylabel(describe)
    ax.set_xlabel('Datetime')
    myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
    ax.xaxis.set_major_formatter(myFmt)
    plt.xticks(rotation=45)

    # make a dir according to datetime, granularity and h threshold
    sdate = describe.split('_')[0]
    cmlib.make_dir(hdname+'output/'+sdate+'_'+str(granu)+'/')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'.pdf')
    plt.close()

    # Record plot data in a separate file for future use
    f = open(hdname+'output/'+sdate+'_'+str(granu)+'/'+\
            describe+'.txt', 'w')
    for i in xrange(0, len(dt)):
        f.write(str(dt[i])+','+str(value[i])+'\n')
    f.close()

    return 0

# TODO: not used yet
def box_plot_grouped(granu, my_dict, describe):
    data_lists = []
    for k in my_dict.keys(): # dv ranges
        tmp_list = []
        for k2 in my_dict[k].keys():
            for i in xrange(0, len(my_dict[k][k2])):        
                tmp_list.append(k2)
        data_lists.append(tmp_list)

    #plot_lists = []
    #large = 0 # the number of sub lists
    #for list in data_lists:
        #if len(list) > large:
            #large = len(list)
    #for i in xrange(0, large):
        #for j in xrange(0, len(data_lists)):
            #tmp_list = []
            #try:
                #tmp_list.append(data_lists[j][i])
            #except:
                #tmp_list.append(0)
            #plot_lists.append(tmp_list)

    #my_labels = my_dict.keys() 
    #fig = plt.figure(figsize=(16, 10))
    #ax = fig.add_subplot(111)
    #ax.boxplot(data_lists)

    # make a dir according to datetime, granularity and h threshold
    sdate = describe.split('_')[0]
    cmlib.make_dir(hdname+'output/'+sdate+'_'+str(granu)+'/')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'.pdf',\
            bbox_inches='tight')
    plt.close()

    # Record plot data in a separate file for future use
    f = open(hdname+'output/'+sdate+'_'+str(granu)+'_'+'/'+\
            describe+'.txt', 'w')
    for k in my_dict.keys():
        f.write(str(k)+':')
        for k2 in my_dict[k].keys():
            f.write(str(k2)+'|')
            f.write(str(my_dict[k][k2]))
            f.write(',')
        f.write('\n')
    f.close()

    return 0

def direct_ts_plot(granu, describe, thres, soccur, eoccur, des):
    sdate = describe.split('_')[0]
    count_peak = 0
    fname =\
            hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'.txt'
    dt = []
    value = []
    circlex = []
    circley = []
    detectx = -1
    detecty = -1

    f = open(fname, 'r')
    for line in f:
        line = line.replace('\n', '').split(',')
        tmp = line[0]
        tmp = datetime.datetime.strptime(tmp, '%Y-%m-%d %H:%M:%S')
        dt.append(tmp)
        value.append(float(line[1]))
        # novelty detection
        if float(line[1]) > thres:
            count_peak += 1
            circlex.append(tmp)
            circley.append(float(line[1]))
    f.close()

    if 'dvi' in describe:
        print sdate,':',count_peak
        print circley

    for j in xrange(0, len(circlex)):
        circlex[j] = mpldates.date2num(circlex[j])

    try:
        detectx = circlex[0]
        detecty = circley[0]
    except:
        pass

    if soccur != '':
        occur_dt = datetime.datetime.strptime(soccur, '%Y-%m-%d %H:%M:%S')
        soccur = datetime.datetime.strptime(soccur, '%Y-%m-%d %H:%M:%S')

    if eoccur != '': # '' means not a range
        eoccur = datetime.datetime.strptime(eoccur, '%Y-%m-%d %H:%M:%S')

    if len(dt) > 600:
        dt = dt[:577]
        value = value[:577]

    sdt = dt[0]
    edt = dt[-10]+datetime.timedelta(days=1)
    edt = edt.replace(hour=0,minute=0,second=0,microsecond=0)

    if 'update' in describe:
        for i in xrange(0, len(value)):
            value[i] = float(value[i]*0.00001)
    if 'prefix' in describe:
        for i in xrange(0, len(value)):
            value[i] = float(value[i]*0.0001)

    # Plotting
    fig = plt.figure(figsize=(20, 10))
    if 'prefix' in describe or 'update' in describe:
        fig = plt.figure(figsize=(20, 11))
    ax = fig.add_subplot(111)
    ax.plot(dt, value, 'k-')
    ax.set_xlim([mpldates.date2num(sdt), mpldates.date2num(edt)])

    # setting axises
    ax.xaxis.set_major_locator(HourLocator(byhour=None, interval=12, tz=None))
    ax.xaxis.set_minor_locator(HourLocator(byhour=None, interval=2, tz=None))
    ax.xaxis.set_tick_params(which='major', width=4, size=8)
    ax.xaxis.set_tick_params(which='minor', width=2, size=4)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    myFmt = mpldates.DateFormatter('%H:00\n%b%d')
    ax.xaxis.set_major_formatter(myFmt)

    # TODO: set these parameters in function parameter
    x1 = -180
    y1 = 350
    x2 = -50
    y2 = 50
    if sdate == '20130213':
        x1 = 20
        y1 = 300
        x2 = -180
        y2 = -100
    if sdate == '20061225':
        x2 = -75
    if sdate == '20100226':
        x2 = -40
        
    # add annotation
    if 'prefix' not in describe:
        if eoccur == '':
            if soccur != '':
                ax.annotate(des,(mpldates.date2num(occur_dt),0),xytext=(x1,y1),textcoords='offset\
                        points',arrowprops=dict(arrowstyle='simple',fc='0.3',ec='none',\
                        connectionstyle='arc3',alpha=0.5))
        else: # happen inside a range
            ax.annotate(des,(mpldates.date2num(soccur),0),xytext=(0, 465),textcoords='offset\
                    points',)
            plt.axvspan(mpldates.date2num(soccur),mpldates.date2num(eoccur),facecolor='0.3',alpha=0.3)

    if 'dvi' in describe :
        ax.set_ylabel('Dynamic Visibility Index')
        ax.set_ylim([0, 3])

        if not detectx == -1: # really detected
            ax.annotate('Detected',(detectx,detecty),xytext=(x2,y2),textcoords='offset\
                    points',arrowprops=dict(arrowstyle='->',\
                    connectionstyle='arc3'))
        # all novelties
        plt.scatter(circlex,circley,s=80,facecolors='none',edgecolors='k')
    
    if 'prefix' in describe:
        ax.set_ylabel('Prefix quantity')
    if 'update' in describe:
        ax.set_ylabel('Update quantity')

    # 10^x
    if 'dvi' in describe:
        ax.annotate(r'$\times10^{-2}$',(mpldates.date2num(sdt),0),xytext=(0, 585),textcoords='offset\
                points',)
    if 'update' in describe:
        ax.annotate(r'$\times10^{5}$',(mpldates.date2num(sdt),0),xytext=(0, 645),textcoords='offset\
                points',)
    if 'prefix' in describe:
        ax.annotate(r'$\times10^{4}$',(mpldates.date2num(sdt),0),xytext=(0, 645),textcoords='offset\
                points',)

    ax.tick_params(axis='y',pad=10)
    # save figure
    if 'dvi' in describe:
        plt.savefig(hdname+'output/'+sdate+'.pdf',\
                bbox_inches='tight')
    if 'update' in describe:
        if sdate == '20030813' or sdate == '20110310':
            plt.savefig(hdname+'output/'+sdate+'update.pdf',\
                    bbox_inches='tight')
    if 'prefix' in describe:
        if sdate == '20030813' or sdate == '20110310':
            plt.savefig(hdname+'output/'+sdate+'prefix.pdf',\
                    bbox_inches='tight')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'_new.pdf')
    plt.close()

def direct_cdf_plot(granu, describe):
    sdate = describe.split('_')[0]
    fname =\
            hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'.txt'
    xlist = []
    ylist = []
    f = open(fname, 'r')
    for line in f:
        line = line.replace('\n', '').split(',')
        xlist.append(float(line[0]))
        ylist.append(float(line[1]))
    f.close()

    # TODO: do not hard code these values
    t1 = 80
    t2 = 95
    t3 = 98
    x1, y1 = 0, 0
    x2, y2 = 0, 0
    x3, y3 = 0, 0

    for j in xrange(0, len(ylist)):
        if ylist[j] > t1:
            x1 = xlist[j]
            y1 = ylist[j]
            break
    for j in xrange(0, len(ylist)):
        if ylist[j] > t2:
            x2 = xlist[j]
            y2 = ylist[j]
            break
    for j in xrange(0, len(ylist)):
        if ylist[j] > t3:
            x3 = xlist[j]
            y3 = ylist[j]
            break

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    ax.plot(xlist, ylist, 'k-')
    ax.set_ylim([0,105])
    ax.set_xlim([-5,70])
    ax.set_ylabel('prefix-time (%) CDF')
    ax.set_xlabel('route monitor (%)')

    # annotate
    plt.plot([x1,x1],[0,y1],'k--',lw=4)
    plt.plot([-5,x1],[y1,y1],'k--',lw=4)
    ax.annotate('p1',(x1,y1),xytext=(20,-30),textcoords='offset points',)
    plt.plot([x2,x2],[0,y2],'k--',lw=4)
    plt.plot([-5,x2],[y2,y2],'k--',lw=4)
    ax.annotate('p2',(x2,y2),xytext=(20,-30),textcoords='offset points',)
    plt.plot([x3,x3],[0,y3],'k--',lw=4)
    plt.plot([-5,x3],[y3,y3],'k--',lw=4)
    ax.annotate('p3',(x3,y3),xytext=(20,-30),textcoords='offset points',)
    #print 'p1',x1,y1
    #print 'p2',x2,y2
    #print 'p3',x3,y3

    sdate = describe.split('_')[0]
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'/'+describe+'_new.pdf')
    plt.close()

def combine_ts_diffgranu(fdict, xlabel, ylabel):
    #fdict = {fname:[legend, cut]}
    flist = []
    lglist = [] # legend list
    cutlist = []
    for fname in sorted(fdict):
        flist.append(fname)
        lglist.append(fdict[fname][0])
        cutlist.append(fdict[fname][1])

    xlists = []
    ylists = []

    for fname in flist:
        dt = []
        value = []
        f = open(fname, 'r')
        for line in f:
            line = line.replace('\n', '').split(',')
            tmp = line[0]
            tmp = datetime.datetime.strptime(tmp, '%Y-%m-%d %H:%M:%S')
            dt.append(tmp)
            value.append(float(line[1]))
        f.close()
        xlists.append(dt)
        ylists.append(value)

    # Plotting
    for i in xrange(0, len(flist)):
        xlists[i] = xlists[i][0:cutlist[i]]
        ylists[i] = ylists[i][0:cutlist[i]]

    sdt = xlists[1][0]+datetime.timedelta(minutes=1)
    edt = xlists[1][-10]+datetime.timedelta(days=1)
    edt = edt.replace(hour=0,minute=0,second=0,microsecond=0)

    fig = plt.figure(figsize=(20, 10))
    ax = fig.add_subplot(111)
    for i in xrange(0, len(flist)):
        ax.plot(xlists[i], ylists[i], line_type[i], label=lglist[i])

    legend = ax.legend(loc='upper left',shadow=False)
    ax.set_xlim([mpldates.date2num(sdt), mpldates.date2num(edt)])
    # setting axises
    ax.xaxis.set_major_locator(HourLocator(byhour=None, interval=3, tz=None))
    ax.xaxis.set_minor_locator(HourLocator(byhour=None, interval=1, tz=None))
    ax.xaxis.set_tick_params(which='major', width=4, size=8)
    ax.xaxis.set_tick_params(which='minor', width=2, size=4)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    myFmt = mpldates.DateFormatter('%H:00\n%b%d')
    ax.xaxis.set_major_formatter(myFmt)
    # add the label 10^x to the left upper side of the figure
    ax.annotate(r'$\times10^{-2}$',(mpldates.date2num(sdt),0),xytext=(0, 585),textcoords='offset\
            points',)
    # save figure
    ax.tick_params(axis='y',pad=10)
    ax.set_ylabel(ylabel)
    plt.savefig(hdname+'output/combine_slot_'+flist[0]+'.pdf',\
            bbox_inches='tight')
    plt.close()

def combine_ts_samegranu(fdict, xlabel, ylabel): 
    #fdict = {fname:[legend, cut]}
    flist = []
    lglist = [] # legend list
    cutlist = []
    for fname in sorted(fdict):
        flist.append(fname)
        lglist.append(fdict[fname][0])
        cutlist.append(fdict[fname][1])

    xlists = []
    ylists = []
    
    for fname in flist:
        dt = []
        value = []
        f = open(fname, 'r')
        for line in f:
            line = line.replace('\n', '').split(',')
            tmp = line[0]
            tmp = datetime.datetime.strptime(tmp, '%Y-%m-%d %H:%M:%S')
            dt.append(tmp)
            value.append(float(line[1]))
        f.close()
        xlists.append(dt)
        ylists.append(value)

    for i in xrange(0, len(flist)):
        xlists[i] = xlists[i][0:cutlist[i]]
        ylists[i] = ylists[i][0:cutlist[i]]

    # set x range to exactly a day
    sdt = xlists[0][0]+datetime.timedelta(minutes=1)
    edt = xlists[0][-1]+datetime.timedelta(days=1)
    edt = edt.replace(hour=0,minute=0,second=0,microsecond=0)

    fig = plt.figure(figsize=(20, 10))
    ax = fig.add_subplot(111)
    for i in xrange(0, len(flist)):
        ax.plot(xlists[i], ylists[i], line_type[i], label=lglist[i])
    
    # TODO: do not hard code
    legend = ax.legend(loc='upper center',bbox_to_anchor=(0.43,1),shadow=False) 

    ax.set_xlim([mpldates.date2num(sdt), mpldates.date2num(edt)])

    # setting axises
    ax.xaxis.set_major_locator(HourLocator(byhour=None, interval=3, tz=None))
    ax.xaxis.set_minor_locator(HourLocator(byhour=None, interval=1, tz=None))
    ax.xaxis.set_tick_params(which='major', width=4, size=8)
    ax.xaxis.set_tick_params(which='minor', width=2, size=4)
    ax.xaxis.set_ticks_position('bottom')
    ax.yaxis.set_ticks_position('left')
    myFmt = mpldates.DateFormatter('%H:00\n%b%d')
    ax.xaxis.set_major_formatter(myFmt)

    ax.set_ylabel(ylabel)
    ax.tick_params(axis='y',pad=10)
    # save figure
    plt.savefig(hdname+'output/combine_ts_samegranu_'+flist[0]+'.pdf',\
            bbox_inches='tight')
    plt.close()

def combine_cdf(fdict, xlabel, ylabel): # fdict {filename: legend}
    flist = []
    lglist = [] # legend list
    for fname in sorted(fdict):
        flist.append(fname)
        lglist.append(fdict[fname])

    xlists = [] # list of list
    ylists = []
    for fname in flist:
        xl = []
        yl = []
        f = open(fname, 'r')
        for line in f:
            line = line.replace('\n', '').split(',')
            xl.append(float(line[0]))
            yl.append(100-float(line[1])) # this is actually CCDF
        f.close()
        xlists.append(xl)
        ylists.append(yl)

    interest = [5,10,20] # get exact data later and write in the paper
    x1, y1 = 0, 0
    x2, y2 = 0, 0
    x3, y3 = 0, 0

    # get data and write paper
    for t in interest:
        for ii in xrange(0, len(xlists)):
            for j in xrange(0, len(xlists[ii])):
                if xlists[ii][j] > t:
                    print xlists[ii][j]
                    print ylists[ii][j]
                    break

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    for i in xrange(0, len(flist)):
        ax.plot(xlists[i], ylists[i], line_type[i], label=lglist[i])
    legend = ax.legend(loc='upper right',shadow=False)

    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    # TODO: do not hard code these values
    ax.set_ylim([-2,102])
    ax.set_xlim([-5,75])
    ax.tick_params(axis='y', pad=10)
    plt.savefig(hdname+'output/combine_cdf_'+flist[0]+'.pdf', bbox_inches='tight')
    plt.close()
