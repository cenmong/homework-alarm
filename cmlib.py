import os
import urllib
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
import patricia
import gzip
import time as time_lib

from matplotlib.dates import HourLocator
from netaddr import *
from env import *

def download_file(url, save_loc, filename):
    make_dir(save_loc)
    if os.path.exists(save_loc+filename):
        return
    while 1:
        try:
            urllib.urlretrieve(url+filename, save_loc+filename)
            break
        except:
            pass

def force_download_file(url, save_loc, filename):
    make_dir(save_loc)
    while 1:
        try:
            urllib.urlretrieve(url+filename, save_loc+filename)
            break
        except:
            pass

def get_unpack_gz(url, save_loc, filename):
    if not os.path.exists(save_loc+re.sub('\.gz$', '', filename)):
        urllib.urlretrieve(url+filename, save_loc+filename)
        subprocess.call('gunzip '+save_loc+filename, shell=True)
    else:
        pass

def get_unpack_bz2(url, save_loc, filename):
    if not os.path.exists(save_loc+re.sub('\.bz2$', '', filename)):
        urllib.urlretrieve(url+filename, save_loc+filename)
        subprocess.call('bunzip2 -d '+save_loc+filename, shell=True)
    else:
        pass

def pack_gz(loc_fname):
    if not os.path.exists(loc_fname+'.gz'):
        subprocess.call('gzip '+loc_fname, shell=True)
    else:
        pass

def make_dir(location):
    if not os.path.isdir(location):
        os.makedirs(location)

def get_weblist(url):
    while 1:
        try:
            webhtml = urllib.urlopen(url).read()
            webraw = nltk.clean_html(webhtml)
            break
        except:
            pass
    return webraw

def parse_mrt(old_loc_fname, new_loc_fname):
    if not os.path.exists(new_loc_fname):
        subprocess.call('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m '+\
                old_loc_fname+' > '+new_loc_fname, shell=True)
    else:  # file already exists
        pass

def simple_plot(active_t, granu, my_dict, describe): # start date is always first attribute
    value = []
    dt = my_dict.keys()
    dt.sort()
    for key in dt:
        value.append(my_dict[key])
    dt = [datetime.datetime.fromtimestamp(ts) for ts in dt]  # int to obj
    
    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    ax.plot(dt, value, 'b-')
    ax.set_ylabel(describe)
    ax.set_xlabel('Datetime')
    myFmt = mpldates.DateFormatter('%Y-%m-%d %H%M')
    ax.xaxis.set_major_formatter(myFmt)
    plt.xticks(rotation=45)

    sdate = describe.split('_')[0]
    make_dir(hdname+'output/'+sdate+'_'+str(granu)+'_'+str(active_t)+'/')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'_'+str(active_t)+'/'+describe+'.pdf')
    plt.close()
    return 0

def cdf_plot(active_t, granu, my_dict, describe): # start date is always first attribute
    xlist = [0]
    ylist = [0]
    for key in sorted(my_dict):
        xlist.append(key)
        ylist.append(my_dict[key])

    for i in xrange(1, len(ylist)):
        ylist[i] += ylist[i-1]

    giant = ylist[-1]
    for i in xrange(0, len(ylist)):
        ylist[i] = float(ylist[i])/float(giant)

    fig = plt.figure(figsize=(16, 10))
    ax = fig.add_subplot(111)
    ax.plot(xlist, ylist, 'b-')
    ax.set_ylim([0,1.1])
    ax.set_xlim([-0.1,1])
    ax.set_ylabel('prefix-times (%)')
    ax.set_xlabel('monitor count (%)')

    sdate = describe.split('_')[0]
    make_dir(hdname+'output/'+sdate+'_'+str(granu)+'_'+str(active_t)+'/')
    plt.savefig(hdname+'output/'+sdate+'_'+str(granu)+'_'+str(active_t)+'/'+describe+'.pdf')
    plt.close()
    return 0

def print_dt(dt):
    try:
        print datetime.datetime.fromtimestamp(dt)
    except:
        print dt
    return 0

def ip_to_binary(content, peer):  # can deal with ip addr and pfx
    length = None
    pfx = content.split('/')[0]
    try:
        length = int(content.split('/')[1])
    except:  # an addr, not a pfx
        pass
    if '.' in peer:  # IPv4
        addr = IPAddress(pfx).bits()
        addr = addr.replace('.', '')
        if length:
            addr = addr[:length]
        return addr
    elif ':' in peer:
        addr = IPAddress(pfx).bits()
        addr = addr.replace(':', '')
        if length:
            addr = addr[:length]
        return addr
    else:
        print 'protocol false!'
        return 0

def get_collector(sdate):
    clist = []
    dir_list = os.listdir(hdname+'metadata/'+sdate+'/')
    for f in dir_list:
        if not 'filelist' in f:
            continue
        if 'test' in f:
            continue
        
        cl = f.split('_')[-1]
        if cl == 'comb':
            continue
        clist.append(cl)
    return clist

def size_u2v(unit):
    if unit in ['k', 'K']:
        return 1024
    if unit in ['m', 'M']:
        return 1048576
    if unit in ['g', 'G']:
        return 1073741824

def get_pfx2as_file(sdate):
    location = hdname + 'topofile/' + sdate + '/'
    print 'get pfx2as file ... (only after 2005.06)'
    year, month = sdate[:4], sdate[4:6] # YYYY, MM
    webloc = 'http://data.caida.org/datasets/routing/routeviews-prefix2as' +\
                    '/' + year + '/' + month + '/'
    webraw = get_weblist(webloc)
    for line in webraw.split('\n'):
        if not sdate in line:
            continue
        if os.path.exists(hdname+location+line.split()[0].replace('.gz',\
                    '')):
            break

        make_dir(location)
        urllib.urlretrieve(webloc+line.split()[0], location+line.split()[0])
        subprocess.call('gunzip -c '+location+line.split()[0]+' > '+\
                location+line.split()[0].replace('.gz', ''), shell=True)
        os.remove(location+line.split()[0])

def get_monitor_c(sdate):
    mydate = sdate[0:4] + '.' + sdate[4:6]
    clist = get_collector(sdate)
    rib_location = ''
    peers = set()
    for c in clist:
        if c.startswith('rrc'):
            rib_location = hdname+'data.ris.ripe.net/'+c+'/'+mydate+'/'
            dir_list = os.listdir(hdname+'data.ris.ripe.net/'+c+'/'+mydate+'/')
            for f in dir_list:
                if not f.startswith('bview'):
                    dir_list.remove(f)
        else:
            if c == '':
                rib_location = hdname+'routeviews.org/bgpdata/'+mydate+'/RIBS/'
                dir_list =\
                    os.listdir(hdname+'routeviews.org/bgpdata/'+mydate+'/RIBS/')
            else:
                rib_location = 'routeviews.org/'+c+'/bgpdata/'+mydate+'/RIBS/'
                dir_list =\
                    os.listdir(hdname+'routeviews.org/'+c+'/bgpdata/'+mydate+'/RIBS/')

        rib_location = rib_location + dir_list[0] # if RIB is of the same month. That's OK.
        print 'getting peer count from RIB file: ', rib_location

        if rib_location.endswith('txt.gz'):
            subprocess.call('gunzip '+rib_location, shell=True)  # unpack                        
            rib_location = rib_location.replace('.txt.gz', '.txt')
        elif not rib_location.endswith('txt'):  # .bz2/.gz file exists
            parse_mrt(rib_location, rib_location+'.txt')
            rib_location = rib_location + '.txt'
            os.remove(rib_location)  # then remove .bz2/.gz
        # now rib file definitely ends with .txt  
        with open(rib_location, 'r') as f:  # get peers from RIB
            for line in f:
                try:
                    addr = line.split('|')[3]
                    peers.add(addr)
                except:
                    pass
        f.close()
        # compress RIB into .gz
        if not os.path.exists(rib_location+'.gz'):
            pack_gz(rib_location)

        print str(len(peers))

    f = open(hdname+'metadata/sdate&peercount', 'a')
    f.write(sdate+' '+str(len(peers))+'\n')
    f.close()

    return len(peers)

def get_all_pcount(sdate):
    objdt = datetime.datetime.strptime(sdate, '%Y%m%d') 
    intdt = time_lib.mktime(objdt.timetuple())

    dtlist = []
    pclist = []
    floc = hdname + 'topofile/bgp-active.txt'
    f = open(floc, 'r')
    for line in f:
        dt = line.split()[0]
        pcount = line.split()[1]
        dtlist.append(int(dt))
        pclist.append(int(pcount))
    f.close()

    least = 9999999999
    loc = 0
    for i in xrange(0, len(dtlist)):
        if abs(dtlist[i]-intdt) < least:
            least = abs(dtlist[i]-intdt)
            loc = i

    goal = 0
    for j in xrange(loc, len(dtlist)-1):
        prev = pclist[j-1]
        goal = pclist[j]
        nex = pclist[j+1]
        if abs(goal-prev) > prev/7 or abs(goal-nex) > nex/7: # outlier
            continue
        else:
            break

    return goal
