import os
import urllib
import subprocess
import re
import nltk
import numpy as np
import matplotlib
# This is useful. I can render figures thourgh ssh. VNC viewer in unnecessary.
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import datetime
import patricia
import gzip
import time as time_lib
import operator

from netaddr import *
from env import *

def get_peer_list_from_rib(rib_full_loc): # file name end with .bz2/gz.txt.gz
    peers = []
    txtfile = rib_full_loc.replace('txt.gz', 'txt')
    subprocess.call('gunzip '+rib_comp_loc, shell=True)
    with open(txtfile, 'r') as f:  # get peers from RIB
        for line in f:
            try:
                addr = line.split('|')[3]
                if addr not in peers:
                    peers.append(addr)
            except:
                pass
    f.close()

    # compress RIB into .gz
    if not os.path.exists(rib_comp_loc):
        cmlib.pack_gz(txtfile)

    return peers

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
        subprocess.call('~/tool/libbgpdump-1.4.99.11/bgpdump -m '+\
                old_loc_fname+' > '+new_loc_fname, shell=True)
    else:  # file already exists
        pass

def store_symbol_count(granu, my_dict, describe):
    xlist = [0]
    ylist = [0]
    for item in sorted(my_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
        xlist.append(item[0])
        ylist.append(item[1])

    sdate = describe.split('_')[0]
    f = open(datadir+'output/'+sdate+'_'+str(granu)+'/'+\
            describe+'-raw.txt', 'w')
    for i in xrange(0, len(xlist)):
        f.write(str(xlist[i])+'|ASN,'+str(ylist[i])+'|count\n')
    f.close()

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
        print 'Protocol false!'
        return 0

def get_collector(sdate):
    clist = []
    dir_list = os.listdir(datadir+'metadata/'+sdate+'/')
    for f in dir_list:
        if not 'filelist' in f:
            continue
        if 'test' in f:
            continue
        
        cl = f.split('_')[-1]
        if cl == 'comb':
            continue
        if cl.endswith('~'):
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

def get_all_length(sdate):
    print 'Getting all prefix lengthes from RIB...'

    len_count = dict() # length:count
    trie = patricia.trie(None)

    mydate = sdate[0:4] + '.' + sdate[4:6]
    dir_list = os.listdir(datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/')
    rib_location = datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/'
    for f in dir_list:
        if not f.startswith('.'):
            rib_location = rib_location + f # if RIB is of the same month. That's OK.
            break

    if rib_location.endswith('txt.gz'):
        subprocess.call('gunzip '+rib_location, shell=True)  # unpack                        
        rib_location = rib_location.replace('.txt.gz', '.txt')
    elif not rib_location.endswith('txt'):  # .bz2/.gz file exists
        parse_mrt(rib_location, rib_location+'.txt')
        os.remove(rib_location)  # then remove .bz2/.gz
        rib_location = rib_location + '.txt'
    # now rib file definitely ends with .txt  
    with open(rib_location, 'r') as f:  # get monitors from RIB
        for line in f:
            try:
                pfx = line.split('|')[5]
                pfx = ip_to_binary(pfx, '0.0.0.0')
            except: # incomplete entry may exsits
                continue
            try: 
                test = trie[pfx] # whether already exists
            except:
                trie[pfx] = True
    f.close()
    # compress the RIB back into .gz
    if not os.path.exists(rib_location+'.gz'):
        pack_gz(rib_location)

    pfx_count = 0
    for pfx in trie.iter(''):
        if pfx != '':
            pfx_count += 1
            try:
                len_count[len(pfx)] += 1
            except:
                len_count[len(pfx)] = 1
    del trie 

    return [len_count, pfx_count]

def get_monitors(sdate):
    mydate = sdate[0:4] + '.' + sdate[4:6]
    clist = get_collector(sdate)
    rib_location = ''
    monitors = dict() # monitor ip: AS
    for c in clist:
        if c.startswith('rrc'):
            rib_location = datadir+'data.ris.ripe.net/'+c+'/'+mydate+'/'
            dir_list_tmp = os.listdir(datadir+'data.ris.ripe.net/'+c+'/'+mydate+'/')
            dir_list = []
            for f in dir_list_tmp:
                if f.startswith('bview'):
                    dir_list.append(f)
        else:
            if c == '':
                rib_location = datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/'
                dir_list =\
                    os.listdir(datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/')
            else:
                rib_location = 'routeviews.org/'+c+'/bgpdata/'+mydate+'/RIBS/'
                dir_list =\
                    os.listdir(datadir+'routeviews.org/'+c+'/bgpdata/'+mydate+'/RIBS/')

        for f in dir_list:
            if not f.startswith('.'):
                rib_location = rib_location + f # if RIB is of the same month. That's OK.
                break
        print 'Getting monitors/AS:', rib_location

        if rib_location.endswith('txt.gz'):
            # unpack (don't ask about overwirte existent ones)                         
            subprocess.call('yes n | gunzip '+rib_location, shell=True)
            rib_location = rib_location.replace('.txt.gz', '.txt')
        elif not rib_location.endswith('txt'):  # .bz2/.gz file exists
            parse_mrt(rib_location, rib_location+'.txt')
            os.remove(rib_location)  # then remove .bz2/.gz
            rib_location = rib_location + '.txt'

        # Now tbe RIB file definitely ends with .txt  
        f = open(rib_location, 'r')  # get monitors from RIB
        for line in f:
            try:
                addr = line.split('|')[3]
                asn = int(line.split('|')[4])
            except: # incomplete entry may exsits
                continue
            try: 
                test = monitors[addr] # whether already exists
            except:
                monitors[addr] = asn
        f.close()
        
        # compress the RIB back into .gz
        if not os.path.exists(rib_location+'.gz'):
            pack_gz(rib_location)

        print 'Monitor quantity by now: ', len(monitors.keys())

    f = open(datadir+'metadata/sdate&peercount', 'a')
    f.write(sdate+' '+str(len(monitors.keys()))+'\n')
    f.close()

    return monitors

# get the number of all prefixes at certain date time
def get_all_pcount(sdate):
    objdt = datetime.datetime.strptime(sdate, '%Y%m%d') 
    intdt = time_lib.mktime(objdt.timetuple())

    dtlist = []
    pclist = []
    floc = datadir + 'support/bgp-active.txt'
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

def get_all_ascount(sdate):
    objdt = datetime.datetime.strptime(sdate, '%Y%m%d') 
    intdt = time_lib.mktime(objdt.timetuple())

    dtlist = []
    pclist = []
    floc = datadir + 'support/bgp-as-count.txt'
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
