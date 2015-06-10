import signal
import os
import urllib
import urllib2 # use this instead of urllib
import subprocess
import re
import nltk
import matplotlib
# This is useful. I can render figures thourgh ssh. (VNC viewer is unnecessary.)
matplotlib.use('Agg') # must be before fisrtly importing pyplot or pylab
import datetime
import socket
import gzip
import patricia
import time as time_lib
import operator
from cStringIO import StringIO

from netaddr import *
from env import *

# Get all the info once and forever, store in a well-known place
# (Actually when pre-processing)
# Note: we should read RIB as less as possible because it costs too much time
def get_peer_info(rib_full_loc):
    print 'Getting and storing peer info from:', rib_full_loc
    output = peer_path_by_rib_path(rib_full_loc)
    if os.path.exists(output):
        print 'Peer info file already exists!'
        return output

    peer_pfx_count = dict()
    peer2as = dict()

    p = subprocess.Popen(['zcat', rib_full_loc],stdout=subprocess.PIPE)
    f = StringIO(p.communicate()[0])
    assert p.returncode == 0

    for line in f:
        try:
            peer = line.split('|')[3]
            asn = line.split('|')[4]
            if int(asn) > 0: # I ran into a 0 for several times
                peer2as[peer] = asn
                try:
                    peer_pfx_count[peer] += 1
                except:
                    peer_pfx_count[peer] = 1
        except: # strange line
            pass
    f.close()

    fo = open(output, 'w')
    for peer in peer_pfx_count:
        # peer IP(including IPv6):pfx count|ASN
        fo.write(peer+'@'+str(peer_pfx_count[peer])+'|'+peer2as[peer]+'\n')
    fo.close()

    return output

def peer_path_by_rib_path(rib_full_loc):
    path = get_file_dir(rib_full_loc) + 'peers_' +\
        get_file_name(rib_full_loc).replace('txt.gz','txt')
    return path

def get_file_dir(file_full_loc):
    tmp_filename = file_full_loc.split('/')[-1]
    tmp_dir = file_full_loc.replace(tmp_filename, '')
    return tmp_dir

def get_file_name(file_full_loc):
    tmp_filename = file_full_loc.split('/')[-1]
    return tmp_filename

def download_file(url, save_loc, filename):
    print 'Downloading '+url+filename
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
        print 'Downloading '+url+filename
        try:
            f = urllib2.urlopen(url+filename, timeout = 5)
            with open(save_loc+filename,'wb') as code:
                code.write(f.read())
            break
        #except socket.timeout, e:
        except:
            print 'Time out or other failure! Retry...'

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
    print 'gz packing: '+loc_fname
    if not os.path.exists(loc_fname+'.gz'):
        subprocess.call('gzip '+loc_fname, shell=True)
    else:
        pass

def make_dir(location):
    if not os.path.isdir(location):
        os.makedirs(location)

def get_weblist(url):
    print 'Obtaining filelist from: ', url
    while 1:
        try:
            webhtml = urllib2.urlopen(url, timeout = 5).read()
            webraw = nltk.clean_html(webhtml)
            break
        except socket.timeout, e:
            print 'Time out! Retry...'

    return webraw

class TO(Exception):
    pass

def TO_handler(signum, frame):
    raise TO

def parse_mrt(old_loc_fname, new_loc_fname, fsize):
    print 'Parsing: '+old_loc_fname
    if not os.path.exists(new_loc_fname):
        
        signal.signal(signal.SIGALRM, TO_handler)
        time_s = fsize / float(60000) * 5
        if time_s < 1:
            time_s = 1

        signal.alarm(int(time_s+1))
        while(1):
            try:
                subprocess.call(projectdir + 'tool/libbgpdump-1.4.99.11/bgpdump -m '+\
                        old_loc_fname+' > '+new_loc_fname, shell=True)
                break
            except TO:
                print 'Time out! Retry...'
                signal.alarm(int(time_s+1))

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
        print datetime.datetime.utcfromtimestamp(dt)
    except:
        print dt
    return 0

def ip4_to_binary(content):  # can deal with ip addr and pfx
    tmp = content.split('/')
    pfx = tmp[0]
    addr = IPAddress(pfx).bits().replace('.', '')

    try:
        length = int(tmp[1])
        addr = addr[:length]
    except:  # an addr, not a pfx
        pass

    return addr

def pfx4_to_binary(content):
    tmp = content.split('/')
    pfx = tmp[0]
    addr = IPAddress(pfx).bits().replace('.', '')
    length = int(tmp[1])
    addr = addr[:length]
    return addr

def binary_to_ip4(content):
    decimal = int(content, 2)
    addr = str(IPAddress(decimal))
    if len(content) == 32:
        return addr
    else:
        return addr + '/' + str(len(content))

def ip_to_integer(ip):
    tmp = IPAddress(ip)
    return int(tmp)

def size_u2v(unit):
    if unit in ['k', 'K']:
        return 1024
    if unit in ['m', 'M']:
        return 1048576
    if unit in ['g', 'G']:
        return 1073741824

def parse_size(size):
    if size.isdigit():
        return float(size)
    else:
        return float(size[:-1]) * size_u2v(size[-1])

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

def get_file_list_indate(updtlist_file, sdt_obj, edt_obj):
    filepath_list = list()

    f = open(updtlist_file, 'r')
    for fline in f:
        # get date from file name
        updatefile = fline.split('|')[0]

        file_attr = updatefile.split('.')
        fattr_date, fattr_time = file_attr[-5], file_attr[-4]
        fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                int(fattr_date[4:6]), int(fattr_date[6:8]),\
                int(fattr_time[0:2]), int(fattr_time[2:4]))
        
        fline = datadir + fline.split('|')[0]

        # get current file's collector name
        attributes = fline.split('/') 
        j = -1
        for a in attributes:
            j += 1
            if a.startswith('data.ris') or a.startswith('archi'):
                break

        co = fline.split('/')[j + 1]
        if co == 'bgpdata':  # route-views2, the special case
            co = ''


        # Deal with several special time zone problems
        if co == 'route-views.eqix' and fname_dt_obj <= dt_anchor2: # PST time
            fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
        elif not co.startswith('rrc') and fname_dt_obj <= dt_anchor1:
            fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

        if co.startswith('rrc'):
            shift = -10
        else:
            shift = -30


        # Check whether the file is a possible target
        if not sdt_obj+datetime.timedelta(minutes=shift)<=fname_dt_obj<=edt_obj:
            continue

        filepath_list.append(fline)

    f.close()
    return filepath_list

    
def select_update_files(flist, sdt_unix, edt_unix):
    goallist = list()
    sdt_obj = datetime.datetime.utcfromtimestamp(sdt_unix)
    edt_obj = datetime.datetime.utcfromtimestamp(edt_unix)

    for fpath in flist:
        file_attr = fpath.split('.')
        fattr_date, fattr_time = file_attr[-5], file_attr[-4]
        fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                int(fattr_date[4:6]), int(fattr_date[6:8]),\
                int(fattr_time[0:2]), int(fattr_time[2:4]))

        attributes = fpath.split('/') 
        j = -1
        for a in attributes:
            j += 1
            if a.startswith('data.ris') or a.startswith('archi'):
                break

        co = fpath.split('/')[j + 1]
        if co == 'bgpdata':  # route-views2, the special case
            co = ''

        # Deal with several special time zone problems
        if co == 'route-views.eqix' and fname_dt_obj <= dt_anchor2: # PST time
            fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
        elif not co.startswith('rrc') and fname_dt_obj <= dt_anchor1:
            fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

        if co.startswith('rrc'):
            shift = -10
        else:
            shift = -30


        # Check whether the file is a possible target
        if sdt_obj+datetime.timedelta(minutes=shift)<=fname_dt_obj<=edt_obj:
            goallist.append(fpath)

    return goallist
