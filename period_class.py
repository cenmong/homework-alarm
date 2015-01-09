from env import *
from downloader_class import *

import traceback
import cmlib
import patricia
import os
import datetime
import time as time_lib
import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

# Work as input to update analysis functions
class Period():

    def __init__(self, index):
        self.sdate = daterange[index][0] 
        self.edate = daterange[index][1] 
        
        # location to store supporting files
        self.spt_dir = datadir + 'support/' + str(index) + '/'

        # Store the rib information of every collector (Note: do not change this!)
        self.rib_info_file = rib_info_dir + self.sdate + '_' + self.edate + '.txt'
    
        # TODO select only one monitor in each AS
        self.co_mo = dict() # collector: monitor list (does not store empty list)
        self.no_prefixes = patricia.trie(None) # prefixes that should be ignored TODO

        # XXX Get this only when necessary
        self.as2nation = self.get_as2nation_dict()

        # Occassionally run it to get the latest data. (Now up to 20141225)
        #self.get_fib_size_file()

    def get_fib_size_file(self):
        url = 'http://bgp.potaroo.net/as2.0/'
        cmlib.force_download_file(url, pub_spt_dir, 'bgp-active.txt')
        return 0

    def get_fib_size(self):
        objdt = datetime.datetime.strptime(self.sdate, '%Y%m%d') 
        intdt = time_lib.mktime(objdt.timetuple())

        dtlist = []
        pclist = []
        fibsize_f = pub_spt_dir + 'bgp-active.txt'
        f = open(fibsize_f, 'r')
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
        

    # Run it once will be enough. (Note: we can only get the *latest* AS to nation mapping)
    def get_as2nation_file(self):
        print 'Downloading AS to nation file...'
        if os.path.exists(pub_spt_dir+'as2nation.txt'):
            return 0

        the_url = 'http://bgp.potaroo.net/cidr/autnums.html'
        rows = cmlib.get_weblist(the_url).split('\n')

        f = open(pub_spt_dir+'as2nation.txt', 'w')
        for line in rows:
            if 'AS' not in line:
                continue
            nation = line.split(',')[-1] 
            ASN = line.split()[0].strip('AS')
            f.write(ASN+' '+nation+'\n')
        f.close()

        return 0

    def get_as2nation_dict(self):
        print 'Constructing AS to nation dict...'
        as2nation = {}

        f = open(datadir+'support/as2nation.txt')
        for line in f:
            as2nation[int(line.split()[0])] = line.split()[1]
        f.close()

        return as2nation

    def get_global_monitors(self):
        norm_size = self.get_fib_size()

        f = open(self.rib_info_file, 'r')
        totalc = 0
        totalok = 0
        nationc = dict() # nation: count
        for line in f:
            co = line.split(':')[0]
            logging.info('collector:%s', co)
            ribfile = line.split(':')[1]
            peerfile = cmlib.peer_path_by_rib_path(ribfile).rstrip('\n')

            count = 0
            ok = 0
            fp = open(peerfile, 'r')
            for line in fp:
                if len(line.split(':')) > 2:
                    continue
                mo_ip = line.split(':')[0]
                fibsize = int(line.split(':')[1].split('|')[0])
                if fibsize > 0.9 * norm_size:
                    try: 
                        test = self.co_mo[co]
                    except:
                        self.co_mo[co] = list()
                    if mo_ip not in self.co_mo[co]:
                        self.co_mo[co].append(mo_ip)
                    ok += 1
                    asn = int(line.split(':')[1].split('|')[1])
                    try:
                        nation = self.as2nation[asn]
                    except:
                        nation = 'unknown'
                    try:
                        nationc[nation] += 1
                    except:
                        nationc[nation] = 1
                count += 1
            fp.close()
            logging.info('This collector Feasible monitor %d/%d', ok, count)
            totalc += count
            totalok += ok
        f.close()
        logging.info('Feasible monitors:%d/%d', totalok, totalc)
        logging.info('%s', str(nationc))
        
        return 0

    # choose only one monitor from each AS
    # Note: the choice should be consistent (e.g., choose the one with the largest prefix integer)
    def mo_filter_same_as(self):
        print 'Selecting only one monitor in each AS...'
        mo_co = dict()
        for co in self.co_mo.keys():
            for mo in self.co_mo[co]:
                mo_co[mo] = co

        mo_list = mo_co.keys()

        asn_mo = dict() # ASN: monitor list

        f = open(self.rib_info_file, 'r')
        for line in f:
            co = line.split(':')[0]
            ribfile = line.split(':')[1]
            peerfile = cmlib.peer_path_by_rib_path(ribfile).rstrip('\n')
            fp = open(peerfile, 'r')
            for line in fp:
                if len(line.split(':')) > 2:
                    continue
                mo_ip = line.split(':')[0]
                asn = int(line.split(':')[1].split('|')[1])
                if mo_ip in mo_list:
                    try:
                        asn_mo[asn].append(mo_ip)
                    except:
                        asn_mo[asn] = list()
                        asn_mo[asn].append(mo_ip)
                else:
                    pass
            fp.close()
        f.close()

        remove_mo = list() # monitors to remove

        for asn in asn_mo.keys(): 
            tmp_list = asn_mo[asn]
            if len(tmp_list) <= 1:
                continue

            max = 0
            selected = ''
            for mo in tmp_list:
                if cmlib.ip_to_integer(mo) > max:
                    selected = mo
            tmp_list.remove(selected)
            remove_mo.extend(tmp_list)

        for rmo in remove_mo:
            try:
                co = mo_co[rmo]
            except: # no such monitor in self.co_mo
                continue
            
            try:
                self.co_mo[co].remove(rmo)
            except:
                pass

            if self.co_mo[co] == []: # empty list
                del self.co_mo[co]

        count = 0
        for co in self.co_mo.keys():
            count += len(self.co_mo[co])
        logging.info('Filtered out same-AS-monitors, # now:%d', count)

    def get_filelist(self):
        listdir = ''

        co_list = self.co_mo.keys()
        listfiles = list()
        for co in co_list:
            dl = Downloader(self.sdate, self.edate, co)
            listfiles.append(dl.get_listfile())
            listdir = dl.get_listfile_dir()

        fnames = dict()
        for lf in listfiles:
            f = open(lf, 'r')
            for name in f:
                name = name.replace('\n', '')
                file_attr = name.split('.')
                try:
                    file_dt = file_attr[5] + file_attr[6]
                    dt_obj = datetime.datetime.strptime(file_dt, '%Y%m%d%H%M')
                except:
                    file_dt = file_attr[4] + file_attr[5]
                    dt_obj = datetime.datetime.strptime(file_dt, '%Y%m%d%H%M')
                fnames[name] = dt_obj
            f.close()
        tmpdict = sorted(fnames, key=fnames.get)

        filelist = listdir + 'combined_list.txt'
        f = open(filelist, 'w')
        for name in tmpdict:
            f.write(name+'\n')
        f.close()

        return filelist

    def get_prefix(self):
        return 0

    #TODO select monitors; build a trie;  output monitor info
    #TODO select prefixes in two ways
    #TODO download support files and build it in memo if necessary

