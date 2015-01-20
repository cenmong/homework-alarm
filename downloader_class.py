# While this code is mainly for downloading updates, we also download a RIB for 
# 1) deleting reset; 2) get the monitors' info
# TODO: when downloading multiple RIBs, use another mechanism other than this
#--------------------------------------------------------------------------------
# Note: when analyzing updates, ignore the first and last hours because the 
# reset-updates in it may have not been completely deleted
# This decision also makes the synchronization much easier

import radix # takes 1/4 the time as patricia
import gzip
import traceback
import cmlib
import datetime
import patricia
import subprocess
import os
import logging
import numpy as np
logging.basicConfig(filename='all.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

from env import *
from cStringIO import StringIO

#--------------------------------------------------------------------
# Stand-alone functions

# output: .bz2/gz.txt.gz files
def parse_update_files(listfile): # all update files from one collectors/list
    print 'Parsing update files...'
    flist = open(listfile, 'r')
    for line in flist:
        line = line.split('|')[0].replace('.txt.gz', '') # get the original .bz2/gz file name
        if not os.path.exists(datadir+line+'.txt.gz'):
            cmlib.parse_mrt(datadir+line, datadir+line+'.txt') # .bz2/gz => .bz2/gz.txt
            cmlib.pack_gz(datadir+line+'.txt') # .bz2/gz.txt => .bz2/gz.txt.gz
            os.remove(datadir+line)  # remove the original .bz2/.gz file
        else:
            print 'Parsed file exists'
            pass
    flist.close()
    return 0

#---------------------------------------------------------------------
# Get and parse the RIB that is closest to 00:00 of sdate

def get_parse_one_rib(co, sdate):
    tmp_month = sdate[0:4] + '.' + sdate[4:6]
    if co.startswith('rrc'):
        web_location = rrc_root + co + '/' + tmp_month + '/' 
    else:
        web_location = rv_root + co + '/bgpdata/' + tmp_month + '/RIBS/'
        web_location = web_location.replace('//', '/')
    webraw = cmlib.get_weblist('http://' + web_location)

    cmlib.make_dir(datadir+web_location)

    rib_list = webraw.split('\n')
    filter(lambda a: a != '', rib_list)
    filter(lambda a: a != '\n', rib_list)
    rib_list = [item for item in rib_list if 'rib' in item or 'bview' in item]

    sizelist = list()
    for line in rib_list:
        size = line.split()[-1]
        fsize = cmlib.parse_size(size)
        sizelist.append(fsize)

    avg = np.mean(sizelist) 

    target_line = '' # the RIB file for downloading
    closest = 99999
    for line in rib_list:
        fdate = line.split()[0].split('.')[-3]
        diff = abs(int(fdate)-int(sdate)) # >0
        if diff < closest:
            closest = diff
            size = line.split()[-1]
            fsize = cmlib.parse_size(size)
            if fsize > 0.8 * avg:
                target_line = line

    print 'Selected RIB:', target_line
    size = target_line.split()[-1] # claimed RIB file size
    fsize = cmlib.parse_size(size)

    filename = target_line.split()[0]
    full_loc = datadir + web_location + filename # .bz2/.gz

    if os.path.exists(full_loc+'.txt'):
        os.remove(full_loc+'.txt')

    print 'Supposed full location of the RIB:', full_loc
    if os.path.exists(full_loc+'.txt.gz'): 
        print 'existed file size:%f;original size:%f',os.path.getsize(full_loc+'.txt.gz'),fsize
        if os.path.getsize(full_loc+'.txt.gz') > 0.6 * fsize: # Good!
            return full_loc+'.txt.gz'
        else:
            os.remove(full_loc+'.txt.gz') # too small to be complete

    if os.path.exists(full_loc): 
        if os.path.getsize(full_loc) <= 0.95 * fsize:
            os.remove(full_loc)
        else: # Good!
            cmlib.parse_mrt(full_loc, full_loc+'.txt')
            cmlib.pack_gz(full_loc+'.txt')
            return full_loc+'.txt.gz'


    cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
    cmlib.parse_mrt(full_loc, full_loc+'.txt')
    cmlib.pack_gz(full_loc+'.txt')

    return full_loc+'.txt.gz'


def delete_reset(co, rib_full_loc, tmp_full_listfile):
    ## record reset info into a temp file
    reset_info_file = datadir + 'peer_resets.txt'

    print 'Obtaining BGP session reset start-end period...'
    subprocess.call('perl '+projectdir+'tool/bgpmct.pl -rf '+rib_full_loc+' -ul '+\
            tmp_full_listfile + ' > '+reset_info_file, shell=True)

    if os.path.exists(reset_info_file): 
        if os.path.getsize(reset_info_file) == 0:
            print 'no reset at all!'
            return
    else:
        print 'no reset at all!'
        return 
    
    peer_resettime = dict() # peer: list of [reset start, reset end]
    resetf = open(reset_info_file, 'r')
    for line in resetf:
        if line.startswith('run') or line.startswith('/') or '#' in line:
            continue
        if ':' in line:
            now_peer = line.rstrip(':\n')
            continue

        stime_unix, endtime_unix= int(line.split(',')[0]), int(line.split(',')[1])
        try:
            peer_resettime[now_peer].append([stime_unix, endtime_unix])
        except:
            peer_resettime[now_peer] = [[stime_unix, endtime_unix], ]

    resetf.close()

    if peer_resettime == {}:
        print 'no reset at all!'
    else:
        print peer_resettime

    for p in peer_resettime:
        for l in peer_resettime[p]:
            delete_reset_updates(co, p, l[0], l[1], tmp_full_listfile)
            pass

    os.remove(reset_info_file)
    return 0

def delete_reset_updates(co, peer, stime_unix, endtime_unix, tmp_full_listfile):
    start_datetime = datetime.datetime.utcfromtimestamp(stime_unix)
    end_datetime = datetime.datetime.utcfromtimestamp(endtime_unix)
    print 'Deleting session reset ',peer,':[',start_datetime,',',end_datetime,']...'

    #---------------------------------------------------------------------
    # Read the temproary full-path file list (original list is relative path)
    
    time_found = False # XXX Error if cannot find time

    f = open(tmp_full_listfile, 'r')
    for line in f:  
        updatefile = line.rstrip('\n')
        file_attr = updatefile.split('.')

        if co.startswith('rrc'): # note the difference in file name formats
            fattr_date, fattr_time = file_attr[rrc_date_fpos], file_attr[rrc_time_fpos]
        else:
            fattr_date, fattr_time = file_attr[rv_date_fpos], file_attr[rv_time_fpos]

        # Get file datetime obj dt from the file's name
        dt = datetime.datetime(int(fattr_date[0:4]),\
                int(fattr_date[4:6]), int(fattr_date[6:8]),\
                int(fattr_time[0:2]), int(fattr_time[2:4]))

        # Deal with several special time zone problems
        dt_anchor1 = datetime.datetime(2003,2,3,19,0)
        dt_anchor2 = datetime.datetime(2006,2,1,21,0)
        if co == 'route-views.eqix' and dt <= dt_anchor2: # now dt is PST time
            dt = dt + datetime.timedelta(hours=8)
        elif not co.startswith('rrc') and dt <= dt_anchor1: # PST time
            dt = dt + datetime.timedelta(hours=8)

        # Check whether the file is our target
        if not start_datetime + datetime.timedelta(minutes=-30) <= dt <= end_datetime:
            # FIXME redundant files are also shrinked
            continue

        logging.info('Processing (session reset probably exists) file: %s', updatefile)
        # record the prefix whose update has already been deleted (for once)
        size_before = os.path.getsize(updatefile)
        counted_pfx = radix.Radix()

        p = subprocess.Popen(['zcat', updatefile],stdout=subprocess.PIPE)
        old_f = StringIO(p.communicate()[0])
        assert p.returncode == 0
        new_f = gzip.open(datadir + updatefile.split('/')[-1], 'wb')

        # find and delete the reset updates
        for updt in old_f:
            try:
                attr = updt.rstrip('\n').split('|')
                if cmp(attr[3], peer) == 0 and (stime_unix<int(attr[1])<endtime_unix):
                    time_found = True
                    pfx = attr[5]
                    try: # Test whether the trie has the pfx
                        rnode = counted_pfx.search_exact(pfx)
                        rnode.data[0] += 1
                        # pfx has been deleted
                        new_f.write(updt+'\n') # pfx exists
                    except: # pfx does not exist
                        rnode = counted_pfx.add(pfx)
                        rnode.data[0] = 1
                else: # not culprit update
                    new_f.write(updt+'\n')
            except Exception, err:
                if updt != '':
                    logging.info(traceback.format_exc())
                    logging.info(update)
                    print traceback.format_exc()

        old_f.close()
        new_f.close()

        # use the new file to replace the old file
        os.remove(updatefile)
        tmp_loc = cmlib.get_file_dir(updatefile)
        subprocess.call('mv '+datadir+updatefile.split('/')[-1]+' '+tmp_loc, shell=True)
        size_after = os.path.getsize(updatefile)
        logging.info('size(b):%f,size(a):%f', size_before, size_after)
               
    f.close()

    assert time_found == True

#--------------------------------------------------------------------------
TEST = False

class Downloader():

    def __init__(self, sdate, edate, co):
        self.sdate = sdate
        self.edate = edate
        self.co = co
        self.listfile = datadir + 'update_list/' + sdate + '_' + edate + '/' + co + '_list.txt'

    def get_listfile(self):
        return self.listfile

    def get_listfile_dir(self):
        tmp_filename = self.listfile.split('/')[-1]
        tmp_dir = self.listfile.replace(tmp_filename, '')
        return tmp_dir

    def get_tmp_full_list(self):
        tmp_file = self.get_listfile_dir() + 'tmp_full_list'

        fr = open(self.listfile, 'r')
        fw = open(tmp_file, 'w')
        for line in fr:
            line = line.split('|')[0]
            fw.write(datadir+line+'\n')
        fr.close()
        fw.close()

        return tmp_file

    def get_update_list(self):
        #---------------------------------------------------------------------
        # Get a list of the target monthes for forming the target urls 
        smonth = int(sdate[0:4])*12 + int(sdate[4:6])
        emonth = int(edate[0:4])*12 + int(edate[4:6])
        month_gap = emonth - smonth # could be zero

        month_list = [] 
        for m in xrange(0, month_gap+1):
            now_month = smonth + m
            if now_month % 12 == 0: # the 12th month
                now_year = now_month / 12 - 1
                now_month = 12
            else:
                now_year = now_month / 12
                now_month = now_month % 12

            if now_month / 10 == 0:
                str_now_month = '0' + str(now_month) # E.g., '5'=>'05'
            else:
                str_now_month = str(now_month)

            month_list.append(str(now_year)+'.'+str_now_month) 

        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        # Create the urls according to the month list and obtain the file list
        tmp_dir = self.get_listfile_dir()
        cmlib.make_dir(tmp_dir)
        flist = open(self.listfile, 'w')  

        for month in month_list:
            web_location = ''
            if self.co.startswith('rrc'):
                web_location = rrc_root + self.co + '/' + month + '/' 
            else:
                web_location = rv_root + self.co + '/bgpdata/' + month + '/UPDATES/'
                web_location = web_location.replace('//', '/')  # when name is ''

            print 'Getting update list: http://' + web_location
            webraw = cmlib.get_weblist('http://' + web_location)
            cmlib.make_dir(datadir+web_location)

            for line in webraw.split('\n'):
                if not 'updates' in line or line == '' or line == '\n':
                    continue

                size = line.split()[-1]
                fsize = cmlib.parse_size(size)
                filename = line.split()[0]  # omit uninteresting info
                filedate = filename.split('.')[-3]

                # check whether its date in our range
                if int(filedate) < int(self.sdate) or int(filedate) > int(self.edate):
                    continue
                # XXX storing the original .bz2/.gz file name makes logic clearer
                flist.write(web_location+filename+'.txt.gz|'+str(fsize)+'\n')
                logging.info('record file name: '+web_location+filename+'.txt.gz|'+str(fsize))

        return 0


    #------------------------------------------------------------------------------
    # Read the file list and download the files
    def download_updates(self):
        if TEST: # XXX Just read several files when testing
            testcount = 0

        f = open(self.listfile, 'r')
        for line in f:
            line = line.replace('\n', '').replace('.txt.gz', '') # get original .bz2/gz name
            tmp = line.split('|')[0]
            filename = tmp.split('/')[-1]
            web_location = tmp.replace(filename, '') 
            fsize = float(line.split('|')[1])
            full_path = datadir + web_location + filename

            # Goal: only XXX.bz2/.gz or XXX.bz2/gz.txt.gz exists
            # remove (if) existing xx.txt file to make things clearer
            if os.path.exists(full_path+'.txt'):
                os.remove(full_path+'.txt')

            if os.path.exists(full_path+'.txt.gz'): # parsed file exists
                if os.path.getsize(full_path+'.txt.gz') > 0.8 * fsize: # size OK
                    logging.info('file exists:%s', full_path+'.txt.gz')
                    if os.path.exists(full_path):  # .bz2/.gz useless anymore
                        os.remove(full_path)
                    continue
                else:
                    os.remove(full_path+'.txt.gz')

            if os.path.exists(full_path): # original file exists
                now_size = os.path.getsize(full_path)
                if now_size > 0.95 * fsize: # size OK
                    logging.info('file exists:%s', full_path)
                    continue
                else:
                    os.remove(full_path)

            cmlib.force_download_file('http://'+web_location, datadir+web_location, filename) 

            if TEST: # XXX only download 5 files when testing
                testcount += 1
                if testcount == 5:
                    break
        f.close()
        return 0


    def get_all_updates(self):
        self.get_update_list()
        # Do it twice to make sure everything is downloaded
        self.download_updates()
        self.download_updates()


#----------------------------------------------------------------------------
# The main function of this py file
if __name__ == '__main__':
    order_list = [27]
    # collector_list = {27:('','rrc00')} # For TEST

    # all co that has appropriate date
    collector_list = dict()
    for i in order_list:
        collector_list[i] = list()
        for co in all_collectors.keys():
            if int(all_collectors[co]) <= int(daterange[i][0]):
                collector_list[i].append(co)
        print i,':',collector_list[i]
    
    '''
    listfiles = []
    # download update files
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            dl = Downloader(sdate, edate, co)
            dl.get_all_updates()
            listf = dl.get_listfile()
            listfiles.append(listf)

    # parse all the update files into readable ones
    for listf in listfiles:
        parse_update_files(listf)
    '''

    # Deleting updates caused by reset
    for order in order_list:
        co_rib = dict() # co: rib full path TODO rib=>ribs

        sdate = daterange[order][0]
        edate = daterange[order][1]
        #TODO check the length of the period according to UNIX timestamp

        # If period <= 31 days
        for co in collector_list[order]:
            # FIXME download a RIB every two months for long duration
            rib_full_loc = get_parse_one_rib(co, sdate)
            cmlib.get_peer_info(rib_full_loc) # do not delete this line TODO for long period get many files
            co_rib[co] = rib_full_loc

            # create temproary full-path update file list
            dl = Downloader(sdate, edate, co)
            full_list = dl.get_tmp_full_list() #TODO for long period get list of lists

            delete_reset(co, rib_full_loc, full_list)
            os.remove(full_list)

        # for each period, maintain a file that record its related RIBs
        # TODO diff format for long period
        cmlib.make_dir(rib_info_dir)
        # Do not change this
        rib_info = rib_info_dir + sdate + '_' + edate + '.txt'
        f = open(rib_info, 'w')
        for co in co_rib:
            f.write(co+':'+co_rib[co]+'\n')
        f.close()

        
