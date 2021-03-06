# While this code is mainly for downloading updates, we also download a RIB for 
# 1) deleting reset; 2) get the monitors' info
#--------------------------------------------------------------------------------
# Note: when analyzing updates, ignore the first and last hours because the 
# reset-updates in it may have not been completely deleted
# This decision also makes the synchronization much easier

import ast
import period_class
import radix # takes 1/4 the time as patricia
import gzip
import traceback
import cmlib
import datetime
import subprocess
import os
import logging
import shutil
import numpy as np
logging.basicConfig(filename='all.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

import time as time_lib
from env import *
from cStringIO import StringIO
#from guppy import hpy

def parse_update_files(listfile): # all update files from one collectors/list
    flist = open(listfile, 'r')
    for line in flist:
        line = line.rstrip('\n')
        fsize = float(line.split('|')[1])
        print 'fsize=',fsize
        line = line.split('|')[0].replace('.txt.gz', '') # get the original .bz2/gz file name
        if not os.path.exists(datadir+line+'.txt.gz'):
            cmlib.parse_mrt(datadir+line, datadir+line+'.txt', fsize) # .bz2/gz => .bz2/gz.txt
            cmlib.pack_gz(datadir+line+'.txt') # .bz2/gz.txt => .bz2/gz.txt.gz
            #os.remove(datadir+line)  # remove the original .bz2/.gz file
        else:
            print 'Parsed file exists'
            print datadir+line+'.txt.gz'
            pass
    flist.close()
    return 0


class Downloader():

    def __init__(self, sdate, edate, co):
        self.period = None
        self.global_peers = None

        self.sdate = sdate
        self.edate = edate
        self.sdt_obj = datetime.datetime(int(sdate[0:4]),int(sdate[4:6]),int(sdate[6:8]),0,0) # is UTC
        self.edt_obj = datetime.datetime(int(edate[0:4]),int(edate[4:6]),int(edate[6:8]),0,0) # is UTC
        self.gap_days = (self.edt_obj-self.sdt_obj).days + 1
        self.co = co

        self.rib_list = list()

        self.listfile = update_list_dir + sdate + '_' + edate + '/' + co + '_list.txt'
        
        self.reset_info = reset_info_dir + self.sdate + '_' + self.edate + '.txt' # Do not change this
        #self.counted_pfx = None

        self.dt_anchor1 = datetime.datetime(2003,2,3,19,0)
        self.dt_anchor2 = datetime.datetime(2006,2,1,21,0)

    def set_period(self, index):
        self.period = period_class.Period(index)
        self.period.get_global_monitors()
        self.global_peers = []
        for key in self.period.co_mo:
            self.global_peers.extend(self.period.co_mo[key])

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

    def get_month_list_dot(self):
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

            month_list.append(str(now_year)+'.'+str_now_month) #XXX note the dot

        return month_list


    def get_month_list(self):
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

            month_list.append(str(now_year)+str_now_month) #XXX note the dot

        return month_list

    def download_updates_starter(self):
        self.get_update_list()
        self.download_updates()

    def get_update_list(self):
        tmp_dir = self.get_listfile_dir()
        cmlib.make_dir(tmp_dir)
        flist = open(self.listfile, 'w')  
    
        month_list = self.get_month_list_dot()
        for month in month_list:
            web_location = ''
            if self.co.startswith('rrc'):
                web_location = rrc_root + self.co + '/' + month + '/' 
            else:
                web_location = rv_root + self.co + '/bgpdata/' + month + '/UPDATES/'
                web_location = web_location.replace('//', '/')  # when name is ''

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
                # note: storing the original .bz2/.gz file name makes logic clearer
                flist.write(web_location+filename+'.txt.gz|'+str(fsize)+'\n')
                logging.info('record file name: '+web_location+filename+'.txt.gz|'+str(fsize))

        return 0


    #------------------------------------------------------------------------------
    # Read the file list and download the files
    def download_updates(self):
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

        f.close()
        return 0

    def get_rib(self):
        if self.gap_days < 32:
            rib_full_loc = self.download_one_rib(self.sdate)
            self.rib_list = [rib_full_loc]
        else:
            month_list = self.get_month_list()
            print 'month list:', month_list
            datelist = list()
            for m in month_list:
                datelist.append(m+'01') # XXX RIB from the first day of the month
            print 'date list:', datelist

            rib_list = list()
            for date in datelist:
                rib_full_loc = self.download_one_rib(date)
                rib_list.append(rib_full_loc)
            self.rib_list = rib_list


    def download_one_rib_before_unix(self, my_date, unix): # my_date for deciding month
        tmp_month = my_date[0:4] + '.' + my_date[4:6]
        if self.co.startswith('rrc'):
            web_location = rrc_root + self.co + '/' + tmp_month + '/' 
        else:
            web_location = rv_root + self.co + '/bgpdata/' + tmp_month + '/RIBS/'
            web_location = web_location.replace('//', '/')

        try:
            webraw = cmlib.get_weblist('http://' + web_location)
            print 'Getting list from ' + 'http://' + web_location
        except:
            return -1

        cmlib.make_dir(datadir+web_location)

        #----------------------------------------------------------------
        # select a RIB file right before the unix and with reasonable (not strange) file size
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

        ok_rib_list = list() # RIBs whose size is OK
        for line in rib_list:
            fsize = cmlib.parse_size(line.split()[-1])
            if fsize > 0.9 * avg:
                ok_rib_list.append(line)

        target_line = None # the RIB closest to unix 
        min = 9999999999
        for line in ok_rib_list:
            fdate = line.split()[0].split('.')[-3]
            ftime = line.split()[0].split('.')[-2]
            dtstr = fdate+ftime
            objdt = datetime.datetime.strptime(dtstr, '%Y%m%d%H%M') 
            runix = time_lib.mktime(objdt.timetuple()) + 8*60*60 # F**k! Time zone!
            print objdt, runix, unix
            if runix <= unix and unix-runix < min:
                min = unix-runix
                print 'min changed to ', min
                target_line = line

        print 'Selected RIB:', target_line
        if target_line == None:
            return -1
        size = target_line.split()[-1] # claimed RIB file size
        fsize = cmlib.parse_size(size)

        filename = target_line.split()[0]
        full_loc = datadir + web_location + filename # .bz2/.gz

        if os.path.exists(full_loc+'.txt'): # only for clearer logic
            os.remove(full_loc+'.txt')

        #------------------------------------------------------------------
        # Download the RIB
        if os.path.exists(full_loc+'.txt.gz'): 
            print 'existed!!!!!!!!!!!!'
            return full_loc+'.txt.gz' # Do not download

        if os.path.exists(full_loc): 
            cmlib.parse_mrt(full_loc, full_loc+'.txt', fsize)
            cmlib.pack_gz(full_loc+'.txt')
            return full_loc+'.txt.gz'


        cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
        cmlib.parse_mrt(full_loc, full_loc+'.txt', fsize)
        cmlib.pack_gz(full_loc+'.txt')
        os.remove(full_loc) # remove the original file

        return full_loc+'.txt.gz'


    def download_one_rib(self, my_date):
        tmp_month = my_date[0:4] + '.' + my_date[4:6]
        if self.co.startswith('rrc'):
            web_location = rrc_root + self.co + '/' + tmp_month + '/' 
        else:
            web_location = rv_root + self.co + '/bgpdata/' + tmp_month + '/RIBS/'
            web_location = web_location.replace('//', '/')
        webraw = cmlib.get_weblist('http://' + web_location)

        cmlib.make_dir(datadir+web_location)

        #----------------------------------------------------------------
        # select a RIB file with reasonable (not strange) file size
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

        target_line = None # stores the RIB file for downloading
        largest_line = None
        max = -1
        closest = 99999
        for line in rib_list:
            fdate = line.split()[0].split('.')[-3]
            size = line.split()[-1]
            fsize = cmlib.parse_size(size)
            if fsize > max:
                max = fsize
                largest_line = line
            
            diff = abs(int(fdate)-int(my_date)) # >0
            # XXX logic here not clear (but seems effective)
            if diff <= closest and fsize > 0.9 * avg and fsize < 1.1 * avg:
                target_line = line
                closest = diff

        if target_line is None:
            assert largest_line is not None
            print 'Failed. Resort to downloading the largest RIB...'
            target_line = largest_line # work-around for a special case


        print 'Selected RIB:', target_line
        size = target_line.split()[-1] # claimed RIB file size
        fsize = cmlib.parse_size(size)

        filename = target_line.split()[0]
        full_loc = datadir + web_location + filename # .bz2/.gz

        if os.path.exists(full_loc+'.txt'): # only for clearer logic
            os.remove(full_loc+'.txt')

        #------------------------------------------------------------------
        # Download the RIB
        if os.path.exists(full_loc+'.txt.gz'): 
            print 'existed size & original size:',os.path.getsize(full_loc+'.txt.gz'),fsize
            if os.path.getsize(full_loc+'.txt.gz') > 0.6 * fsize: # 0.6 is good enough
                return full_loc+'.txt.gz' # Do not download
            else:
                os.remove(full_loc+'.txt.gz') # too small to be complete

        if os.path.exists(full_loc): 
            if os.path.getsize(full_loc) <= 0.95 * fsize:
                os.remove(full_loc)
            else: # Good!
                cmlib.parse_mrt(full_loc, full_loc+'.txt', fsize)
                cmlib.pack_gz(full_loc+'.txt')
                return full_loc+'.txt.gz'


        cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
        cmlib.parse_mrt(full_loc, full_loc+'.txt', fsize)
        cmlib.pack_gz(full_loc+'.txt')
        os.remove(full_loc) # remove the original file

        return full_loc+'.txt.gz'

    def delete_reset(self):
        rib_info = rib_info_dir + sdate + '_' + edate + '.txt' # Do not change this

        if self.gap_days < 32:
            f = open(rib_info, 'r')
            rib_full_loc = ''
            for line in f:
                line = line.rstrip('\n')
                now_co = line.split(':')[0]
                if now_co == self.co:
                    rib_full_loc = line.split(':')[1]
            f.close()

            assert rib_full_loc != ''
            # create temproary full-path update file list only for this task
            full_list = self.get_tmp_full_list()
            self.rm_reset_one_list(rib_full_loc, full_list)
        else:
            f = open(rib_info, 'r')
            ribs = list()
            for line in f:
                line = line.rstrip('\n')
                now_co = line.split(':')[0]
                if now_co != self.co:
                    continue
                ribs = line.split(':')[1].split('|')
            
            f.close()

            assert ribs != []

            full_list = self.get_tmp_full_list()
            month_list = self.get_month_list()

            assert len(ribs) == len(month_list)

            month_rib_udt = dict() # month: [rib_full_loc, update_full_loc_list]
            for m in month_list:
                full_list_part = full_list + '_' + m
                f = open(full_list, 'r')
                fin = open(full_list_part, 'w')
                for line in f:
                    date = line.split('.')[-5]
                    fmonth = date[:6]
                    day = date[6:8]
                    if fmonth == m or (self.month_larger_one(fmonth,m) and day == '01'):
                        fin.write(line)
                        
                f.close()
                fin.close()

                rib = ''
                for r in ribs:
                    if r.split('.')[-5][:6] == m:
                        rib = r
                month_rib_udt[m] = [rib, full_list_part]

            #TODO test before really remove updates

    def month_larger_one(month1, month2): # whether month1 larger than month2 for 1 month
        if month1[:4] == month2[:4]:
            if int(month1[4:6]) - int(month2[4:6]) == 1:
                return True
            else:
                return False
        elif int(month1[:4]) - int(month2[:4]) == 1:
            if month1[4:6] == '01' and month2[4:6] == '12':
                return True
            else:
                return False
        else:
            return False


    def rm_reset_one_list(self, rib_full_loc, tmp_full_listfile):
        ## record reset info into a temp file
        reset_info_file = datadir + 'peer_resets.txt'

        print self.co, ' obtaining BGP session reset start-end period...'
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
            if line.startswith('run') or line.startswith('/') or ('#' in line):
                continue
            if ':' in line:
                now_peer = line.rstrip(':\n')
                continue

            stime_unix, endtime_unix= int(line.split(',')[0]), int(line.split(',')[1])
            try:
                peer_resettime[now_peer].append([stime_unix, endtime_unix])
            except:
                peer_resettime[now_peer] = [[stime_unix, endtime_unix],]
        resetf.close()

        # write the reset info into a file
        # TODO deal with gap > 32 days
        cmlib.make_dir(reset_info_dir)
        f = open(self.reset_info, 'a')
        f.write(self.co+':\n')
        for p in peer_resettime:
            f.write(p+'@\n')
            for rs in peer_resettime[p]:
                f.write(str(rs)+'\n')
        f.close()
        '''
        # XXX only for once start (continue after the program stopped because of memo issue)
        # FIXME Giant bug in these code. In future, re-download the affected collectors
        this_co_peers = []
        peer_file = cmlib.peer_path_by_rib_path(rib_full_loc)
        fff = open(peer_file, 'r')
        for line in fff:
            peer = line.split('@')[0]
            this_co_peers.append(peer)
        fff.close()
        
        peer_resettime = dict()
        record = False
        f = open(self.reset_info, 'r')
        for line in f:
            line = line.rstrip('@\n')
            if ':' in line:
                record = False
                continue
            if line[0].isdigit():
                record = True
                p = line
                peer_resettime[p] = list()
            elif record is True:
                thelist = ast.literal_eval(line)
                peer_resettime[p].append(thelist)
            else:
                assert 1 == 0
        f.close()
        # XXX only for once end
        '''

        # different collectors in the same file
        for p in peer_resettime:
            if ':' in p: # We do not really delete IPv6 updates
                continue
            #if p not in this_co_peers: # XXX used with the previous commented out code
            #    continue
            if p not in self.global_peers: # We ignore non-global peers to save time
                continue
            for l in peer_resettime[p]:
                print 'deleting reset for ', p
                self.delete_reset_updates(p, l[0], l[1], tmp_full_listfile)
                #h = hpy()
                #print h.heap()

        os.remove(reset_info_file) #XXX comment out when 'doing it once'...

    def delete_reset_updates(self, peer, stime_unix, endtime_unix, tmp_full_listfile):
        # FIXME something is eating up memory!
        start_datetime = datetime.datetime.utcfromtimestamp(stime_unix)
        end_datetime = datetime.datetime.utcfromtimestamp(endtime_unix)
        logging.info( 'Deleting session reset %s: [%s, %s]', peer, str(stime_unix), str(endtime_unix))

        time_found = False # Raise an error if cannot find time

        f = open(tmp_full_listfile, 'r')
        for line in f:  
            updatefile = line.rstrip('\n')

            file_attr = updatefile.split('.')
            fattr_date, fattr_time = file_attr[-5], file_attr[-4]
            fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))

            # Deal with several special time zone problems
            if self.co == 'route-views.eqix' and fname_dt_obj <= self.dt_anchor2: # PST time
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
            elif not self.co.startswith('rrc') and fname_dt_obj <= self.dt_anchor1:
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

            # Note: some times the time intervals are not 5 and 15, so we set 10 and 30 here
            if co.startswith('rrc'):
                shift = -10
            else:
                shift = -30

            # Check whether the file is a possible target
            if not start_datetime+datetime.timedelta(minutes=shift)<=fname_dt_obj<=end_datetime:
                continue

            logging.info('Reading: %s', updatefile)
            size_before = os.path.getsize(updatefile)
            # record the prefix whose update has already been deleted for once
            #self.counted_pfx = radix.Radix()

            existed_pfx = dict() # pfx: True

            p = subprocess.Popen(['zcat', updatefile],stdout=subprocess.PIPE, close_fds=True)
            old_f = StringIO(p.communicate()[0])
            assert p.returncode == 0
            tmp_file_loc = datadir + updatefile.split('/')[-1]
            new_f = gzip.open(tmp_file_loc, 'wb')

            # find and delete the reset updates
            for updt in old_f:
                try:
                    attr = updt.rstrip('\n').split('|')
                    pfx = attr[5]
                    if attr[3]==peer and stime_unix<=int(attr[1])<=endtime_unix:
                        time_found = True
                        #rnode = self.counted_pfx.search_exact(pfx)
                        #if rnode is None: # cannot find, delete the update
                        #    rnode = self.counted_pfx.add(pfx)
                        #else: # found, so the prefix has been deleted once
                        #    new_f.write(updt)
                        try:
                            test = existed_pfx[pfx]
                            new_f.write(updt)
                        except:
                            existed_pfx[pfx] = True
                    else:
                        new_f.write(updt)
                except Exception, err:
                    if updt != '':
                        logging.info(traceback.format_exc())
                        logging.info(updt)

            old_f.close()
            new_f.close()

            del existed_pfx

            #p.kill()#This will cause an Error
            #self.counted_pfx = None
            #del self.counted_pfx

            # use the new file to replace the old file
            shutil.move(updatefile,updatefile+'.bak')
            shutil.move(tmp_file_loc, updatefile)
            size_after = os.path.getsize(updatefile)
            logging.info('size(b):%f,size(a):%f', size_before, size_after)
            os.remove(updatefile+'.bak')
                   
        f.close()

        if time_found == False: # Very rarely happens!
            logging.error('%s:Cannot find time in the files!!!!!!!(Error)', self.co)

if __name__ == '__main__':
    order = 286
    unix = cluster1_2[0]

    sdate = daterange[order][0]
    edate = daterange[order][1]

    rib_files = list()
    for co in all_collectors.keys():
        dl = Downloader(sdate, edate, co)
        rfilepath = dl.download_one_rib_before_unix(sdate, unix) # download RIB       
        if rfilepath != -1: # cannot get 
            rib_files.append(rfilepath)

    # output the rib file-list
    dir = final_output_root + 'additional_rib_list/' 
    cmlib.make_dir(dir)
    ofpath = dir + str(order) + '_' + str(unix) + '.txt'
    f = open(ofpath, 'w')
    for rpath in rib_files:
        f.write(rpath + '\n')
    f.close()

#----------------------------------------------------------------------------
# The main function
if __name__ == '__main__' and 1 == 2:
    order_list = [303]
    # we select all collectors that have appropriate start dates
    collector_list = dict()
    for i in order_list:
        collector_list[i] = list()
        for co in all_collectors.keys():
            co_sdate = all_collectors[co]
            sdate = daterange[i][0]
            edate = daterange[i][1]
            if co not in co_blank.keys():
                if int(co_sdate) <= int(sdate):
                    collector_list[i].append(co)
            else:
                bstart = co_blank[co][0]
                bend = co_blank[co][1]
                if int(co_sdate)<=int(sdate) and not (int(bstart)<=int(sdate)<=\
                        int(bend) or int(bstart)<=int(edate)<=int(bend)) and not\
                        (int(sdate)<=int(bstart) and int(edate)>=int(bend)):
                    collector_list[i].append(co)

        print i,':',collector_list[i]

    listfiles = [] # a list of update file list files
    # download update files
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            dl = Downloader(sdate, edate, co)
            #dl.download_updates_starter() # Download updates here
            listf = dl.get_listfile()
            listfiles.append(listf)

    '''
    # parse all the updates
    for listf in listfiles:
        parse_update_files(listf)

    # Download and record RIB and get peer info 
    for order in order_list:
        co_ribs = dict() # co: a list of rib files (full path)

        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            dl = Downloader(sdate, edate, co)
            dl.get_rib() # download RIB and store the list in downloader.rib_list
            co_ribs[co] = dl.rib_list
            for r in co_ribs[co]:
                cmlib.get_peer_info(r) # do not delete this line
        
        # for each period, maintain a file that record its RIBs (do not delete!)
        rib_info = rib_info_dir + sdate + '_' + edate + '.txt' # Do not change this
        cmlib.make_dir(rib_info_dir)
        f = open(rib_info, 'w')
        for co in co_ribs:
            f.write(co+':')
            for r in co_ribs[co][:-1]: # OK even when len(co_ribs[co]) == 1
                f.write(r+'|')
            f.write(co_ribs[co][-1]+'\n')
        f.close()
    '''
    # Delete reset updates
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            # TODO get all reset info first, then delete all reset 
            dl = Downloader(sdate, edate, co)
            dl.set_period(order)
            dl.delete_reset()
            del dl
