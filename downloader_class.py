# While this code is mainly for downloading updates, we also download a RIB for 
# 1) deleting reset; 2) get the monitors' info
#--------------------------------------------------------------------------------
# Note: when analyzing updates, ignore the first and last hours because the 
# reset-updates in it may have not been completely deleted
# This decision also makes the synchronization much easier

import radix # takes 1/4 the time as patricia
import gzip
import traceback
import cmlib
import datetime
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

#--------------------------------------------------------------------------
class Downloader():

    def __init__(self, sdate, edate, co):
        self.sdate = sdate
        self.edate = edate
        self.sdt_obj = datetime.datetime(int(sdate[0:4]),int(sdate[4:6]),int(sdate[6:8]),0,0) # is UTC
        self.edt_obj = datetime.datetime(int(edate[0:4]),int(edate[4:6]),int(edate[6:8]),0,0) # is UTC
        self.gap_days = (self.edt_obj-self.sdt_obj).days + 1
        self.co = co

        self.rib_list = list()

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

    def get_all_updates(self):
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
            datelist = list()
            for m in month_list:
                datelist.append(m+'01') # XXX RIB from the first day of the month

            riblist = list()
            for date in datelist:
                rib_full_loc = self.download_one_rib(date)
                riblist.append(rib_full_loc)
            self.rib_list = rib_list

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

        target_line = '' # stores the RIB file for downloading
        closest = 99999
        for line in rib_list:
            fdate = line.split()[0].split('.')[-3]
            diff = abs(int(fdate)-int(my_date)) # >0
            # XXX logic here not clear (maybe effective)
            if diff <= closest:
                size = line.split()[-1]
                fsize = cmlib.parse_size(size)

                if fsize > 0.9 * avg and fsize < 1.1 * avg:
                    target_line = line
                    closest = diff

        assert target_line != ''

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
                cmlib.parse_mrt(full_loc, full_loc+'.txt')
                cmlib.pack_gz(full_loc+'.txt')
                return full_loc+'.txt.gz'


        cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
        cmlib.parse_mrt(full_loc, full_loc+'.txt')
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

        for p in peer_resettime:
            for l in peer_resettime[p]:
                self.delete_reset_updates(p, l[0], l[1], tmp_full_listfile)

        os.remove(reset_info_file)

    def delete_reset_updates(self, peer, stime_unix, endtime_unix, tmp_full_listfile):
        start_datetime = datetime.datetime.utcfromtimestamp(stime_unix)
        end_datetime = datetime.datetime.utcfromtimestamp(endtime_unix)
        print 'Deleting session reset ',peer,':[',start_datetime,',',end_datetime,']...'

        time_found = False # Raise an error if cannot find time

        f = open(tmp_full_listfile, 'r')
        for line in f:  
            updatefile = line.rstrip('\n')
            file_attr = updatefile.split('.')
            fattr_date, fattr_time = file_attr[-5], file_attr[-4]
            # Get file datetime obj dt from the file's name
            dt = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))

            # Deal with several special time zone problems
            dt_anchor1 = datetime.datetime(2003,2,3,19,0)
            dt_anchor2 = datetime.datetime(2006,2,1,21,0)
            if self.co == 'route-views.eqix' and dt <= dt_anchor2: # now dt is PST time
                dt = dt + datetime.timedelta(hours=8)
            elif not co.startswith('rrc') and dt <= dt_anchor1: # PST time
                dt = dt + datetime.timedelta(hours=8)

            # Check whether the file is a possible target
            if co.startswith('rrc'): # note the difference in file name formats
                shift = -5
            else:
                shift = -15
            if not start_datetime + datetime.timedelta(minutes=shift) <= dt <= end_datetime:
                continue

            logging.info('Session reset probably exists in: %s', updatefile)
            size_before = os.path.getsize(updatefile)
            # record the prefix whose update has already been deleted for once
            counted_pfx = radix.Radix()

            p = subprocess.Popen(['zcat', updatefile],stdout=subprocess.PIPE)
            old_f = StringIO(p.communicate()[0])
            assert p.returncode == 0
            tmp_file_loc = datadir + updatefile.split('/')[-1]
            new_f = gzip.open(tmp_file_loc, 'wb')

            # find and delete the reset updates
            for updt in old_f:
                try:
                    attr = updt.rstrip('\n').split('|')
                    if attr[3]==peer and stime_unix<=int(attr[1])<=endtime_unix:
                        time_found = True
                        pfx = attr[5]
                        rnode = counted_pfx.search_exact(pfx)
                        if rnode is None:
                            rnode = counted_pfx.add(pfx)
                        else:
                            new_f.write(updt)
                    else:
                        new_f.write(updt)
                except Exception, err:
                    if updt != '':
                        logging.info(traceback.format_exc())
                        logging.info(update)

            old_f.close()
            new_f.close()

            # use the new file to replace the old file
            os.remove(updatefile)
            target_loc = cmlib.get_file_dir(updatefile)
            subprocess.call('mv '+tmp_file_loc+' '+target_loc, shell=True)
            size_after = os.path.getsize(updatefile)
            logging.info('size(b):%f,size(a):%f', size_before, size_after)
                   
        f.close()

        assert time_found == True # does not fit a very active peer
        #if time_found == False:
        #    logging.error('%s:Cannot find time in files when deleting reset...', self.co)

#----------------------------------------------------------------------------
# The main function
if __name__ == '__main__':
    #order_list = [0,1,3,4,5]
    order_list = [27]

    # we select all collectors that have appropriate start dates
    collector_list = dict()
    for i in order_list:
        collector_list[i] = list()
        for co in all_collectors.keys():
            if int(all_collectors[co]) <= int(daterange[i][0]):
                collector_list[i].append(co)
        print i,':',collector_list[i]

    collector_list[27] = collector_list[27][-1:] #XXX test
    #collector_list[27] = ['route-views.eqix']
    
    '''
    listfiles = [] # a list of update file list files
    # download update files
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            dl = Downloader(sdate, edate, co)
            dl.get_all_updates() # Download updates here
            listf = dl.get_listfile()
            listfiles.append(listf)

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
            dl.get_rib()
            co_ribs[co] = dl.rib_list
            for r in co_ribs[co]:
                cmlib.get_peer_info(r) # do not delete this line

        # for each period, maintain a file that record its RIBs (do not delete!)
        rib_info = rib_info_dir + sdate + '_' + edate + '.txt' # Do not change this
        cmlib.make_dir(rib_info_dir)
        f = open(rib_info, 'w')
        for co in co_ribs:
            f.write(co+':')
            for r in co_ribs[co][:-1]: #XXX test when len(co_ribs[co]) == 1
                f.write(r+'|')
            f.write(co_ribs[co][-1]+'\n')
        f.close()
    '''

    # Delete reset updates
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list[order]:
            dl = Downloader(sdate, edate, co)
            dl.delete_reset()
