# The downloading work should be de-coupled from other works, and should run alone.
# While this code is mainly for downloading updates, we also download a RIB for 
# 1) deleting reset; 2) get the monitors' info
# TODO: when downloading multiple RIBs, use another mechanism other than this

import cmlib
import datetime
import patricia
import subprocess
import os
import logging
logging.basicConfig(filename='download.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

from env import *

#--------------------------------------------------------------------
# Stand-alone functions

# output: .bz2/gz.txt.gz files
def parse_update_files(listfile): # all update files from one collectors/list
    print 'Parsing update files...'
    flist = open(listfile, 'r')
    for line in flist:
        line = line.split('|')[0].replace('.txt.gz', '') # get the original .bz2/gz file name
        logging.info('Parsing:%s', line)
        if not os.path.exists(datadir+line+'.txt.gz'):
            cmlib.parse_mrt(datadir+line, datadir+line+'.txt') # .bz2/gz => .bz2/gz.txt
            cmlib.pack_gz(datadir+line+'.txt') # .bz2/gz.txt => .bz2/gz.txt.gz
            os.remove(datadir+line)  # remove the original .bz2/.gz file
        else:
            logging.info('Parsed file exists')
            pass
    flist.close()
    return 0

#---------------------------------------------------------------------
# Get and parse the RIB that is closest to 00:00 of sdate

def get_parse_one_rib(co, sdate):
    tmp_month = sdate[0:4] + '.' + sdate[4:6]
    if co.startswith('rrc'):
        web_location = 'data.ris.ripe.net/' + co + '/' + tmp_month + '/' 
    else:
        web_location = 'routeviews.org/' + co + '/bgpdata/' + tmp_month + '/RIBS/'
        web_location = web_location.replace('//', '/')
    webraw = cmlib.get_weblist('http://' + web_location)

    cmlib.make_dir(datadir+web_location)

    rib_list = webraw.split('\n')
    filter(lambda a: a != '', rib_list)
    filter(lambda a: a != '\n', rib_list)
    rib_list = [item for item in rib_list if 'rib' in item or 'bview' in item]

    # XXX avoid the RIB having strange size
    target_line = '' # the RIB file for downloading
    closest = 99999
    for line in rib_list:
        fdate = line.split()[0].split('.')[-3]
        diff = abs(int(fdate)-int(sdate)) # >0
        if diff < closest:
            closest = diff
            target_line = line

    print 'Selected RIB:', target_line
    size = target_line.split()[-1] # claimed RIB file size
    if size.isdigit():
        fsize = float(size)
    else:
        fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])

    filename = target_line.split()[0]
    full_loc = datadir + web_location + filename # .bz2/.gz

    if os.path.exists(full_loc+'.txt'):
        os.remove(full_loc+'.txt')

    if os.path.exists(full_loc+'.txt.gz'): 
        print 'file exists:%f;original:%f',os.path.getsize(full_loc+'.txt.gz'),fsize
        if os.path.getsize(full_loc+'.txt.gz') > 1 * fsize: # FIXME change the ratio
            if os.path.exists(full_loc):  # .bz2/.gz useless anymore
                os.remove(full_loc)
            return full_loc+'.txt.gz'
        else:
            os.remove(full_loc+'.txt.gz') # too small to be complete
            cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
            print 'downloading %s:', filename

    if os.path.exists(full_loc): 
        if os.path.getsize(full_loc) <= 0.95 * fsize:
            os.remove(full_loc)
            cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
            print 'downloading %s:', filename
        else:
            pass

    print 'Parsing and packing the downloaded RIB'
    cmlib.parse_mrt(full_loc, full_loc+'.txt')
    try:
        os.remove(full_loc)  # then remove .bz2/.gz
    except: # XXX I do not know why file not exist
        pass
    cmlib.pack_gz(full_loc+'.txt')

    return full_loc+'.txt.gz'


def delete_reset(rib_full_loc, tmp_full_listfile):
    peers = cmlib.get_peer_list_from_rib(rib_full_loc)
    print 'peers: ', peers

    if TEST:
        peers = peers[0:2]

    for peer in peers:
        print '\ndeleting reset updates caused by peer: ', peer
        peer = peer.rstrip()

        ## record reset info into a temp file
        reset_info_file = peer+'_resets.txt'

        #FIXME create a temprory list (do not hard code the full path in the original list!)
        #TODO add 2 hours' redundant update files before and after the duration
        # Note: the list has to store XXX.txt.gz full path file names
        subprocess.call('perl '+homedir+'tool/bgpmct.pl -rf '+rib_comp_loc+' -ul '+\
                tmp_full_listfile+' -p '+peer+' > '+ datadir+'tmp/'+reset_info_file, shell=True)

        # No reset for this peer    
        if os.path.exists(datadir+'tmp/'+reset_info_file): 
            if os.path.getsize(datadir+'tmp/'+reset_info_file) == 0:
                continue
        else:
            continue
        
        # delete the corresponding updates
        del_tabletran_updates(peer, reset_info_file, tmp_full_listfile)

    subprocess.call('rm '+datadir+'tmp/*', shell=True)
                        
    return 0

# XXX this function is highly ineffective. But I seem cannot improve it
def del_tabletran_updates(peer, reset_info_file, tmp_full_listfile):
    # the reset info for this peer
    f_results = open(datadir+'tmp/'+reset_info_file, 'r')
    for line in f_results: 
        print line

        attr = line.replace('\n', '').split(',')
        if attr[0] == '#START':
            continue

        stime_unix, endtime_unix= int(attr[0]), int(attr[1])
        start_datetime = datetime.datetime.fromtimestamp(stime_unix) +\
                datetime.timedelta(hours=-8) # XXX note the time shift
        end_datetime = datetime.datetime.fromtimestamp(endtime_unix) +\
                datetime.timedelta(hours=-8)
        print 'session reset from ', start_datetime, ' to ', end_datetime

        f = open(tmp_full_listfile, 'r')
        for updatefile in f:  
            updatefile = updatefile.replace('\n', '')

            file_attr = updatefile.split('.')
            # FIXME the position of each factor has changed?
            if self.co.startswith('rrc'): # note the difference in file name formats
                fattr_date, fattr_time = file_attr[5], file_attr[6]
            else:
                fattr_date, fattr_time = file_attr[3], file_attr[4]
            dt = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))
            
            # filename not OK
            if not start_datetime + datetime.timedelta(minutes = -15) <= dt <=\
                    end_datetime:
                continue

            print 'session reset exists in: ', updatefile
            size_before = os.path.getsize(updatefile)
            myfilename = updatefile.replace('txt.gz', 'txt')
            subprocess.call('gunzip -c ' + updatefile + ' > ' + myfilename, shell=True)
            oldfile = open(myfilename, 'r')
            newfile = open(datadir+'tmp/'+myfilename.split('/')[-1], 'w')

            counted_pfx = patricia.trie(None)
            for updt in oldfile:  # loop over each update
                updt = updt.rstrip('\n')
                attr = updt.split('|')
                # culprit update confirmed
                if cmp(attr[3], peer) == 0 and (stime_unix < int(attr[1]) < endtime_unix):
                    pfx = attr[5]
                    try:  # Test whether the trie has the pfx
                        test = counted_pfx[pfx]
                        newfile.write(updt+'\n')  # pfx exists
                    except:  # Node does not exist
                        counted_pfx[pfx] = True
                else:  # not culprit update
                    newfile.write(updt+'\n')

            oldfile.close()
            newfile.close()

            os.remove(updatefile)  # remove old .gz file
            # compress .txt into txt.gz to replace the old file
            subprocess.call('gzip -c '+datadir+'tmp/'+myfilename.split('/')[-1]+\
                    ' > '+updatefile, shell=True)
            size_after = os.path.getsize(updatefile)
            print 'size(b):', size_before, ',size(a):', size_after
                   
        f.close()
    f_results.close()
    return 0

# FIXME not flexible
def combine_flist(self, order):
    sdate = daterange[order][0]
    fnames = {}
    clist = cmlib.get_collector(sdate)
    for cl_name in clist:
        if cl_name.startswith('rrc'):
            cl_type = 1 # collector type
        else:
            cl_type = 0
        # .bz2.txt.gz file name
        flist = open(datadir+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')
        for filename in flist:
            filename = filename.replace('\n', '')
            file_attr = filename.split('.')
            if cl_type == 0:
                file_dt = file_attr[3] + file_attr[4]
            else:
                file_dt = file_attr[5] + file_attr[6]
            dt_obj = datetime.datetime.strptime(file_dt, '%Y%m%d%H%M')
            fnames[filename] = dt_obj
        flist.close()
    fnlist = sorted(fnames, key=fnames.get)

    if os.path.exists(datadir+'metadata/'+sdate+'/updt_filelist_comb'):
        os.remove(datadir+'metadata/'+sdate+'/updt_filelist_comb')
    fcomb = open(datadir+'metadata/'+sdate+'/updt_filelist_comb', 'w')  # .bz2.txt.gz file name
    for fn in fnlist:
        fcomb.write(fn+'\n')
    fcomb.close()
    return 0


def download_redundant_updates(sdate, edate, 2):
    smonth
    emonth
    r_smonth
    r_stime
    r_emonth
    r_etime
    
    # get update lists from r monthes
    # filter and form our list
    # download files from the lists


#--------------------------------------------------------------------------
# The class definition

# XXX: change file name for RV when time < Feb, 2003.
TEST = False

class Downloader():

    def __init__(self, sdate, edate, co):
        self.sdate = sdate
        self.edate = edate
        self.co = co
        self.listfile = datadir + 'update_list/' + sdate + '_' + edate + '/' + co + '_list.txt'

    def get_listfile(self):
        return self.listfile

    #-----------------------------------------------------------------------
    # Get a list of the update files before downloading them
    def get_update_list(self):
        if int(self.sdate) < int(all_collectors[self.co]):
            print 'error: this collector started too late'
            logging.error('collector\' start date too late')
            return -1

        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
        tmp_filename = self.listfile.split('/')[-1]
        tmp_dir = self.listfile.replace(tmp_filename, '')
        cmlib.make_dir(tmp_dir)
        flist = open(self.listfile, 'w')  

        for month in month_list:
            web_location = ''
            if self.co.startswith('rrc'):
                web_location = 'data.ris.ripe.net/'+self.co+'/'+month+'/' 
            else:
                #web_location = 'archive.routeviews.org/' +\ # XXX I don't know why change
                web_location = 'routeviews.org/' + self.co + '/bgpdata/' + month + '/UPDATES/'
                web_location = web_location.replace('//', '/')  # when name is ''

            webraw = cmlib.get_weblist('http://' + web_location)
            cmlib.make_dir(datadir+web_location)

            for line in webraw.split('\n'):
                if not 'updates' in line or line == '' or line == '\n':
                    continue

                size = line.split()[-1]
                if size.isdigit():
                    fsize = float(size)
                else: # E.g., 155 K
                    fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])
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
                if os.path.getsize(full_path+'.txt.gz') > 2 * fsize: # size OK
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
            print 'Downloading ' + 'http://'+web_location + filename
            logging.info('Downloading ' + 'http://'+web_location + filename)

            if TEST: # XXX only download 5 files when testing
                testcount += 1
                if testcount == 5:
                    break
        f.close()
        return 0


    def get_all_updates(self):
            tmp_flag = self.get_update_list()
            if tmp_flag == -1: # fail to create
                logging.info('collector start date too late')
                return -1
            self.download_updates()


#----------------------------------------------------------------------------
# The main function of this py file
if __name__ == '__main__':
    order_list = [27]
    collector_list = ['', 'rrc00']

    listfiles = []
    '''
    # download update files
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list:
            dl = Downloader(sdate, edate, co)
            dl.get_all_updates()
            listf = dl.get_listfile()
            listfiles.append(listf)


    # parse all the update files into readable ones TODO under test
    for listf in listfiles:
        parse_update_files(listf)
    '''

    # Deleting updates caused by reset
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list:
            # XXX download a RIB every one or two months, cannot be too long
            rib_full_loc = get_parse_one_rib(co, sdate)
            rlist = download_redundant_updates(sdate, edate, 2) # 2 hours before and after
            # TODO create full-path update file list according to original and abundant lists
            dl = Downloader(sdate, edate, co)
            listf = dl.get_listfile()
            '''
            full_list = get_tmp_full_list(listf, rlist)
            # TODO delete the reset updates
            delete_reset(rib_full_loc, full_list)
            '''


