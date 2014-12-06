# The downloading work should be de-coupled from other works, and should run alone.
# While this code is mainly for downloading updates, we also download a RIB for 
# 1) deleting reset; 2) get the monitors' info
# TODO: when downloading multiple RIBs, use another mechanism other than this

import cmlib
import datetime
import patricia
import subprocess
import logging
logging.basicConfig(filename='download.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

from env import *
# XXX: change file name for RV when time < Feb, 2003.
TEST = False

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

class Downloader():

    def __init__(self, sdate, edate, co):
        self.sdate = sdate
        self.edate = edate
        self.co = co

    # output: .bz2/gz.txt.gz files
    def parse_updates(self, listfile): # all update files from one collectors/list
        flist = open(listfile, 'r')
        for line in flist:
            line = line.split('|')[0].replace('.txt.gz', '') # get the original .bz2/gz file name
            if not os.path.exists(datadir+line+'.txt.gz'):
                cmlib.parse_mrt(line, line+'txt') # .bz2/gz => .bz2/gz.txt
                cmlib.pack_gz(line+'txt') # .bz2/gz.txt => .bz2/gz.txt.gz
                os.remove(line)  # remove the original .bz2/.gz file
            else:
                pass
        flist.close()
        return 0

    def del_tabletran_updates(self, peer, sdate, cl_name, cl_type):
        f_results = open(datadir+'tmp/'+peer+'_result.txt', 'r')
        for line in f_results:  # get all affection info of this peer
            line = line.replace('\n', '')
            attr = line.split(',')
            if attr[0] == '#START':
                continue

            print line
            print 'get session reset time...'
            stime_unix = int(attr[0])
            endtime_unix = int(attr[1])
            start_datetime = datetime.datetime.fromtimestamp(stime_unix) +\
                    datetime.timedelta(hours=-8)
            end_datetime = datetime.datetime.fromtimestamp(endtime_unix) +\
                    datetime.timedelta(hours=-8)
            print 'from ', start_datetime, ' to ', end_datetime

            updatefile_list =\
                    open(datadir+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')
            for updatefile in updatefile_list:  
                updatefile = updatefile.replace('\n', '')
                file_attr = updatefile.split('.')
                if cl_type == 0:
                    fattr_date, fattr_time = file_attr[3], file_attr[4]
                else:
                    fattr_date, fattr_time = file_attr[5], file_attr[6]
                dt = datetime.datetime(int(fattr_date[0:4]),\
                        int(fattr_date[4:6]), int(fattr_date[6:8]),\
                        int(fattr_time[0:2]), int(fattr_time[2:4]))

                if not start_datetime + datetime.timedelta(minutes =\
                        -15) <= dt <= end_datetime:  # filename not OK
                    continue
                print 'session reset exists in: ', updatefile
                size_before = os.path.getsize(updatefile)
                # unpack
                myfilename = updatefile.replace('txt.gz', 'txt')  # .txt file
                subprocess.call('gunzip -c ' + updatefile + ' > ' +\
                        myfilename, shell=True)
                # only .txt from now on!
                oldfile = open(myfilename, 'r')
                newfile = open(datadir+'tmp/'+myfilename.split('/')[-1], 'w')

                counted_pfx = patricia.trie(None)
                for updt in oldfile:  # loop over each update
                    updt = updt.replace('\n', '')
                    update_attr = updt.split('|')
                    if (cmp(update_attr[3], peer)==0)\
                    & (stime_unix<int(update_attr[1])<\
                    endtime_unix):  # culprit update confirmed
                        pfx = update_attr[5]
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
                       
            updatefile_list.close()
        f_results.close()
        return 0

    def get_peers(self, rib_comp_loc): # file name end with .bz2/gz.txt.gz
        peers = []
        txtfile = rib_comp_loc.replace('txt.gz', 'txt')
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

    def get_update_list(self, clctr, listfile):
        if int(self.sdate) < int(all_collectors[clctr]):
            print 'error: this collector started too late'
            logging.error('collector\' start date too late')
            return -1

        # TODO
        ''' Seems useless anymore
        if clctr.startswith('rrc'):
            datadir_detail = datadir + 'data.ris.ripe.net/' + clctr + '/'
        else:
            datadir_detail = datadir + 'archive.routeviews.org/' + clctr + '/bgpdata/'
            datadir_detail = datadir_detail.replace('//', '/') # when cl = ''
        '''

        # change into month quantity for easy calculation
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


        tmp_filename = listfile.split('/')[-1]
        tmp_dir = listfile.replace(tmp_filename, '')
        cmlib.make_dir(tmp_dir)
        flist = open(listfile, 'w')  

        for month in month_list:
            web_location = ''
            if clctr.startswith('rrc'):
                web_location = 'data.ris.ripe.net/'+clctr+'/'+month+'/' 
            else:
                #web_location = 'archive.routeviews.org/' +\ # XXX I don't know why change
                web_location = 'routeviews.org/' + clctr + '/bgpdata/' + month + '/UPDATES/'
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
                # .bz2/gz.txt.gz file list
                flist.write(web_location+filename+'.txt.gz|'+str(fsize)+'\n')
                logging.info('record file name: '+web_location+filename+'.txt.gz|'+str(fsize))

        return 0


    def download_updates(self, listfile):
        if TEST: # XXX Just read several files when testing
            testcount = 0

        f = open(listfile, 'r')
        for line in f:
            line = line.replace('\n', '').replace('.txt.gz', '') # get original .bz2/gz name
            filename = tmp.split('/')[-1]
            web_location = tmp.replace(filename, '') 
            fsize = int(line.split('|')[1])

            # remove (if) existing xx.txt file to make things clearer
            # consequence: only XXX.bz2/.gz or XXX.bz2/gz.txt.gz exists
            if os.path.exists(web_location+filename+'.txt'):
                os.remove(web_location+filename+'.txt')

            if os.path.exists(web_location+filename+'.txt.gz'): # parsed file exists
                if os.path.getsize(web_location+filename+'.txt.gz') > 0.1 * fsize: # size OK
                    if os.path.exists(web_location+filename):  # .bz2/.gz useless anymore
                        os.remove(web_location+filename)
                    continue
                else:
                    os.remove(web_location+filename+'.txt.gz')

            if os.path.exists(web_location+filename): # original file exists
                if os.path.getsize(web_location+filename) > 0.95 * fsize: # size OK
                    continue
                else:
                    os.remove(web_location+filename)

            cmlib.force_download_file('http://'+web_location, datadir+web_location, filename) 
            print 'Downloading ' + 'http://'+web_location + filename
            logging.info('Downloading ' + 'http://'+web_location + filename)

            if TEST: # XXX only download 5 files when testing
                testcount += 1
                if testcount == 5:
                    break
        f.close()

        return 0

    # Note: update and RIB file formats are either .bz2/gz or .xx.txt.gz!
    def get_all_updates(self, listfile):
        for clctr in self.co:
            listname = self.get_update_list(clctr, listfile)
            if listname == -1: # fail to create
                continue
            self.download_updates(listfile)
            self.parse_updates(listfile)
            rib_full_loc = self.get_parse_one_rib(clctr)
            #self.delete_reset(rib_full_loc, listname) # should after RIB download and parse
        #self.combine_flist(order)

        return 0

    def get_parse_one_rib(self, clctr):
        tmp_month = self.sdate[0:4] + self.sdate[4:6]
        if clctr.startswith('rrc'):
            filelocation = 'data.ris.ripe.net/' + cl_name + '/' + tmp_month + '/' 
        else:
            filelocation = 'routeviews.org/' + cl_name + '/bgpdata/' + tmp_month + '/RIBS/'
            filelocation = filelocation.replace('//', '/')
        webraw = cmlib.get_weblist('http://' + filelocation)

        cmlib.make_dir(datadir+filelocation)

        # download one RIB on or near sdate only for deleting reset
        # TODO: if duration too long, we may need multiple RIBs
        rib_list = webraw.split('\n')
        filter(lambda a: a != '', rib_list)
        filter(lambda a: a != '\n', rib_list)
        rib_list = [item for item in rib_list if 'rib' in item or 'bview' in item]

        # TODO: avoid the RIB with strange size
        target_line = '' # the RIB file for downloading
        closest = 99999
        for line in rib_list:
            fdate = line.split()[0].split('.')[-3]
            diff = abs(int(fdate)-int(self.sdate)) # >0
            if diff < closest:
                closest = diff
                target_line = line

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
            if os.path.getsize(full_loc+'.txt.gz') > 0.1 * fsize:
                if os.path.exists(full_loc):  # .bz2/.gz useless anymore
                    os.remove(full_loc)
                return full_loc+'.txt.gz'
            else:
                os.remove(full_loc+'.txt.gz') # too small to be complete
                cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)

        if os.path.exists(full_loc): 
            if os.path.getsize(full_loc) <= 0.95 * fsize:
                os.remove(full_loc)
                cmlib.force_download_file('http://'+web_location, datadir+web_location, filename)
            else:
                pass

        cmlib.parse_mrt(full_loc, full_loc+'.txt')
        os.remove(full_loc)  # then remove .bz2/.gz
        cmlib.pack_gz(full_loc+'.txt')

        return full_loc+'.txt.gz'


    def delete_reset(self, rib_comp_loc, listname):
        peers = self.get_peers(rib_comp_loc)
        print 'peers: ', peers
        
        print 'determining table transfers start and end time for each peer...'
        if TEST:
            peers = peers[0:2]
        for peer in peers:  # must process each peer one by one
            peer = peer.rstrip()
            print 'processing ',peer,'...'
            subprocess.call('perl '+homedir+'tool/bgpmct.pl -rf '+rib_comp_loc+' -ul '+\
                    listname+' -p '+peer+' > '+\
                    datadir+'tmp/'+peer+'_result.txt', shell=True)
                
        print 'delete updates caused by session reset for each peer...'
        for peer in peers:
            # No reset from this peer, so nothing in the file
            try:
                if os.path.getsize(datadir+'tmp/'+peer+'_result.txt') == 0:
                    continue
            except: # cannot find file
                continue
            print '\nculprit now: ', peer
            self.del_tabletran_updates(peer, sdate, cl_name, cl_type)

        # delete all rubbish in the end
        subprocess.call('rm '+datadir+'tmp/*', shell=True)
                            
        return 0

if __name__ == '__main__':
    order_list = [27]
    collector_list = ['', 'rrc00']
    for order in order_list:
        sdate = daterange[order][0]
        edate = daterange[order][1]
        for co in collector_list:
            dl = Downloader(sdate, edate, co)
            listfile = datadir + 'update_list/' + sdate + '_' + edate + '/' + co + '_list.txt'
            dl.get_all_updates(listfile) # para: update list file location
    #dl = Downloader([0])
    #dl.get_file()
