import cmlib
import datetime
import patricia
import subprocess

from env import *
# TODO: change file name for RV when time < Feb, 2003.
TEST = False

class Downloader():

    def __init__(self, order_list):
        self.order_list = order_list

    # TODO not flexible
    def combine_flist(self, sdate):
        fnames = {}
        clist = cmlib.get_collector(sdate)
        for cl_name in clist:
            if cl_name.startswith('rrc'):
                cl_type = 1 # collector type
            else:
                cl_type = 0
            # .bz2.txt.gz file name
            flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')
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

        fcomb = open(hdname+'metadata/'+sdate+'/updt_filelist_comb', 'w')  # .bz2.txt.gz file name
        for fn in fnlist:
            fcomb.write(fn+'\n')
        fcomb.close()
        return 0

    def parse_updates(self, sdate, cl_name): # all updates from a collectors
        flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')  # .xx.txt.gz file name
        for line in flist:
            line = line.replace('\n', '')
            if not os.path.exists(line):  # xx.txt.gz not exists, .bz2/.gz exists
                print line
                cmlib.parse_mrt(line.replace('.txt.gz', ''), line.replace('txt.gz', 'txt'))
                cmlib.pack_gz(line.replace('txt.gz', 'txt'))
                os.remove(line.replace('.txt.gz', ''))  # remove .bz2/.gz update files
            else:  # xx.txt.gz exists
                pass
        flist.close()
        return 0

    def del_tabletran_updates(self, peer, sdate, cl_name, cl_type):
        f_results = open(hdname+'tmp/'+peer+'_result.txt', 'r')
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
                    open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')
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
                newfile = open(hdname+'tmp/'+myfilename.split('/')[-1], 'w')

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
                subprocess.call('gzip -c '+hdname+'tmp/'+myfilename.split('/')[-1]+\
                        ' > '+updatefile, shell=True)
                size_after = os.path.getsize(updatefile)
                print 'size(b):', size_before, ',size(a):', size_after
                       
            updatefile_list.close()
        f_results.close()
        return 0

    def get_peers(self, rib_location): # should end with .bz2/.gz
        print rib_location
        peers = []
        # get .txt
        if os.path.exists(rib_location+'.txt.gz'):  # .xx.txt.gz file exists
            subprocess.call('gunzip '+rib_location+'.txt.gz', shell=True)  # unpack                        
        elif os.path.exists(rib_location):  # .bz2/.gz file exists
            cmlib.parse_mrt(rib_location, rib_location+'.txt')
            os.remove(rib_location)  # then remove .bz2/.gz
        # read .txt
        with open(rib_location+'.txt', 'r') as f:  # get peers from RIB
            for line in f:
                try:
                    addr = line.split('|')[3]
                    if addr not in peers:
                        peers.append(addr)
                except:
                    pass
        f.close()
        # compress RIB into .gz
        if not os.path.exists(rib_location+'.txt.gz'):
            cmlib.pack_gz(rib_location+'.txt')

        return peers

    def get_update_list(self, order, clctr):
        try:
            if int(daterange[order][0]) < int(clctr[2]):
                print 'error: this collector started too late'
                continue
        except:  # usually when testing, clctr[2] may not be set
            pass

        cl_name = clctr[0]
        cl_type = clctr[1]

        if cl_type == 0: # RouteViews
            hdname_detail = hdname + 'archive.routeviews.org/' + cl_name +\
                '/bgpdata/'
            hdname_detail = hdname_detail.replace('//', '/') # happens when cl = ''
        else:
            hdname_detail = hdname + 'data.ris.ripe.net/' + cl_name + '/'

        sdate = daterange[order][0]
        sdate_obj = datetime.datetime.strptime(sdate, '%Y%m%d').date()
        edate_obj = sdate_obj + datetime.timedelta(days=(daterange[order][1]-1))
        edate = edate_obj.strftime('%Y%m%d')

        # Now we can only deal with at most 2 months
        # TODO handle more months
        yearmonth = [] 
        yearmonth.append(sdate[0:4] + '.' + sdate[4:6])
        if edate[0:4] + '.' + edate[4:6] not in yearmonth:
            yearmonth.append(edate[0:4] + '.' + edate[4:6])

        cmlib.make_dir(hdname+'metadata/'+sdate)
        flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'w')  

        for ym in yearmonth:
            filelocation = ''
            if cl_type == 0:
                #filelocation = 'archive.routeviews.org/' +\
                filelocation = 'routeviews.org/' +\
                        cl_name + '/bgpdata/' + ym + '/UPDATES/'
                filelocation = filelocation.replace('//', '/')  # when name is ''
                webraw = cmlib.get_weblist('http://' + filelocation)
            else:
                filelocation = 'data.ris.ripe.net/'+cl_name+'/'+ym+'/' 
                webraw = cmlib.get_weblist('http://' + filelocation)

            cmlib.make_dir(hdname+filelocation)

            for line in webraw.split('\n'):
                if not 'updates' in line or line == '' or line == '\n':
                    continue

                size = line.split()[-1]
                if size.isdigit():
                    fsize = float(size)
                else:
                    fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])
                filename = line.split()[0]  # omit uninteresting info
                filedate = filename.split('.')[-3]

                # check whether its datetime in our range
                if int(filedate) < int(sdate) or int(filedate) > int(edate):
                    continue

                flist.write(filelocation+filename+'|'+str(fsize)+'\n')  # .bz2/.gz file list

        return hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name


    def get_parse_update(self, listname):
        f = open(listname, 'r')
        if TEST: # Just read several files when testing
            testcount = 0
        for line in f:
            line = line.replace('\n', '')
            updatefile = line.split('|')[0] # == filelocation + filename
            filename = updatefile.split('/')[-1]
            filelocation = updatefile.replace(filename, '') 
            fsize = int(line.split('|')[1])

            # remove existing xx.txt file to make things clearer
            try:
                os.remove(updatefile+'.txt')
            except:
                pass

            if os.path.exists(updatefile+'.txt.gz'): # parsed file exists
                if os.path.getsize(updatefile+'.txt.gz') > 0.1 * fsize: # size OK
                    if os.path.exists(updatefile):  # .bz2/.gz useless anymore
                        os.remove(updatefile)
                    continue
                else:
                    os.remove(updatefile+'.txt.gz')

            if os.path.exists(updatefile): # original file exists
                if os.path.getsize(updatefile) > 0.95 * fsize: # size OK
                    continue
                else:
                    os.remove(updatefile)

            cmlib.force_download_file('http://'+filelocation, hdname+filelocation, filename) 
            print 'Downloading ' + 'http://'+filelocation

            if TEST:
                testcount += 1
                if testcount == 5:
                    break

        f.close()

        return 0

    def get_file(self):
        for order in self.order_list:
            for clctr in collectors:
                listname = self.get_update_list(order, clctr)
                ## update and RIB file formats are either .bz2/gz or .xx.txt.gz!
                self.get_parse_update(listname)
                print 'parsing updates...'
                # TODO clctr
                self.parse_updates(sdate, cl_name)
                self.get_parse_rib(order, clctr)

        return 0

    def get_parse_rib(self, order, clctr):
        cl_name = clctr[0]
        cl_type = clctr[1]
        if cl_type == 0:
            #filelocation = 'archive.routeviews.org/' +\
            filelocation = 'routeviews.org/' +\
                    cl_name + '/bgpdata/' + yearmonth[0] + '/RIBS/'
            filelocation = filelocation.replace('//', '/')  # when name is ''
            webraw = cmlib.get_weblist('http://' + filelocation)
        else:
            filelocation = 'data.ris.ripe.net/' + cl_name + '/' + yearmonth[0] + '/' 
            webraw = cmlib.get_weblist('http://' + filelocation)
        
        cmlib.make_dir(hdname+filelocation)

        sdate = daterange[order][0]
        sdate_obj = datetime.datetime.strptime(sdate, '%Y%m%d').date()

        # for each event, we only download one RIB (on or near the sdate)
        rib_fname = ''
        rib_list = webraw.split('\n')
        filter(lambda a: a != '', rib_list)
        filter(lambda a: a != '\n', rib_list)
        rib_list = [item for item in rib_list if 'rib' in item or 'bview' in item]

        target_line = ''
        closest = 99999
        for line in rib_list:
            fdate = line.split()[0].split('.')[-3]
            diff = abs(int(fdate)-int(sdate)) # >0
            if diff < closest:
                closest = diff
                target_line = line

        size = target_line.split()[-1]
        if size.isdigit():
            fsize = float(size)
        else:
            fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])

        filename = target_line.split()[0]
        print filename
        origin_floc = hdname + filelocation + filename # RIB .bz2/.gz loc+name

        try:
            os.remove(origin_floc+'.txt')
        except:
            pass

        rib_fname = filelocation + filename
        if os.path.exists(origin_floc+'.txt.gz'): 
            if os.path.getsize(origin_floc+'.txt.gz') > 0.1 * fsize:
                if os.path.exists(origin_floc):  # .bz2/.gz useless anymore
                    os.remove(origin_floc)
            else:
                os.remove(origin_floc+'.txt.gz')

        if os.path.exists(origin_floc): 
            if os.path.getsize(origin_floc) <= 0.9 * fsize:
                os.remove(origin_floc)

        cmlib.force_download_file('http://'+filelocation, hdname+filelocation, filename)

        return 0


                print 'parsing RIB and getting peers...'
                rib_location = hdname + rib_fname  # .bz2/.gz
                peers = self.get_peers(rib_location)
                print 'peers: ', peers
                
                print 'determining table transfers start and end time for each peer...'
                if TEST:
                    peers = peers[0:2]
                for peer in peers:  # must process each peer one by one
                    peer = peer.rstrip()
                    print 'processing ',peer,'...'
                    subprocess.call('perl '+homedir+'tool/bgpmct.pl -rf '+rib_location+'.txt.gz'+' -ul '+\
                            hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name+' -p '+peer+' > '+\
                            hdname+'tmp/'+peer+'_result.txt', shell=True)
                        
                print 'delete updates caused by session reset for each peer...'
                for peer in peers:
                    # No reset from this peer, so nothing in the file
                    try:
                        if os.path.getsize(hdname+'tmp/'+peer+'_result.txt') == 0:
                            continue
                    except: # cannot find file
                        continue
                    print '\nculprit now: ', peer
                    self.del_tabletran_updates(peer, sdate, cl_name, cl_type)

                # delete all rubbish in the end
                subprocess.call('rm '+hdname+'tmp/*', shell=True)
                                    
            self.combine_flist(sdate)
        return 0

if __name__ == '__main__':
    dl = Downloader([24])
    dl.get_file()
