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

        if os.path.exists(hdname+'metadata/'+sdate+'/updt_filelist_comb'):
            os.remove(hdname+'metadata/'+sdate+'/updt_filelist_comb')
        fcomb = open(hdname+'metadata/'+sdate+'/updt_filelist_comb', 'w')  # .bz2.txt.gz file name
        for fn in fnlist:
            fcomb.write(fn+'\n')
        fcomb.close()
        return 0

    # result: leaving only .bz2/gz.txt.gz update files
    def parse_update(self, listname): # all update files from one collectors/list
        flist = open(listname, 'r')
        for line in flist:
            line = line.split('|')[0]
            line = line.replace('.txt.gz', '') # get the original .bz2/gz file name
            if not os.path.exists(hdname+line+'.txt.gz'):
                cmlib.parse_mrt(line, line+'txt') # .bz2/gz => .bz2/gz.txt
                cmlib.pack_gz(line+'txt') # .bz2/gz.txt => .bz2/gz.txt.gz
                os.remove(line)  # remove the original .bz2/.gz file
            else:  # bz2/gz.txt.gz file already exists
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

    def get_update_list(self, order, clctr):
        try:
            if int(daterange[order][0]) < int(clctr[2]):
                print 'error: this collector started too late'
                return -1
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
        if os.path.exists(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name):
            os.remove(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name)
        flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'w')  

        for ym in yearmonth:
            web_location = ''
            if cl_type == 0:
                #web_location = 'archive.routeviews.org/' +\
                web_location = 'routeviews.org/' +\
                        cl_name + '/bgpdata/' + ym + '/UPDATES/'
                web_location = web_location.replace('//', '/')  # when name is ''
                webraw = cmlib.get_weblist('http://' + web_location)
            else:
                web_location = 'data.ris.ripe.net/'+cl_name+'/'+ym+'/' 
                webraw = cmlib.get_weblist('http://' + web_location)

            cmlib.make_dir(hdname+web_location)

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
                # .bz2/gz.txt.gz file list
                flist.write(web_location+filename+'.txt.gz|'+str(fsize)+'\n')
                print web_location+filename+'.txt.gz|'+str(fsize)

        return hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name # listname


    def get_update(self, listname):
        f = open(listname, 'r')
        if TEST: # Just read several files when testing
            testcount = 0
        for line in f:
            line = line.replace('\n', '')
            line = line.replace('.txt.gz', '') # for getting original .bz2/gz name
            tmp = line.split('|')[0] # == web location + filename
            filename = tmp.split('/')[-1]
            web_location = tmp.replace(filename, '') 
            fsize = int(line.split('|')[1])

            # remove existing xx.txt file to make things clearer
            try:
                os.remove(web_location+filename+'.txt')
            except:
                pass

            # consequence: ???.bz2/.gz or ???.bz2/gz.txt.gz

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

            cmlib.force_download_file('http://'+web_location, hdname+web_location, filename) 
            print 'Downloading ' + 'http://'+web_location

            if TEST:
                testcount += 1
                if testcount == 5:
                    break

        f.close()

        return 0

    ## update and RIB file formats are either .bz2/gz or .xx.txt.gz!
    def get_file(self):
        for order in self.order_list:
            for clctr in collectors:
                listname = self.get_update_list(order, clctr)
                if listname == -1: # fail
                    continue
                #self.get_update(listname)
                #self.parse_update(listname)
                #rib_comp_loc = self.get_parse_rib(order, clctr)
                #self.delete_reset(rib_comp_loc, listname) # should after RIB download and parse
            self.combine_flist(order)

        return 0

    def get_parse_rib(self, order, clctr):
        cl_name = clctr[0]
        cl_type = clctr[1]
        if cl_type == 0:
            #filelocation = 'archive.routeviews.org/' +\  # RV abandoned this
            filelocation = 'routeviews.org/' +\
                    cl_name + '/bgpdata/' + yearmonth[0] + '/RIBS/'
            filelocation = filelocation.replace('//', '/')  # when name is ''
            webraw = cmlib.get_weblist('http://' + filelocation)
        else:
            filelocation = 'data.ris.ripe.net/' + cl_name + '/' + yearmonth[0] + '/' 
            webraw = cmlib.get_weblist('http://' + filelocation)
        
        cmlib.make_dir(hdname+filelocation)
        sdate = daterange[order][0]

        # for each event, we only download one RIB (on or near the sdate)
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

        size = target_line.split()[-1] # claimed RIB file size
        if size.isdigit():
            fsize = float(size)
        else:
            fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])

        filename = target_line.split()[0]
        complete_loc = hdname + web_location + filename # .bz2/.gz

        try:
            os.remove(complete_loc+'.txt')
        except:
            pass

        if os.path.exists(complete_loc+'.txt.gz'): 
            if os.path.getsize(complete_loc+'.txt.gz') > 0.1 * fsize:
                if os.path.exists(complete_loc):  # .bz2/.gz useless anymore
                    os.remove(complete_loc)
                return complete_loc+'.txt.gz'
            else:
                os.remove(complete_loc+'.txt.gz') # too small to be complete
                cmlib.force_download_file('http://'+web_location, hdname+web_location, filename)

        if os.path.exists(complete_loc): 
            if os.path.getsize(complete_loc) <= 0.9 * fsize:
                os.remove(complete_loc)
                cmlib.force_download_file('http://'+web_location, hdname+web_location, filename)
            else:
                pass

        cmlib.parse_mrt(complete_loc, complete_loc+'.txt')
        os.remove(complete_loc)  # then remove .bz2/.gz
        cmlib.pack_gz(complete_loc+'.txt')

        return complete_loc+'.txt.gz'


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
                            
        return 0

if __name__ == '__main__':
    dl = Downloader([0])
    dl.get_file()
