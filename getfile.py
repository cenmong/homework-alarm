import cmlib
from env import *
import datetime
import patricia
import subprocess

global TEST
#TEST = True
TEST = False

if TEST:
    collectors = [('', 0), ('rrc00', 1), ('rrc01', 1),]
    #collectors = [('rrc01', 1)]

# TODO: change file name: RV & < Feb, 2003.

def combine_flist(sdate):
    fnames = {}
    clist = cmlib.get_collector(sdate)
    for cl_name in clist:
        if cl_name.startswith('rrc'):
            cl_type = 1
        else:
            cl_type = 0

        flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')  # .bz2.txt.gz file name
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


def parse_updates(sdate, cl_name):
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


def del_tabletran_updates(peer, sdate, cl_name, cl_type):
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

def get_peers(rib_location): # should end with .bz2/.gz
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

def get_file():
    for i in xrange(13, 16):
        for clctr in collectors:
            try:
                if int(daterange[i][0]) < int(clctr[2]):
                    print'this collector is born later than we want'
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

            sdate = daterange[i][0]
            sdate_obj = datetime.datetime.strptime(sdate, '%Y%m%d').date()
            edate_obj = sdate_obj + datetime.timedelta(days=(daterange[i][1]-1))
            edate = edate_obj.strftime('%Y%m%d')

            # Now we can only deal with at most 2 months
            yearmonth = [] 
            yearmonth.append(sdate[0:4] + '.' + sdate[4:6])
            if edate[0:4] + '.' + edate[4:6] not in yearmonth:
                yearmonth.append(edate[0:4] + '.' + edate[4:6])

            cmlib.make_dir(hdname+'metadata/'+sdate)
            flist = open(hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name, 'w')  

            # only for downloading updates, not RIBs
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

                if TEST:
                    testcount = 0  # TODO: testonly

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

                    print filename

                    if TEST:
                        testcount += 1 #TODO: 3 lines testonly
                        if testcount == 5:
                            break

                    origin_floc = hdname + filelocation + filename # original file loc&name
                    flist.write(origin_floc+'.txt.gz\n')  # .xx.txt.gz file list

                    # remove existing xx.txt file to make things clearer
                    try:
                        os.remove(origin_floc+'.txt')
                    except:
                        pass

                    if os.path.exists(origin_floc+'.txt.gz'):
                        if os.path.getsize(origin_floc+'.txt.gz') > 0.1 * fsize:
                            if os.path.exists(origin_floc):  # .bz2/.gz useless anymore
                                os.remove(origin_floc)
                            continue
                        else:
                            os.remove(origin_floc+'.txt.gz')

                    if os.path.exists(origin_floc):
                        if os.path.getsize(origin_floc) > 0.9 * fsize:
                            continue
                        else:
                            os.remove(origin_floc)


                    cmlib.force_download_file('http://'+filelocation, hdname+filelocation, filename) 

            # file that stores update list
            flist.close()

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

            # for each event, we only download one RIB (on the sdate)
            rib_fname = ''
            for line in webraw.split('\n'):

                if not 'rib' in line and not 'bview' in line:
                    continue
                if line == '' or line == '\n':
                    continue

                size = line.split()[-1]
                if size.isdigit():
                    fsize = float(size)
                else:
                    fsize = float(size[:-1]) * cmlib.size_u2v(size[-1])

                filename = line.split()[0]
                if not int(filename.split('.')[-3]) == int(sdate):
                    continue
                print filename
                origin_floc = hdname + filelocation + filename # original file loc&name

                try:
                    os.remove(origin_floc+'.txt')
                except:
                    pass

                rib_fname = filelocation + filename
                if os.path.exists(origin_floc+'.txt.gz'): 
                    if os.path.getsize(origin_floc+'.txt.gz') > 0.1 * fsize:
                        if os.path.exists(origin_floc):  # .bz2/.gz useless anymore
                            os.remove(origin_floc)
                        break
                    else:
                        os.remove(origin_floc+'.txt.gz')

                if os.path.exists(origin_floc): 
                    if os.path.getsize(origin_floc) > 0.9 * fsize:
                        break
                    else:
                        os.remove(origin_floc)

                cmlib.force_download_file('http://'+filelocation, hdname+filelocation, filename)

                break

            ## now for update and RIB files, their formats are either .bz2/gz or
            ## .xx.txt.gz!!!

            print 'parsing updates...'
            parse_updates(sdate, cl_name)

            print 'parsing RIB and getting peers...'
            rib_location = hdname + rib_fname  # .bz2/.gz
            peers = get_peers(rib_location)
            print 'peers: ', peers
            
            print 'determining table transfers start and end time for each peer...'
            if TEST:
                peers = peers[0:2]  # TODO: testonly
            for peer in peers:  # must process each peer one by one
                peer = peer.rstrip()
                print 'processing ',peer,'...'
                subprocess.call('perl '+homedir+'tool/bgpmct.pl -rf '+rib_location+'.txt.gz'+' -ul '+\
                        hdname+'metadata/'+sdate+'/updt_filelist_'+cl_name+' -p '+peer+' > '+\
                        hdname+'tmp/'+peer+'_result.txt', shell=True)
                    
            print 'delete updates caused by session reset for each peer...'
            for peer in peers:
                # No reset from this peer, so nothing in the file
                if os.path.getsize(hdname+'tmp/'+peer+'_result.txt') == 0:
                    continue
                print '\nculprit now: ', peer
                del_tabletran_updates(peer, sdate, cl_name, cl_type)

            # delete all rubbish in the end
            subprocess.call('rm '+hdname+'tmp/*', shell=True)
                                
        combine_flist(sdate)
    return


if __name__ == '__main__':
    get_file()
