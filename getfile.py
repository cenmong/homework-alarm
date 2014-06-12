import os
from env import *
import datetime
import patricia

# 0: routeviews; 1: ripe ris
'''
collectors = [('', 0), ('rrc00', 1), ('rrc01', 1), ('rrc03', 1),\
             ('rrc04', 1), ('rrc05', 1), ('rrc06', 1), ('rrc07', 1),\
             ]
'''
# TODO: testonly
#collectors = [('', 0), ('rrc00', 1), ('rrc01', 1),]
# TODO: testonly
collectors = [('rrc00', 1)]

# number of days in total
daterange = [('20061225', 4, '2006 taiwan cable cut'),\
            ('20081218', 4, '2008 mediterranean cable cut 2'),\
            ('20050911', 4, 'LA blackout'),\
            ]
def get_file():

    for i in range(0, 1):
        for clctr in collectors:
            # get basic info of this collector
            cl_name = clctr[0]
            cl_type = clctr[1]
            if cl_type == 0:
                hdname_detail = hdname + 'archive.routeviews.org/' + cl_name +\
                    '/bgpdata/'
                hdname_detail = hdname_detail.replace('//', '/') # happens when cl = ''
            else:  # cl_type == 1
                hdname_detail = hdname + 'data.ris.ripe.net/' + cl_name + '/'

            # get date range info, sdate and edate are strings
            sdate = daterange[i][0]
            sdate_obj = datetime.datetime.strptime(sdate, '%Y%m%d').date()
            edate_obj = sdate_obj + datetime.timedelta(days=(daterange[i][1]-1))
            edate = edate_obj.strftime('%Y%m%d')

            # Now it can only deal with at most 2 months
            yearmonth = [] 
            yearmonth.append(sdate[0:4] + '.' + sdate[4:6])
            if edate[0:4] + '.' + edate[4:6] not in yearmonth:
                yearmonth.append(edate[0:4] + '.' + edate[4:6])

            ## download updatefile
            if not os.path.isdir('metadata/'+sdate):
                os.system('mkdir metadata/'+sdate)
            # TODO: if list file exists, means this collector fully processed
            flist = open('metadata/'+sdate+'/updt_filelist_'+cl_name, 'w')  # .bz2.txt.gz file name
            for ym in yearmonth:
                if cl_type == 0:
                    loc = 'archive.routeviews.org/' +\
                            cl_name + '/bgpdata/' + ym + '/UPDATES/'
                    loc = loc.replace('//', '/')  # when name is ''
                    loc = 'http://' + loc
                    os.system('lynx -dump '+loc+' > tmpfile')
                else:
                    os.system('lynx -dump\
                            http://data.ris.ripe.net/'+cl_name+'/'+ym+'/ > tmpfile')

                f = open('tmpfile', 'r')

                #testcount = 0  # TODO: testonly
                for line in f.readlines():
                    try:
                        if line.split('.')[-4].split('/')[1] == 'bview':  # RIB file name
                            continue
                    except:
                        pass
                    if line.split('.')[-1] != 'bz2\n' and\
                            line.split('.')[-1] != 'gz\n':
                        continue
                    if int(line.split('.')[-3]) < int(sdate) or\
                            int(line.split('.')[-3]) > int(edate):
                        continue

                    # TODO: testonly
                    #testcount += 1
                    #if testcount == 5:
                    #    break
                        
                    # get the name of a .bz2/gz update file
                    updt_fname = line.split('//')[1].replace('\n', '')
                    flist.write(hdname+updt_fname+'.txt.gz\n')  # .xx.txt.gz file list
                    if os.path.exists(hdname+updt_fname+'.txt.gz'):
                        print '.xx.txt.gz update file exists!'
                        if os.path.exists(hdname+updt_fname):  # .bz2/.gz useless anymore
                            os.system('rm '+hdname+updt_fname)
                        continue
                    if os.path.exists(hdname+updt_fname):
                        print '.bz2/.gz update file exists!'
                        continue
                    # remove existing xx.txt file to make things clearer
                    if os.path.exists(hdname+updt_fname+'.txt'):
                        os.system('rm '+hdname+updt_fname+'.txt')

                    if cl_type == 0:  # get .bz2
                        os.system('wget -e robots=off --connect-timeout=3000\
                                -np -P '+hdname+' -c -m -r -A.bz2\
                                http://'+updt_fname)
                    else:  # == 1 get .gz
                        os.system('wget -e robots=off --connect-timeout=3000\
                                -np -P '+hdname+' -c -m -r -A.gz\
                                http://'+updt_fname)

                f.close()
                os.system('rm tmpfile')
                if cl_type == 0:
                    os.system('rm '+hdname_detail+ym+'/UPDATES/*.txt')
                else:
                    os.system('rm '+hdname_detail+ym+'/*.txt')

            flist.close()

            # TODO: change file name: RV & < Feb, 2013.


            ## Download RIB. RIB date is always the same as updates' start date
            ribtime = sdate
            # TODO: RIPE
            # only download rib of the starting date!
            if cl_type == 0:
                loc = 'archive.routeviews.org/' +\
                        cl_name + '/bgpdata/' + ym + '/RIBS/'
                loc = loc.replace('//', '/')  # when name is ''
                loc = 'http://' + loc
                os.system('lynx -dump '+loc+' > tmpfile')
            else:
                os.system('lynx -dump\
                        http://data.ris.ripe.net/'+cl_name+'/'+ym+'/ > tmpfile')
            f = open('tmpfile', 'r')
            for line in f.readlines():
                if line.split('.')[-1] != 'bz2\n' and\
                        line.split('.')[-1] != 'gz\n':
                    continue
                if line.split('.')[-4].split('/')[1] == 'updates':  # only on NCC RIS
                    continue
                if int(line.split('.')[-3]) == int(ribtime):  # right date
                    rib_fname = line.split('//')[1].replace('\n', '')
                    if os.path.exists(hdname+rib_fname+'.txt.gz'): 
                        print '.xx.txt.gz RIB file exists!'
                        if os.path.exists(hdname+rib_fname): 
                            os.system('rm '+hdname+rib_fname) # .bz2/.gz useless
                        break
                    if os.path.exists(hdname+rib_fname): 
                        print '.bz2/.gz RIB file exists!'
                        break
                    if os.path.exists(hdname+rib_fname+'.txt'): 
                        os.system('rm '+hdname+rib_fname+'.txt')
                    if cl_type == 0:
                        os.system('wget -e robots=off --connect-timeout=3000 -np -P\
                                '+hdname+' -c -m -r -A.bz2 http://'+rib_fname)
                    else:
                        os.system('wget -e robots=off --connect-timeout=3000 -np -P\
                                '+hdname+' -c -m -r -A.gz http://'+rib_fname)
                    break
                else:
                    pass
            f.close()
            os.system('rm tmpfile')


            ## now for update and RIB files, their formats are either .bz2/gz or
            ## .xx.txt.gz!!!


            print 'parsing updates...'
            flist = open('metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')  # .xx.txt.gz file name
            for line in flist.readlines():
                line = line.replace('\n', '')
                if not os.path.exists(line):  # xx.txt.gz not exists, then .bz2 exists
                    # 6 lines RIPE friendly
                    os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m '+\
                            line.replace('.txt.gz', '')+' > '+\
                            line.replace('txt.gz', 'txt'))  # parse .bz2/.gz into .txt
                    # compress xx.txt-->xx.txt.gz
                    os.system('gzip -c '+line.replace('txt.gz', 'txt')+' > '+line)
                    os.system('rm '+line.replace('.txt.gz', ''))  # remove .bz2/.gz update files
                    os.system('rm '+line.replace('txt.gz', 'txt'))  # remove txt update files
                else:  # xx.txt.gz exists
                    pass
            flist.close()


            print 'parsing RIB and getting peers...'
            rib_location = hdname + rib_fname  # .bz2/.gz
            peers = []
            # get .txt
            if os.path.exists(rib_location+'.txt.gz'):  # .xx.txt.gz file exists
                os.system('gunzip -c '+rib_location+'.txt.gz > '+\
                        rib_location+'.txt')  # unpack                        
            elif os.path.exists(rib_location):  # .bz2/.gz file exists
                os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m\
                        '+rib_location+' > '+rib_location+'.txt')  # parse
                os.system('rm '+rib_location)  # then remove .bz2/.gz
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
            print 'peers: ', peers
            # compress RIB into .gz
            if not os.path.exists(rib_location+'.txt.gz'):
                os.system('gzip -c '+rib_location+'.txt'+' > '+rib_location+'.txt.gz')
            os.system('rm '+rib_location+'.txt')  # remove .txt, only .gz left
            

            print 'determining table transfers start and end time...'
            #peers = peers[0:2]  # TODO: testonly
            for peer in peers:  # must process each peer one by one
                peer = peer.rstrip()
                print peer
                os.system('perl tool/bgpmct.pl -rf '+rib_location+'.txt.gz'+' -ul '+\
                        'metadata/'+sdate+'/updt_filelist_'+cl_name+' -p '+peer+' > '+\
                        'tmp/'+peer+'_result.txt')

                    
            print 'delete updates caused by session reset...'
            for peer in peers:
                # No reset for this peer
                if os.path.getsize('tmp/'+peer+'_result.txt') == 0:
                    continue

                print 'culprit now: ', peer
                f_results = open('tmp/'+peer+'_result.txt', 'r')
                for line in f_results:  # get all affection info of this peer
                    line = line.replace('\n', '')
                    attr = line.split(',')
                    if attr[0] == '#START':
                        continue

                    print line
                    # get session reset time
                    print 'get session reset time...'
                    stime_unix = int(attr[0])
                    endtime_unix = int(attr[1])
                    sdt_tmp = datetime.datetime.fromtimestamp(stime_unix) +\
                            datetime.timedelta(hours=-8)
                    edt_tmp = datetime.datetime.fromtimestamp(endtime_unix) +\
                            datetime.timedelta(hours=-8)
                    sdt_tmp = sdt_tmp.strftime('%Y%m%d.%H%M')
                    start_datetime = datetime.datetime(int(sdt_tmp[0:4]),\
                            int(sdt_tmp[4:6]), int(sdt_tmp[6:8]),\
                            int(sdt_tmp[9:11]), int(sdt_tmp[11:13]))                               
                    edt_tmp = edt_tmp.strftime('%Y%m%d.%H%M')
                    end_datetime = datetime.datetime(int(edt_tmp[0:4]),\
                            int(edt_tmp[4:6]), int(edt_tmp[6:8]),\
                            int(edt_tmp[9:11]), int(edt_tmp[11:13]))                               
                    print 'from ', sdt_tmp, ' to ', edt_tmp

                    # now let's clean updates
                    updatefile_list =\
                            open('metadata/'+sdate+'/updt_filelist_'+cl_name, 'r')
                    for updatefile in updatefile_list.readlines():  
                        updatefile = updatefile.replace('\n', '')
                        file_attr = updatefile.split('.')
                        if cl_type == 0:
                            fattr_date = file_attr[4]
                            fattr_time = file_attr[5]
                        else:
                            fattr_date = file_attr[5]
                            fattr_time = file_attr[6]
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
                        os.system('gunzip -c ' + updatefile + ' > ' +\
                                myfilename)
                        # only .txt from now on!
                        oldfile = open(myfilename, 'r')
                        newfile = open('tmp/'+myfilename.split('/')[-1], 'w')

                        counted_pfx = patricia.trie(None)
                        for updt in oldfile.readlines():  # loop over each update
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

                        os.system('rm '+updatefile)  # remove old .gz file
                        # compress .txt into txt.gz to replace the old file
                        os.system('gzip -c tmp/'+myfilename.split('/')[-1]+\
                                ' > '+updatefile)
                        size_after = os.path.getsize(updatefile)
                        print 'size(b):', size_before, ',size(a):', size_after
                               
                    updatefile_list.close()
                f_results.close()

            os.system('rm tmp/*')
                               
        #TODO: combine lists
        for clctr in collectors:
            cl_name = clctr[0]
    return


if __name__ == '__main__':
    get_file()
