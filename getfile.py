import os
from env import hdname
import datetime

hdname_detail = hdname + 'archive.routeviews.org/bgpdata/'

# TODO: start ym, start ymd, number of days
ym = ['2010.02']
ymd1 = ['20100226']
ymd2 = ['20100226']

def get_file():

    for i in range(0, len(ym)):
        ## download updatefile
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/'+ym[i]+'/UPDATES/ > tmpfile')
        f = open('tmpfile', 'r')
        # this will over-write an existing filslist
        flist = open('metadata/updt_filelist'+ymd1[i], 'w')  # .bz2.txt.gz file name

        testcount = 0  # TODO: test only
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) < int(ymd1[i]) or int(line.split('.')[-3]) > int(ymd2[i]):
                continue

            # TODO: for test only
            testcount += 1
            if testcount == 30:
                break

            updt_fname = line.split('//')[1].replace('\n', '')  # name of .bz2 file
            flist.write(hdname+updt_fname+'.txt.gz\n')  # .bz2.txt.gz file list
            if os.path.exists(hdname+updt_fname+'.txt.gz'):
                print '.bz2.txt.gz update file exists!'
                if os.path.exists(hdname+updt_fname):  # .bz2 useless anymore
                    os.system('rm '+hdname+updt_fname)
                continue
            if os.path.exists(hdname+updt_fname):
                print '.bz2 update file exists!'
                continue
            if os.path.exists(hdname+updt_fname+'.txt'):  # remove existing .txt
                os.system('rm '+hdname+updt_fname+'.txt')

            os.system('wget -e robots=off --connect-timeout=3000 -np -P '+hdname+' -c -m -r -A.bz2\
                    http://'+updt_fname)

        flist.close()
        f.close()
        os.system('rm tmpfile')


        ## download rib
        #dt = datetime.datetime.strptime(ymd1[i], "%Y%m%d").date()
        #ribtime = dt + datetime.timedelta(days=-1)  # RIB is 1 day before
        #ribtime = ribtime.strftime('%Y%m%d')
        ribtime = ymd1[i]
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/'+ym[i]+'/RIBS/ > tmpfile')
        f = open('tmpfile', 'r')
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) == int(ribtime):  # right date
                rib_fname = line.split('//')[1].replace('\n', '')
                # Download files
                if os.path.exists(hdname+rib_fname+'.txt.gz'): 
                    print '.bz2.txt.gz RIB file exists!'
                    if os.path.exists(hdname+rib_fname): 
                        os.system('rm '+hdname+rib_fname) # .bz2 useless
                    break
                if os.path.exists(hdname+rib_fname): 
                    print '.bz2 RIB file exists!'
                    break
                if os.path.exists(hdname+rib_fname+'.txt'): 
                    os.system('rm '+hdname+rib_fname+'.txt')
                os.system('wget -e robots=off --connect-timeout=3000 -np -P\
                        '+hdname+' -c -m -r -A.bz2 http://'+rib_fname)
                break
            else:
                pass
        f.close()
        os.system('rm tmpfile')


        ## now for update and RIB files, their formats are either .bz2 or
        ## .bz2.txt.gz!!!


        print 'parsing updates...'
        flist = open('metadata/updt_filelist'+ymd1[i], 'r')  # .bz2.txt.gz file name
        for line in flist.readlines():
            line = line.replace('\n', '')
            print line
            if not os.path.exists(line):  # .gz not exists, then .bz2 exists
                os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m '+\
                        line.replace('.txt.gz', '')+' > '+\
                        line.replace('.gz', ''))  # parse .bz2 into .txt
                # compress .txt into .gz
                os.system('gzip -c '+line.replace('.gz', '')+' > '+line)
                os.system('rm '+line.replace('.txt.gz', ''))  # remove .bz2 update files
                os.system('rm '+line.replace('.gz', ''))  # remove txt update files
            else:  # .gz exists
                pass
        flist.close()


        print 'parsing RIB and getting peers...'
        rib_location = hdname + rib_fname  # end with .bz2
        peers = []
        # get .txt
        if os.path.exists(rib_location+'.txt.gz'):  # .bz2.txt.gz file exists
            os.system('gunzip -c '+rib_location+'.txt.gz > '+\
                    rib_location+'.txt')  # unpack                        
        elif os.path.exists(rib_location):  # .bz2 file exists
            os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m\
                    '+rib_location+' > '+rib_location+'.txt')  # parse
            os.system('rm '+rib_location)  # then remove .bz2
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
        for peer in peers:  # must process each peer one by one
            peer = peer.rstrip()
            print peer
            os.system('perl tool/bgpmct.pl -rf '+rib_location+'.txt.gz'+' -ul '+\
                    'metadata/updt_filelist'+ymd1[i]+' -p '+peer+' > '+\
                    'tmp/'+peer+'_result.txt')

                
        print 'delete updates caused by session reset...'
        for peer in peers:
            # No reset for this peer
            if os.path.getsize('tmp/'+peer+'_result.txt') == 0:
                continue

            print 'culprit now: ', peer
            f_results = open('tmp/'+peer+'_result.txt', 'r')
            for line in f_results:  # get all affection info of this peer
                print line
                attr = line.split(',')
                if attr[0] == '#START':
                    continue

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
                updatefile_list = open('metadata/updt_filelist'+ymd1[i], 'r')
                for updatefile in updatefile_list.readlines():  
                    updatefile = updatefile.replace('\n', '')
                    file_attr = updatefile.split('.')
                    dt = datetime.datetime(int(file_attr[4][0:4]),\
                            int(file_attr[4][4:6]), int(file_attr[4][6:8]),\
                            int(file_attr[5][0:2]), int(file_attr[5][2:4]))

                    if not start_datetime + datetime.timedelta(minutes =\
                            -15) <= dt <= end_datetime:  # filename not OK
                        continue
                    print 'session reset exists in: ', updatefile,
                    # unpack
                    os.system('gunzip -c ' + updatefile.rstrip('\n') + ' > ' + updatefile.rstrip('.gz\n'))
                    # only .txt from now on!
                    myfilename = updatefile.rstrip('.gz\n')  # .txt file
                    oldfile = open(myfilename, 'r')
                    newfile = open('tmp/'+myfilename.split('/')[-1], 'w')
                    list_pfx = []
                    for updt in oldfile.readlines():  # loop over each update
                        updt = updt.replace('\n', '')
                        update_attr = updt.split('|')
                        if (cmp(update_attr[3], peer)==0)\
                        & (stime_unix<int(update_attr[1])<\
                        endtime_unix):  # culprit update confirmed
                            if update_attr[5] in list_pfx:
                                # pfx already counted, so not from reset
                                newfile.write(updt+'\n')
                            else:  # prefix first existence, so from reset
                                list_pfx.append(update_attr[5])
                                continue
                        else:  # not culprit update
                            newfile.write(updt+'\n')

                    oldfile.close()
                    newfile.close()

                    os.system('rm' + updatefile)  # remove old .gz file
                    # compress .txt into .gz to replace the old file
                    os.system('gzip -c tmp/'+myfilename.split('/')[-1]+\
                            ' > '+hdname_detail+ym[i]+\
                            '/UPDATES/'+myfilename.split('/')[-1]+'.gz')
                           
                updatefile_list.close()
            f_results.close()

        #os.system('rm tmp/*')
    return

if __name__ == '__main__':
    get_file()
