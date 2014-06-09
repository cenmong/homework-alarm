import os
from env import hdname
import datetime

hdname_detail = hdname + 'archive.routeviews.org/bgpdata/'

ym = ['2013.07']
ymd1 = ['20130701']
ymd2 = ['20130731']


def get_file():

    for i in range(0, len(ym)):
        ## download updatefile
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/' + ym[i] + '/UPDATES/ > tmpfile')
        f = open('tmpfile', 'r')
        if os.path.exists('metadata/updt_filelist_tmp' + ymd1[i]):
            os.system('rm '+ 'metadata/updt_filelist_tmp' + ymd1[i])
        flist = open('metadata/updt_filelist_tmp' + ymd1[i], 'a')  # filelist in the same dir as the files

        testcount = 0  # TODO: test only
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) < int(ymd1[i]) or int(line.split('.')[-3]) > int(ymd2[i]):
                continue
            updt_fname = line.split('//')[1]
            updt_fname = updt_fname.replace('\n', '')  # name of .bz2 file
            if not os.path.exists(hdname + updt_fname):
                os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
                        http://' + updt_fname)
            else:  # .bz2 file exists
                pass
            #flist.write(hdname +'/' + updt_fname + '\n')
            flist.write(hdname+updt_fname+'\n')  # store location!

            # TODO: for test only
            testcount += 1
            if testcount == 3:
                break

        flist.close()
        f.close()
        os.system('rm tmpfile')

        ## download rib
        dt = datetime.datetime.strptime(ymd1[i], "%Y%m%d").date()
        ribtime = dt + datetime.timedelta(days = -1)  # RIB is 1 day before
        ribtime = ribtime.strftime('%Y%m%d')
        dtplace = ribtime[:4] + '.' + ribtime[4:6]  # RIB may from last month
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/'+dtplace+'/RIBS/ > tmpfile')
        f = open('tmpfile', 'r')
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) == int(ribtime):
                rib_fname = line.split('//')[1]
                rib_fname = rib_fname.replace('\n', '')
                # Download files
                if not os.path.exists(hdname + rib_fname):  # .bz2 file exists
                    os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2 http://' + rib_fname)
                else:
                    pass    
                break
            else:
                pass
        f.close()
        os.system('rm tmpfile')


        ## get all peers (monitors)--->peers.txt
        print 'getting peers...'
        peers = []
        if not os.path.exists(hdname+rib_fname+'.txt'):  # rib txt file not exists
            os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m\
                    '+hdname+rib_fname+'>'+hdname+rib_fname+'.txt')
        if os.path.exists(hdname+rib_fname):  # rib .bz2 file exists
            os.system('rm '+hdname+rib_fname)

        rib_fname = rib_fname + '.txt'  # new name

        ff = open(hdname_detail+ym[i]+'/UPDATES/peers'+ymd1[i], 'w')
        with open(hdname+rib_fname, 'r') as f:
            for line in f:
                try:
                    addr = line.split('|')[3]
                    if addr not in peers:
                        peers.append(addr)
                except:
                    continue

        for p in peers:
            ff.write(p+'\n')

        ff.close()
        f.close()


        ## compress rib file for the filtering process
        print 'compress rib...'
        os.system('gzip -c '+hdname+rib_fname+' > '+hdname+rib_fname+'.gz')
        os.system('rm '+hdname+rib_fname)
        rib_location = hdname + rib_fname + '.gz'
        
        
        ## new filelist after minor filename modification 
        print 'change updatefile name...'
        os.system('tool/raw2txt.sh '+'metadata/updt_filelist_tmp'+ymd1[i])
        f = open('metadata/updt_filelist_tmp'+ymd1[i], 'r')  # .bz file name
        ff = open('metadata/updt_filelist'+ymd1[i], 'w')  # .bz2.txt.gz file name
        for line in f.readlines():
            line = line.rstrip()
            os.system('rm ' + line)
            line = line + '.txt.gz'
            ff.write(line+'\n') 
        f.close()
        ff.close()
        os.system('rm metadata/updt_filelist_tmp'+ymd1[i])

     
        ## determine router table transfers start and end time
        print 'determine router table transfers start and end time...'
        f = open(hdname_detail+ym[i]+'/UPDATES/peers'+ymd1[i],'r')
        for peer in f:  # process each peer separately
            print peer,
            peer = peer.rstrip()
            os.system('perl tool/bgpmct.pl -rf '+rib_location+' -ul '+\
                    'metadata/updt_filelist'+ymd1[i]+' -p '+peer+' > '+\
                    hdname_detail+ym[i]+'/'+peer+'_result.txt')  # ym[i]?
        f.close()


        ## check which peer transfered table
        print 'check which peer transfered table...'
        peer_result_list = os.listdir(hdname_detail+ym[i])  
        for f in peer_result_list:
            if not os.path.isfile(hdname_detail + ym[i] + '/' + f): 
                continue
            if os.path.getsize(hdname_detail + ym[i] + '/' + f) == 0:  # no
                    #reset for this peer, then remove file
                os.system('rm ' + hdname_detail +ym[i] + '/' + f)

                
        ## delete updates caused by session reset
        print 'delete updates caused by session reset...'
        for affected_p in peer_result_list:
            affected_p = affected_p.rstrip('')
            if not os.path.isfile(hdname_detail+ym[i]+'/'+affected_p):
                continue

            f = open(hdname_detail+ym[i]+'/'+affected_p)
            for line in f:  # loop over affected peers
                print line
                attr = line.split(',')
                if attr[0] == '#START':
                    continue

                # get session reset time
                print 'get session reset time...'
                stime_unix = attr[0]
                endtime_unix = attr[1]
                stime_date = datetime.datetime.fromtimestamp(int(stime_unix))
                stime_date = stime_date + datetime.timedelta(hours=-8)
                endtime_date = datetime.datetime.fromtimestamp(int(endtime_unix))
                endtime_date = endtime_date + datetime.timedelta(hours=-8)
                stime_date = stime_date.strftime('%Y%m%d.%H%M')
                start_datetime = datetime.datetime(int(stime_date[0:4]), int(stime_date[4:6]), int(stime_date[6:8]), int(stime_date[9:11]), int(stime_date[11:13]))                               
                endtime_date = endtime_date.strftime('%Y%m%d.%H%M')
                end_datetime = datetime.datetime(int(endtime_date[0:4]), int(endtime_date[4:6]), int(endtime_date[6:8]), int(endtime_date[9:11]), int(endtime_date[11:13]))                               
                print stime_date
                print endtime_date

                # now let's clean updates
                updatefile_list = open('metadata/updt_filelist'+ymd1[i], 'r')
                for updatefile in updatefile_list.readlines():  
                    updt_attr = updatefile.split('.')
                    dt = datetime.datetime(int(updt_attr[4][0:4]),int(updt_attr[4][4:6]),int(updt_attr[4][6:8]),int(updt_attr[5][0:2]),int(updt_attr[5][2:4]))

                    if not start_datetime + datetime.timedelta(minutes =\
                            -15) <= dt <= end_datetime:  # filename not OK
                        continue
                    print updatefile,
                    # unpack
                    os.system('gunzip -c ' + updatefile.rstrip('\n') + ' > ' + updatefile.rstrip('.gz\n'))
                    myfilename = updatefile.rstrip('.gz\n')
                    myfile = open(myfilename,'r')
                    filelines = myfile.readlines()
                    newfile = open(hdname_detail+ym[i]+'/RIBS/'+myfilename.split('/')[-1], 'w')
                    list_pre = []
                    list_new_update = []
                    for updt in filelines:  # loop over each update
                        update_attr = updt.split('|')
                        if (cmp(update_attr[3],f.rstrip('.txt'))==0)\
                        & (int(stime_unix) < int(update_attr[1]) < int\
                        (endtime_unix)):  # culprit update confirmed
                            if update_attr[5] in list_pre:
                                list_new_update.append(updt)  # pfx
                                    # already counted
                            else:  # prefix first existence
                                list_pre.append(update_attr[5])
                                continue
                        else:  # normal update
                            list_new_update.append(updt)

                    if os.path.exists(updatefile):
                        os.system('rm' + updatefile)  # remove old
                        num = 0
                        while 1:
                            try:  # create new
                                newfile.write(list_new_update[num])
                                num = num +1
                            except:
                                break
                    else:
                        num = 0
                        while 1:
                            try:
                                newfile.write(list_new_update[num])
                                num = num +1
                            except:
                                break
                    newfile.close()
                    myfile.close()
                    os.system('gzip -c ' + hdname_detail + ym[i] +
                            '/RIBS/' + myfilename.split('/')[-1]+
                            '>' + hdname_detail +ym[i] +
                            '/UPDATES/'+ myfilename.split('/')[-1]+'.gz')
                    os.system('rm ' + hdname_detail + ym[i]
                            +'/RIBS/' + myfilename.split('/')[-1])
                           
                updatefile_list.close()
            f.close()

        for f in peer_result_list:  # remove every peer's reset info 
            if os.path.isfile(hdname_detail + ym[i] + '/' + f):
                os.system('rm ' + hdname_detail + ym[i] + '/' + f)
    return

if __name__ == '__main__':
    get_file()
