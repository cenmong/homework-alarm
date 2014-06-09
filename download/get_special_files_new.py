import os
from env import hdname
import datetime

hdname_detail = hdname + 'archive.routeviews.org/bgpdata/'

'''
0:ref; 1:slammer; 2:coast BO; 3:Katrina; 4:LA BO; 5:TW cabel; 6:Mid cable 1;
7:Mid cable 2; 8:JP tsunami; 9:Spamhaus; 10:AU route leak; 11: Canada route
leak; 12:SC earthquake; 13:Chile earthquake; 14:Sandy; 15:Northeast US BO;
16:sea-me cable 17:TL BO
'''

ym = ['2013.07']
ymd1 = ['20130701']
ymd2 = ['20130731']

def get_file():

    for i in range(0, len(ym)):
        ## download updatefile
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/' + ym[i] + '/UPDATES/ > tmpfile')
        f = open('tmpfile', 'r')
        if os.path.exists(hdname_detail + ym[i] + '/UPDATES/update' + ymd1[i]):
            os.system('rm '+ hdname_detail + ym[i] + '/UPDATES/update' + ymd1[i])
        flist = open(hdname_detail + ym[i] + '/UPDATES/update' + ymd1[i], 'a')  # filelist in the same dir as the files

        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) < int(ymd1[i]) or int(line.split('.')[-3]) > int(ymd2[i]):
                continue
            filename = line.split('//')[1]
            filename = filename.replace('\n', '')  # name of .bz2 file
            if not os.path.exists(hdname + filename):
                os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
                        http://' + filename)
            else:  # .bz2 file exists
                pass
            flist.write(hdname +'/' + filename + '\n')
        flist.close()
        f.close()
        os.system('rm tmpfile')

        ## download rib
        dt = datetime.datetime.strptime(ymd1[i], "%Y%m%d").date()
        ribtime = dt + datetime.timedelta(days = -1)  # RIB is 1 day before
        ribtime = ribtime.strftime('%Y%m%d')
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/' + ym[i] +'/RIBS/ > tmpribfile')
        f = open('tmpribfile', 'r')
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) == int(ribtime):
                filename = line.split('//')[1]
                filename = filename.replace('\n', '')
                # Download files
                if not os.path.exists(hdname + filename):  # .bz2 file exists
                    os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2 http://' + filename)
                else:
                    pass    
                break
            else:
                pass
        f.close()
        os.system('rm tmpribfile')


        ## get all peers (monitors)--->peers.txt
        peer=[]
        os.system('./bgpdump -m '+hdname+filename+'>'+hdname+filename+'.txt')
        f = open(hdname+filename+'.txt', 'r')  # open rib file
        ff = open(hdname_detail+ym[i]+'/UPDATES/peers'+ymd1[i], 'w')

        for line in f:
            try:
                addr = line.split('|')[3]
                socket.inet_aton(addr)
                peer.append(addr)
            except:
                'get peer fail'
                continue

        peer= list(set(peer))
        for i in range(0, len(peer)):
            ff.write(peer[i]+'\n')

        ff.close()
        f.close()


        ## compress rib file for the filtering process
        os.system('gzip -c '+ hdname + filename + '.txt > ' + hdname + filename + '.txt.gz')
        ribfilename = hdname + filename + '.txt.gz'
        
        
        ## new filelist after minor filename modification 
        os.system('./raw2txt.sh '+hdname_detail+ym[i]+'/UPDATES/update'+ymd1[i])
        f = open(hdname_detail+ym[i]+'/UPDATES/update'+ymd1[i], 'r')
        ff = open(hdname_detail+ym[i]+'/UPDATES/updates'+ymd1[i], 'w')
        for k in f:
            k = k.rstrip() + '.txt.gz'
	        ff.write(k+'\n') 
        f.close()
        ff.close()
     

        ## determine router table transfers start and end time
        f4 = open(hdname_detail + ym[i] + '/UPDATES/peers' + ymd1[i],'r')
        for peer in f4:  # process each peer separately
            print peer,
            peer = peer.rstrip()
            os.system('perl bgpmct.pl -rf '+ribfilename+' -ul '+hdname_detail+\
                    ym[i] + '/UPDATES/updates'+ymd1[i]+' -p '+peer+' > '+\
                    hdname_detail+ym[i]+'/'+peer+'.txt')  # ym[i]?
        f4.close()


        ## check which peer transfered table
        filelist = os.listdir(hdname_detail+ym[i])  
        for f in filelist:
            if os.path.isfile(hdname_detail + ym[i] + '/' + f): 
                if os.path.getsize(hdname_detail + ym[i] + '/' + f) == 0:  # no
                        #reset for this peer, then remove file
                    os.system('rm ' + hdname_detail +ym[i] + '/' + f)

        ## delete updates caused by session reset
        for myline in filelist:
            myline = myline.rstrip('')
            if !os.path.isfile(hdname_detail+ym[i]+'/'+myline):
                continue

            f = open(hdname_detail + ym[i] + '/' + myline)
            for line in f:  # loop over affected peers
                attr = line.split(',')
                if attr[0] == '#START':
                    continue

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

                updatefilelist = open(hdname_detail+ym[i]+'/UPDATES/updates'+ymd1[i], 'r')
                for updatefile in updatefilelist:  # now let's clean updates
                    updt_attr = updatefile.split('.')
                    dt = datetime.datetime(int(updt_attr[4][0:4]),int(updt_attr[4][4:6]),int(updt_attr[4][6:8]),int(updt_attr[5][0:2]),int(updt_attr[5][2:4]))

                    if not start_datetime + datetime.timedelta(minutes =\
                            -15) <= dt <= end_datetime:  # filename OK
                        continue
                    print updatefile,
                    os.system('gunzip -c ' + updatefile.rstrip('\n') + ' > ' + updatefile.rstrip('.gz\n'))
                    myfilename = updatefile.rstrip('.gz\n')
                    myfile = open(myfilename,'r')
                    filelines = myfile.readlines()
                    newfile = open(hdname_detail+ym[i]+'/RIBS/'+myfilename.split('/')[-1], 'w')
                    list_pre = []
                    list_new_update = []
                    for updt in filelines:
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
                           
                updatefilelist.close()
            f.close()

        for f in filelist:  # remove every peer's reset info 
            if os.path.isfile(hdname_detail + ym[i] + '/' + f):
                os.system('rm ' + hdname_detail + ym[i] + '/' + f)
    return

if __name__ == '__main__':
    get_file()
