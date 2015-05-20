import radix # takes 1/4 the time as patricia
import datetime
import numpy as np
import calendar # do not use the time module
import cmlib
import operator
import string
import gzip
import traceback
import logging
import subprocess
import ast
import calendar
import os
import urllib

from netaddr import *
from env import *
from cStringIO import StringIO

class Micro_fighter():

    def __init__(self, reaper):
        self.reaper = reaper
        self.sdate = self.reaper.period.sdate
        self.granu = self.reaper.granu
        self.period = reaper.period
        self.sdt_obj = None
        self.edt_obj = None

        self.filelist = self.period.get_filelist()

        self.mfilegroups = None

        self.middle_dir = self.period.get_middle_dir()
        self.final_dir = self.period.get_final_dir()


    def all_events_cluster(self):
        pfx_set_dict = dict()

        event_dict = self.get_events_list()
        for unix_dt in event_dict:

            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()

            pfx_set_dict[unix_dt] = pfx_set

        # a simple clustering way


    def all_events_tpattern(self): # time patterns of all events
        event_dict = self.get_events_list()
        for unix_dt in event_dict:
            event_size = event_dict[unix_dt][1]
            event_den = event_dict[unix_dt][2]

            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()


            slot_num = 3 # number of time slots before and after the event
            #-----------------------------------
            # determine and read the middle files
            head = unix_dt - self.reaper.granu * 60 * slot_num 
            tail = unix_dt + self.reaper.granu * 60 * slot_num
    
            if head < int(self.reaper.filegroups[0][0].rstrip('.txt.gz')) or\
                    tail > int(self.reaper.filegroups[-1][0].rstrip('.txt.gz')):
                print 'cannot analyze the time pattern of the event'
                continue

            # file group list ordered by datetime
            unix_set = set()
            for i in xrange(0, 2*slot_num+1):
                unix_set.add(head + self.reaper.granu*60*i)

            fg_list = list() # it is supposed to be ordered
            for fg in self.reaper.filegroups:
                if int(fg[0].rstrip('.txt.gz')) in unix_set:
                    fg_list.append(fg)

            # -----------------------------------------
            # obtain density list
            den_list = list() # density list ordered by time
            for fg in fg_list:
                if int(fg[0].rstrip('.txt.gz')) == unix_dt: # current event datetime
                    den_list.append(event_den)
                    continue

                pfx_int_data = dict()
                for fname in fg:
                    floc = self.reaper.middle_dir + fname
                    print 'Reading ', floc
                    p = subprocess.Popen(['zcat', floc],stdout=subprocess.PIPE)
                    fin = StringIO(p.communicate()[0])
                    assert p.returncode == 0
                    for line in fin:
                        line = line.rstrip('\n')
                        if line == '':
                            continue

                        pfx = line.split(':')[0]
                        datalist = ast.literal_eval(line.split(':')[1])

                        try:
                            c_list = pfx_int_data[pfx]
                            combined = [x+y for x,y in zip(datalist, c_list)]
                            pfx_int_data[pfx] = combined
                        except:
                            pfx_int_data[pfx] = datalist


                ones_in_num = 0
                for pfx in pfx_set:
                    for index in mon_set:
                        try:
                            if pfx_int_data[pfx][index] > 0:
                                ones_in_num += 1
                        except: # no such pfx or mon
                            pass

                cden = float(ones_in_num) / float(event_size)
                den_list.append(cden)

            print den_list

            #---------------------------
            #TODO: output to a file


    def all_events_ratios(self):

        event_dict = self.get_events_list()

        for unix_dt in event_dict:
            rel_size = event_dict[unix_dt][0] # relative size
            width = event_dict[unix_dt][4]
            size = event_dict[unix_dt][1]
            height = event_dict[unix_dt][3] # or prefix number


            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()


            #-----------------------------------
            # read the middle files
            target_fg = None
            for fg in self.reaper.filegroups:
                if int(fg[0].rstrip('.txt.gz')) == unix_dt:
                    target_fg = fg
                    break

            pfx_int_data = dict()
            for fname in target_fg:
                floc = self.reaper.middle_dir + fname
                print 'Reading ', floc
                p = subprocess.Popen(['zcat', floc],stdout=subprocess.PIPE)
                fin = StringIO(p.communicate()[0])
                assert p.returncode == 0
                for line in fin:
                    line = line.rstrip('\n')
                    if line == '':
                        continue

                    pfx = line.split(':')[0]
                    datalist = ast.literal_eval(line.split(':')[1])

                    try:
                        c_list = pfx_int_data[pfx]
                        combined = [x+y for x,y in zip(datalist, c_list)]
                        pfx_int_data[pfx] = combined
                    except:
                        pfx_int_data[pfx] = datalist

            #------------------------------------------
            # get the number of updates and 1s in and out of the event
            udt_num = 0
            for pfx in pfx_int_data:
                the_sum = sum(pfx_int_data[pfx])
                udt_num += the_sum

            udt_in_num = 0
            for pfx in pfx_set:
                for mon_index in mon_set:
                    udt_in_num += pfx_int_data[pfx][mon_index]

            udt_out_num = udt_num - udt_in_num
            print udt_num
            print udt_in_num

            ones_num = 0
            for pfx in pfx_int_data:
                datalist = pfx_int_data[pfx]
                for data in datalist:
                    if data > 0:
                        ones_num += 1

            ones_in_num = 0
            for pfx in pfx_set:
                for mon_index in mon_set:
                    if pfx_int_data[pfx][mon_index] > 0:
                        ones_in_num += 1

            ones_out_num = ones_num - ones_in_num
            print ones_num
            print ones_in_num

            #--------------------------------------------
            # analyze prefixes
            all_pfx_num = self.reaper.period.get_fib_size()
            prefix_ratio = float(height) / float(all_pfx_num)
            print prefix_ratio

            #-------------------------------------------
            # distribution of origin ASes TODO: move to somewhere else
            all_AS_num = self.reaper.period.get_AS_num()

            pfx2as = self.get_pfx2as()
            asn_dict = dict()
            for pfx in pfx_set:
                try:
                    asn = pfx2as[pfx]
                except:
                    asn = -1
                try:
                    asn_dict[asn] += 1
                except:
                    asn_dict[asn] = 1

            #for asn in asn_dict:
            #    asn_dict[asn] = float(asn_dict[asn]) / float(all_AS_num)
            print asn_dict

            #-----------------------------------------
            # TODO: write result to files


    def get_events_list(self):
        event_dict = dict()

        path = self.reaper.get_output_dir_event() + self.reaper.events_brief_fname
        f = open(path, 'r')
        for line in f:
            line = line.rstrip('\n')
            unix_dt = int(line.split(':')[0])
            content = line.split(':')[1]
            thelist = ast.literal_eval(content)
            event_dict[unix_dt] = thelist
        f.close()

        return event_dict


    def set_sedate(self, sdt_obj, edt_obj):
        self.sdt_obj = sdt_obj
        self.edt_obj = edt_obj

    def get_mfile_group_trange(self):
        self.mfilegroups = list() # list of middle file groups

        mfiles = os.listdir(self.middle_dir)
        for f in mfiles:
            if not f.endswith('.gz'):
                mfiles.remove(f)
        mfiles.sort(key=lambda x:int(x.rstrip('.txt.gz')))

        #----------------------------------------------------------------------
        # group middle files according to the desired granularity

        # get granularity of middle files
        self.m_granu = (int(mfiles[1].rstrip('.txt.gz')) - int(mfiles[0].rstrip('.txt.gz'))) / 60
        shift = self.reaper.shift
        shift_file_c = shift / self.m_granu
        mfiles = mfiles[shift_file_c:] # shift the interval

        group_size = self.granu / self.m_granu
        group = []
        for f in mfiles:
            group.append(f)
            if len(group) is group_size:
                self.mfilegroups.append(group)
                group = []

        #--------------------------------------------------------
        # delete the files that is not in our concern range
        group_to_delete = []
        for fg in self.mfilegroups:
            unix_dt = int(fg[0].rstrip('.txt.gz')) # timestamp of current file group
            dt_obj = datetime.datetime.utcfromtimestamp(unix_dt)
            if dt_obj < self.sdt_obj or dt_obj > self.edt_obj:
                group_to_delete.append(fg)

        for gd in group_to_delete:
            self.mfilegroups.remove(gd)


    def bgp_leak_pfx(self):
        pfx_set = set()
        fname = datadir + 'final_output/bell-leak.txt'
        f = open(fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            pfx = line.split('=')[1]
            pfx_set.add(pfx)
        f.close()

        return pfx_set

    def get_pfx2as_file(self):
        location = datadir + 'support/' + self.sdate + '/'
        cmlib.make_dir(location)

        tmp = os.listdir(datadir+'support/'+self.sdate+'/')
        for line in tmp:
            if 'pfx2as' in line:
                return 0 # we already have a prefix2as file

        print 'Downloading prefix to AS file ...'
        year, month = self.sdate[:4], self.sdate[4:6] # YYYY, MM
        webloc = 'http://data.caida.org/datasets/routing/routeviews-prefix2as' +\
                '/' + year + '/' + month + '/'

        webraw = cmlib.get_weblist(webloc)
        target_line = ''
        for line in webraw.split('\n'):
            if self.sdate in line:
                target_line = line
                break

        if target_line == '':
            print 'Downloading prefix to AS file fails: no such date!'
            return 0

        fname = target_line.split()[0]
        urllib.urlretrieve(webloc+fname, location+fname)
        subprocess.call('gunzip -c '+location+fname+' > '+\
                location+fname.replace('.gz', ''), shell=True)
        os.remove(location+fname)

        return 0

    def get_pfx2as(self):
        self.get_pfx2as_file()

        print 'Calculating prefix to AS number trie...'
        pfx2as = dict()

        if int(self.sdate) >= 20050509:
            self.get_pfx2as_file()

            pfx2as_file = ''
            tmp = os.listdir(datadir+'support/'+self.sdate+'/')
            for line in tmp:
                if 'pfx2as' in line:
                    pfx2as_file = line
                    break

            f = open(datadir+'support/'+self.sdate+'/'+pfx2as_file)
            for line in f:
                line = line.rstrip('\n')
                attr = line.split()
                if '_' in attr[2] or ',' in attr[2]:
                    continue
                pfx = attr[0]+'/'+attr[1]
                try:
                    pfx2as[pfx] = int(attr[2]) # pfx: origin AS
                except: # When will this happen?
                    pfx2as[pfx] = -1

            f.close()
        else:
            # Extract info from RIB of the monitor route-views2
            mydate = self.sdate[0:4] + '.' + self.sdate[4:6]
            rib_location = datadir+'archive.routeviews.org/bgpdata/'+mydate+'/RIBS/'
            dir_list = os.listdir(datadir+'archive.routeviews.org/bgpdata/'+mydate+'/RIBS/')


            for f in dir_list:
                if not f.startswith('.'):
                    rib_location = rib_location + f # if RIB is of the same month. That's OK.
                    break
            
            if rib_location.endswith('txt.gz'):
                subprocess.call('gunzip '+rib_location, shell=True)  # unpack                        
                rib_location = rib_location.replace('.txt.gz', '.txt')
            elif not rib_location.endswith('txt'):  # .bz2/.gz file exists
                cmlib.parse_mrt(rib_location, rib_location+'.txt')
                os.remove(rib_location)  # then remove .bz2/.gz
                rib_location = rib_location + '.txt'
            # now rib file definitely ends with .txt, let's rock and roll
            with open(rib_location, 'r') as f:
                for line in f:
                    try:
                        tmp = line.split('|')[5]
                        pfx = tmp
                        ASlist = line.split('|')[6]
                        originAS = ASlist.split()[-1]
                        try:
                            pfx2as[pfx] = int(originAS)
                        except:
                            pfx2as[pfx] = -1
                    except:
                        pass

            f.close()
            # compress RIB into .gz
            if not os.path.exists(rib_location+'.gz'):
                cmlib.pack_gz(rib_location)

        return pfx2as

    def analyze_pfx_indate(self, ASes, sdt_obj, edt_obj):
        fmy = open(self.filelist, 'r')
        sdt_unix = calendar.timegm(sdt_obj.utctimetuple())
        edt_unix = calendar.timegm(edt_obj.utctimetuple())
        print sdt_unix, edt_unix

        #pfx_set = set() # 2156 prefixes in total

        target_mon = (['195.66.224.138', '89.149.178.10'])
        target_pfx = set()
        f = open('target_pfx.txt','r')
        for line in f:
            line = line.rstrip('\n')
            target_pfx.add(line)
        f.close()

        #WW:0,AAdu1:1,AAdu2:2,AAdiff:3,WA:4,AW:5
        target_dict = dict() # mon: prefix: successive update type series (0~5)
        target_record = dict() # mon: prefix: latest full update
        for m in target_mon:
            target_dict[m] = dict()
            target_record[m] = dict()

        #fpath_list = cmlib.get_file_list_indate(self.filelist, sdt_obj, edt_obj)

        for fline in fmy:
            # get date from file name
            updatefile = fline.split('|')[0]

            file_attr = updatefile.split('.')
            fattr_date, fattr_time = file_attr[-5], file_attr[-4]
            fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))
            
            fline = datadir + fline.split('|')[0]

            # get current file's collector name
            attributes = fline.split('/') 
            j = -1
            for a in attributes:
                j += 1
                if a.startswith('data.ris') or a.startswith('archi'):
                    break

            co = fline.split('/')[j + 1]
            if co == 'bgpdata':  # route-views2, the special case
                co = ''


            # Deal with several special time zone problems
            if co == 'route-views.eqix' and fname_dt_obj <= dt_anchor2: # PST time
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
            elif not co.startswith('rrc') and fname_dt_obj <= dt_anchor1:
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

            if co.startswith('rrc'):
                shift = -10
            else:
                shift = -30


            # Check whether the file is a possible target
            if not sdt_obj+datetime.timedelta(minutes=shift)<=fname_dt_obj<=edt_obj:
                continue

            # read the update file
            print 'Reading ', fline
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    line = line.rstrip('\n')
                    attr = line.split('|')
                    pfx = attr[5]
                    type = attr[2]
                    mon = attr[3]

                    #if type == 'W':
                    #    continue
                    if mon not in target_mon:
                        continue
                        
                    if pfx not in target_pfx:
                        continue

                    if type == 'A':
                        as_path = attr[6]
                        #as_list = as_path.split()
                        #mylen = len(as_list)
                        #for i in xrange(0, mylen-1):
                        #    as1 = int(as_list[i])
                        #    as2 = int(as_list[i+1])

                        #    if as1 == as2:
                        #        continue
                        #    
                        #    if as1 in ASes and as2 in ASes and i == mylen-2: # last hop
                        #        analyze = True
                        #        break

                    #pfx_set.add(pfx)
                    try:
                        test = target_dict[mon][pfx]
                    except:
                        target_dict[mon][pfx] = list() # list of 0~5

                    try:
                        last_update = target_record[mon][pfx]
                        last_attr = last_update.split('|')
                        last_type = last_attr[2]
                        if last_type is 'A':
                            last_as_path = last_attr[6]
                    except:
                        last_type = 'W'
                        last_as_path = 'Nothing'

                    if last_type is 'W':
                        if type is 'W':
                            print 'WW'
                            target_dict[mon][pfx].append(0)
                        elif type is 'A':
                            print 'WA'
                            target_dict[mon][pfx].append(4)
                            target_record[mon][pfx] = line
                
                    elif last_type is 'A':
                        if type is 'W':
                            print 'AW'
                            target_dict[mon][pfx].append(5)
                            target_record[mon][pfx] = 'Nothing'
                        elif type is 'A':
                            if line == last_update:
                                print 'AAdu1'
                                target_dict[mon][pfx].append(1)
                            elif as_path == last_as_path:
                                print 'AAdu2'
                                target_dict[mon][pfx].append(2)
                                target_record[mon][pfx] = line
                            else:
                                print 'AAdiff'
                                target_dict[mon][pfx].append(3)
                                target_record[mon][pfx] = line
                
                    else: # abnormal
                        continue
                        
                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)
                print len(target_dict['195.66.224.138'])
            myf.close()
            '''
            f3 = open('target_pfx.txt', 'w')
            for pfx in pfx_set:
                f3.write(pfx+'\n')
            f3.close()
            '''
        for mon in target_dict:
            print len(target_dict[mon])
            ff = open(mon+'result.txt','w')
            for pfx in target_dict[mon]:
                ff.write(pfx+':'+str(target_dict[mon][pfx])+'\n')
            ff.close()

        f.close()

    def event_analyze_pfx(self, unix_dt):

        pfx2as = self.get_pfx2as()
        as2count = dict()
        odd_pfx = set()

        event_detail_fname = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_detail_fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            if '#' in line: # monitor line
                pass
            else:
                pfx = line.split(':')[0]
                try:
                    asn = pfx2as[pfx]
                except:
                    asn = -1
                    odd_pfx.add(pfx)
                try:
                    as2count[asn] += 1
                except:
                    as2count[asn] = 1
        f.close()

        tmp_list = sorted(as2count.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f2 = open('as_result.txt','w')
        for item in tmp_list:
            asn = item[0]
            count = item[1]
            f2.write(str(asn)+':'+str(count)+'\n')
        f2.close()

        f = open('odd_pfx.txt', 'w')
        for pfx in odd_pfx:
            f.write(pfx+'\n')
        f.close()
        

    def event_as_link_rank(self, unix_dt):
        as_link_count = dict()
        as_count = dict()

        event_sdt = datetime.datetime.utcfromtimestamp(unix_dt)
        event_unix_sdt = unix_dt
        event_edt = event_sdt + datetime.timedelta(minutes=self.granu)
        event_unix_edt = unix_dt + self.granu * 60

        # get event prefix and monitor set
        pfx_set = set()
        mon_index_set = set()
        event_detail_fname = self.reaper.get_output_dir_event() + str(unix_dt) + '.txt'
        f = open(event_detail_fname, 'r')
        for line in f:
            line = line.rstrip('\n')
            if '#' in line: # monitor line
                mondict = line.split('#')[1]
                mondict = ast.literal_eval(mondict)
                mon_index_set = set(mondict.keys())
            else:
                pfx = line.split(':')[0]
                pfx_set.add(pfx)
        f.close()

        pfx_count = len(pfx_set)
        mon_count = len(mon_index_set)
        event_size = pfx_count * mon_count

        #leak_pfx_set = self.bgp_leak_pfx()
        #common_set = pfx_set & leak_pfx_set
        #common_count = len(common_set)
        #print 'pfx set in event:',pfx_count
        #print 'pfx set in leak',len(leak_pfx_set)
        #print 'common pfx set in leak:',common_count
            
        index2mon = dict()
        mon2index_file = self.period.get_mon2index_file_path()
        f = open(mon2index_file, 'r')
        for line in f:
            line = line.rstrip('\n')
            ip = line.split(':')[0]
            index = int(line.split(':')[1])
            index2mon[index] = ip
        f.close()

        mon_set = set()
        for i in mon_index_set:
            mon_set.add(index2mon[i])

         
        # pfx=>xxxxxx, mon=>xxx, pfx+mon=>xxxxxxxxx, to save memory
        pfx2tag = dict()
        mon2tag = dict()

        start = 100000
        for pfx in pfx_set:
            pfx2tag[pfx] = str(start)
            start += 1

        start = 100
        for mon in mon_set:
            mon2tag[mon] = str(start)
            start += 1


        # obtain the target update file list
        f = open(self.filelist, 'r')
        for fline in f:
            # get date from file name
            updatefile = fline.split('|')[0]

            file_attr = updatefile.split('.')
            fattr_date, fattr_time = file_attr[-5], file_attr[-4]
            fname_dt_obj = datetime.datetime(int(fattr_date[0:4]),\
                    int(fattr_date[4:6]), int(fattr_date[6:8]),\
                    int(fattr_time[0:2]), int(fattr_time[2:4]))
            

            fline = datadir + fline.split('|')[0]


            # get current file's collector name
            attributes = fline.split('/') 
            j = -1
            for a in attributes:
                j += 1
                if a.startswith('data.ris') or a.startswith('archi'):
                    break

            co = fline.split('/')[j + 1]
            if co == 'bgpdata':  # route-views2, the special case
                co = ''


            # Deal with several special time zone problems
            if co == 'route-views.eqix' and fname_dt_obj <= dt_anchor2: # PST time
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=7) # XXX (not 8)
            elif not co.startswith('rrc') and fname_dt_obj <= dt_anchor1:
                fname_dt_obj = fname_dt_obj + datetime.timedelta(hours=8) # XXX here is 8

            if co.startswith('rrc'):
                shift = -10
            else:
                shift = -30


            # Check whether the file is a possible target
            if not event_sdt+datetime.timedelta(minutes=shift)<=fname_dt_obj<=event_edt:
                continue


            # read the update file
            print 'Reading ', fline
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE, close_fds=True)
            myf = StringIO(p.communicate()[0])
            assert p.returncode == 0
            for line in myf:
                try:
                    attr = line.rstrip('\n').split('|')
                    pfx = attr[5]
                    mon = attr[3]

                    if not event_unix_sdt<=int(attr[1])<=event_unix_edt:
                        continue

                    if (pfx not in pfx_set) or (mon not in mon_set):
                        continue

                    # now do something
                    the_tag = pfx2tag[pfx] + mon2tag[mon]

                    as_list = attr[6].split()
                    mylen = len(as_list)
                    for i in xrange(0, mylen-1):
                        as1 = as_list[i]
                        as2 = as_list[i+1]

                        if as1 == as2:
                            continue

                        if int(as1) > int(as2):
                            as_link = as2+'_'+as1
                        else:
                            as_link = as1+'_'+as2

                        try:
                            as_link_count[as_link].add(the_tag)
                        except:
                            as_link_count[as_link] = set()
                            as_link_count[as_link].add(the_tag)

                        try:
                            as_count[as1].add(the_tag)
                        except:
                            as_count[as1] = set()
                            as_count[as1].add(the_tag)

                    try:
                        as_count[as2].add(the_tag)
                    except:
                        as_count[as2] = set()
                        as_count[as2].add(the_tag)


                except Exception, err:
                    if line != '':
                        logging.info(traceback.format_exc())
                        logging.info(line)

            myf.close()

        f.close()

        tmp_dict = dict()
        for al in as_link_count:
            tmp_dict[al] = float(len(as_link_count[al])) / event_size

        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+'as_link_rank_'+str(unix_dt)+'.txt', 'w')
        for item in tmp_list:
            as_link = item[0]
            count = item[1]
            f.write(str(as_link)+':'+str(count)+'\n')
        f.close()


        tmp_dict = dict()
        for a in as_count:
            tmp_dict[a] = float(len(as_count[a])) / event_size
        tmp_list = sorted(tmp_dict.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        f = open(self.reaper.get_output_dir_event()+'as_rank_'+str(unix_dt)+'.txt', 'w')
        for item in tmp_list:
            asn = item[0]
            count = item[1]
            f.write(str(asn)+':'+str(count)+'\n')
        f.close()

        # read the unix_dt.txt file to get prefix and monitor set (a separate function)
        # read each update file to identify the interested pfx and mon
        # record the data we care
