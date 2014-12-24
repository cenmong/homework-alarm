import cmlib
import subprocess
import os
import patricia
import urllib

from env import *

class Supporter():

    def __init__(self, sdate):
        self.sdate = sdate
        cmlib.make_dir(datadir+'support/')

    def get_as2cc_file(self): # AS to customer cone
        location = datadir + 'support/' + self.sdate + '/'
        cmlib.make_dir(location)

        tmp = os.listdir(datadir+'support/'+self.sdate+'/')
        for line in tmp:
            if 'ppdc' in line:
                return 0 # we already have a prefix2as file

        print 'Downloading AS to customer cone file ...'
        webloc = 'http://data.caida.org/datasets/2013-asrank-data-supplement/data/'
        webraw = cmlib.get_weblist(webloc)
        target_line = ''
        yearmonth = self.sdate[:6] # YYYYMM
        for line in webraw.split('\n'):
            if yearmonth in line and 'ppdc' in line:
                target_line = line
                break

        if target_line == '':
            print 'Downloading AS to customer cone file fails: no such month!'
            return -1

        fname = target_line.split()[0]
        urllib.urlretrieve(webloc+fname, location+fname)
        if int(yearmonth) <= 201311:
            # unpack .gz (only before 201311 (include))
            subprocess.call('gunzip '+location+fname, shell=True)
        else:
            # unpack .bz2 (only after 201406 (include))
            subprocess.call('bunzip2 -d '+location+fname, shell=True)

        # Now we have file xxxxyyzz.ppdc-ases.txt

        return 0

    # XXX should we get this from the last hop of AS path?
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

    def get_pfx2as_trie(self):
        print 'Calculating prefix to AS number trie...'
        pfx2as = patricia.trie(None)

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
                pfx = cmlib.ip_to_binary(attr[0]+'/'+attr[1], '0.0.0.0')
                try:
                    pfx2as[pfx] = int(attr[2]) # pfx: origin AS
                except: # When will this happen?
                    pfx2as[pfx] = -1

            f.close()
        else:
            # Extract info from RIB of the monitor route-views2
            mydate = self.sdate[0:4] + '.' + self.sdate[4:6]
            rib_location = datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/'
            dir_list = os.listdir(datadir+'routeviews.org/bgpdata/'+mydate+'/RIBS/')


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
                        pfx = cmlib.ip_to_binary(tmp, '0.0.0.0')
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

    def get_as2nation_dict(self):
        print 'Calculating AS to nation dict...'
        as2nation = {}

        f = open(datadir+'support/as2nation.txt')
        for line in f:
            as2nation[int(line.split()[0])] = line.split()[1]
        f.close()

        return as2nation

    def get_as2type_dict(self):
        as2type = {}

        # TODO: consider datetime
        f = open(datadir+'support/as2attr.txt')
        for line in f:
            line = line.strip('\n')
            as2type[int(line.split()[0])] = line.split()[-1]
        f.close()

        return as2type

    def get_as2cc_dict(self): # AS to customer cone
        self.get_as2cc_file()
        print 'Calculating AS to customer cone dict...'

        as2cc_file = ''
        tmp = os.listdir(datadir+'support/'+self.sdate+'/')
        for line in tmp:
            if 'ppdc' in line:
                as2cc_file = line
                break

        as2cc = {}
        f = open(datadir+'support/'+self.sdate+'/'+as2cc_file)
        for line in f:
            if line == '' or line == '\n':
                continue
            line = line.rstrip('\n')
            attr = line.split()
            if attr[0] == '#':
                continue
            as2cc[int(attr[0])] = len(attr) - 1 

        f.close()

        return as2cc

    def get_nation2cont_dict(self):
        nation2cont == {}

        f = open(datadir+'support/continents.txt')
        for line in f:
            line = line.strip('\n')
            nation2cont[line.split(',')[0]] = line.split(',')[1]
        f.close()

        return nation2cont
