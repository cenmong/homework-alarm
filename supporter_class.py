import cmlib
import subprocess
import os
import patricia

from env import *

class Supporter():

    def __init__(self, sdate):
        self.sdate = sdate
        cmlib.make_dir(hdname+'support/')

    def get_as2nation_file(self):
        if os.path.exists(hdname+'support/as2nation.txt'):
            return 0

        # get the latest AS to nation mapping
        # this mapping seems stable across years
        rows = cmlib.get_weblist('http://bgp.potaroo.net/cidr/autnums.html')
        f = open(hdname+'support/as2nation.txt', 'w')
        for line in rows.split('\n'):
            if 'AS' not in line:
                continue
            nation = line.split(',')[-1] 
            ASN = line.split()[0].strip('AS')
            f.write(ASN+' '+nation+'\n')
        f.close()

        return 0

    def get_as2rank_file(self):
        # TODO: get data from all 3 date
        # only 3 date available
        date = '20121102'
        n = '42697'

        # TODO: if target file already exists
        # download from CAIDA
        rows = cmlib.get_weblist('http://as-rank.caida.org/?data-selected=\
                    2012-11-02&n=42697&ranksort=1&mode0=as-ranking')
        f = open(hdname+'support/asrank_'+date+'.txt', 'w')
        for line in rows.split('\n'):
            if line.isspace() or line == '\n' or line == '': 
                continue
            line = line.strip()
            f.write(line.replace(' ', '')+'\n')
        f.close()

        # extract useful information
        fr = open(hdname+'support/asrank_'+date+'.txt', 'r')
        fw = open(hdname+'support/asrank_'+date+'tmp.txt', 'w')
        trigger = 0
        count = 0
        for line in fr:
            line = line.strip('\n')
            if line == 'datasources':
                break
            if trigger < 2:
                if line == 'IPv4Addresses':
                    trigger += 1
                    continue
                else:
                    continue
            # now we begin to count info
            count += 1
            if count == 1:
                fw.write(line+' ')
            elif count == 2:
                fw.write(line+'\n')
            elif count == 11:
                count = 0
            else:
                pass

        fr.close()
        fw.close()

    def get_pfx2as_file(self):
        location = hdname + 'support/' + self.sdate + '/'
        cmlib.make_dir(location)

        tmp = os.listdir(hdname+'support/'+self.sdate+'/')
        for line in tmp:
            if 'pfx2as' in line:
                return 0 # we already have a prefix2as file

        print 'get pfx2as file ...'
        year, month = self.sdate[:4], self.sdate[4:6] # YYYY, MM
        webloc = 'http://data.caida.org/datasets/routing/routeviews-prefix2as' +\
                '/' + year + '/' + month + '/'

        webraw = get_weblist(webloc)
        target_line = ''
        for line in webraw.split('\n'):
            if self.sdate in line:
                target_line = line
                break

        if target_line == '':
            print 'downloading prefix2as file fails: no such date'
            return 0

        fname = target_line.split()[0]
        urllib.urlretrieve(webloc+fname, location+fname)
        subprocess.call('gunzip -c '+location+fname+' > '+\
                location+fname.replace('.gz', ''), shell=True)
        os.remove(location+fname)

        return 0

    def get_pfx2as_trie(self):
        pfx2as = patricia.trie(None)

        if int(self.sdate) >= 20050509:
            self.get_pfx2as_file()

            pfx2as_file = ''
            tmp = os.listdir(hdname+'support/'+self.sdate+'/')
            for line in tmp:
                if 'pfx2as' in line:
                    pfx2as_file = line
                    break

            f = open(hdname+'support/'+self.sdate+'/'+pfx2as_file)
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
            mydate = sdate[0:4] + '.' + sdate[4:6]
            rib_location = hdname+'routeviews.org/bgpdata/'+mydate+'/RIBS/'
            dir_list = os.listdir(hdname+'routeviews.org/'+c+'/bgpdata/'+mydate+'/RIBS/')


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

    def get_as2nation_dict(self):  # TODO: should consider datetime
        as2nation = {}

        f = open(hdname+'support/as2nation.txt')
        for line in f:
            as2nation[int(line.split()[0])] = line.split()[1]
        f.close()

        return as2nation

    def get_as2type_dict(self):
        as2type == {}

        # TODO: consider datetime
        f = open(hdname+'support/as2attr.txt')
        for line in f:
            line = line.strip('\n')
            as2type[int(line.split()[0])] = line.split()[-1]
        f.close()

        return as2type

    def get_as2rank_dict(self):
        as2rank == {}

        # TODO: consider datetime
        f = open(hdname+'support/asrank_20121102.txt')
        for line in f:
            line = line.strip('\n')
            as2rank[int(line.split()[1])] = int(line.split()[0])
        f.close()

        return as2rank

    def get_nation2cont_dict(self):
        nation2cont == {}

        f = open(hdname+'support/continents.txt')
        for line in f:
            line = line.strip('\n')
            nation2cont[line.split(',')[0]] = line.split(',')[1]
        f.close()

        return nation2cont
