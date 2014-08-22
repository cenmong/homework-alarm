import cmlib
import subprocess
import os

from env import *

class Supporter():

    def __init__(self):
        return 0

    def get_as2nation_file(self):
        if os.path.exists(hdname+'topofile/as2nation.txt'):
            return 0
        rows = cmlib.get_weblist('http://bgp.potaroo.net/cidr/autnums.html')
        f = open(hdname+'topofile/as2nation.txt', 'w')
        for line in rows.split('\n'):
            if 'AS' not in line:
                continue
            nation = line.split(',')[-1] 
            ASN = line.split()[0].strip('AS')
            f.write(ASN+' '+nation+'\n')
        f.close()

        return 0

    def get_asrank_file(self):
        date = '20121102'
        n = '42697'

        # download and write everything
        rows = cmlib.get_weblist('http://as-rank.caida.org/?data-selected=\
                    2012-11-02&n=42697&ranksort=1&mode0=as-ranking')
        f = open(hdname+'topofile/asrank_'+date+'.txt', 'w')
        for line in rows.split('\n'):
            if line.isspace() or line == '\n' or line == '': 
                continue
            line = line.strip()
            f.write(line.replace(' ', '')+'\n')
        f.close()

        # extract useful information
        fr = open(hdname+'topofile/asrank_'+date+'.txt', 'r')
        fw = open(hdname+'topofile/asrank_'+date+'tmp.txt', 'w')
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

    def get_pfx2as_trie(self, sdate):
        pfx2as = patricia.trie(None)

        pfx2as_file = ''
        tmp = os.listdir(hdname+'topofile/'+sdate+'/')
        for line in tmp:
            if 'pfx2as' in line:
                pfx2as_file = line
                break

        f = open(hdname+'topofile/'+sdate+'/'+pfx2as_file)
        for line in f:
            print line
            line = line.rstrip('\n')
            attr = line.split()
            if '_' in attr[2] or ',' in attr[2]:
                continue
            pfx = cmlib.ip_to_binary(attr[0]+'/'+attr[1], '0.0.0.0')
            try:
                pfx2as[pfx] = int(attr[2]) # pfx: origin AS
            except:
                pfx2as[pfx] = -1
        f.close()

        return pfx2as

    def get_as2nation_dict(self, sdate):  # TODO: should consider datetime
        as2nation = {}

        f = open(hdname+'topofile/as2nation.txt')
        for line in f:
            as2nation[int(line.split()[0])] = line.split()[1]
        f.close()

        return as2nation

    def get_as2type_dict(self, sdate):
        as2type == {}

        f = open(hdname+'topofile/as2attr.txt')
        for line in f:
            line = line.strip('\n')
            as2type[int(line.split()[0])] = line.split()[-1]
        f.close()

        return as2type

    def get_as2rank_dict(self, sdate):
        as2rank == {}

        f = open(hdname+'topofile/asrank_20121102.txt')
        for line in f:
            line = line.strip('\n')
            as2rank[int(line.split()[1])] = int(line.split()[0])
        f.close()

        return as2rank

    def get_nation2cont_dict(self):
        nation2cont == {}

        f = open(hdname+'topofile/continents.txt')
        for line in f:
            line = line.strip('\n')
            nation2cont[line.split(',')[0]] = line.split(',')[1]
        f.close()

        return nation2cont
