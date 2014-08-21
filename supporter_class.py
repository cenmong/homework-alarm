import cmlib
import subprocess
import os

from env import *

class Supporter():

    def __init__(self):
        return 0

    def get_as2nation(self):
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

    def get_as_rank(self):
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
