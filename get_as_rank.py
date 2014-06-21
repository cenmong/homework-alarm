import cmlib
from env import *
import subprocess
import os

# run two times 
def get_file():
    date = '20121102'
    n = '42697'
    if os.path.exists(hdname+'topofile/asrank_'+date+'.txt'):
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
        
        os.remove(hdname+'topofile/asrank_'+date+'.txt')
        subprocess.call('mv '+hdname+'topofile/asrank_'+date+'tmp.txt'+' '+\
                hdname+'topofile/asrank_'+date+'.txt', shell=True)
        return 0

    rows =\
        cmlib.get_weblist('http://as-rank.caida.org/?data-selected=2012-11-02&n=42697&ranksort=1&mode0=as-ranking')
    f = open(hdname+'topofile/asrank_'+date+'.txt', 'w')
    for line in rows.split('\n'):
        if line.isspace() or line == '\n' or line == '': 
            continue
        line = line.strip()
        f.write(line.replace(' ', '')+'\n')
    f.close()

if __name__ == '__main__':
    get_file()
