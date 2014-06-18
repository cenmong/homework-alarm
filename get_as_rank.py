import cmlib
from env import *

def get_file():
    date = '20121102'
    #if os.path.exists(hdname+'topofile/asrank_'+date+'.txt'):
    #    return 0
    rows =\
        cmlib.get_weblist('http://as-rank.caida.org/?data-selected=2012-11-02&n=33384&ranksort=1&mode0=org-ranking')
    f = open(hdname+'topofile/asrank_'+date+'.txt', 'w')
    for line in rows.split('\n'):
        #if 'AS' not in line:
        #    continue
        #state = line.split(',')[-1] 
        #ASN = line.split()[0].strip('AS')
        #f.write(ASN+' '+state+'\n')
        f.write(line+'\n')
    f.close()

if __name__ == '__main__':
    get_file()
