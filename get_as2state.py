import cmlib
from env import *
 
def get_file():
    if os.path.exists(hdname+'topofile/as2state.txt'):
        return 0
    rows = cmlib.get_weblist('http://bgp.potaroo.net/cidr/autnums.html')
    f = open(hdname+'topofile/as2state.txt', 'w')
    for line in rows.split('\n'):
        if 'AS' not in line:
            continue
        state = line.split(',')[-1] 
        ASN = line.split()[0].strip('AS')
        f.write(ASN+' '+state+'\n')
    f.close()

if __name__ == '__main__':
    get_file()
