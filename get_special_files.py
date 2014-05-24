import os
from env import hdname

'''
0:ref; 1:slammer; 2:coast BO; 3:Katrina; 4:LA BO; 5:TW cabel; 6:Mid cable 1;
7:Mid cable 2; 8:JP tsunami; 9:Spamhaus; 10:AU route leak; 11: Canada route
leak; 12:SC earthquake; 13:Chile earthquake; 14:Sandy; 15:Northeast US BO;
16:sea-me cable 17:TL BO
'''
clean = [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0]

ym = ['2008.12', '2003.01', '2003.08', '2005.09', '2005.08', '2006.12',\
    '2008.01', '2008.12', '2011.03', '2013.03', '2012.02', '2012.08',\
   '2008.05', '2010.02', '2012.10', '2013.02', '2010.04', '2013.05']

ymd1 = ['20081212', '20030124', '20030813', '20050910', '20050828', '20061225',\
     '20080129', '20081218', '20110310', '20130310', '20120221', '20120807',\
     '20080510', '20100226', '20121020', '20130207', '20100412', '20130520']

ymd2 = ['20081214', '20030126', '20030815', '20050912', '20050830', '20061227',\
     '20080131', '20081220', '20110312', '20130330', '20120224', '20120810',\
     '20080515', '20100228', '20121031', '20130211', '20120417', '20130524']

def get_file():
    for i in range(len(ym)-1, len(ym)):  # Flexible: just get what I need
        os.system('lynx -dump http://archive.routeviews.org/bgpdata/' + ym[i] +\
                '/UPDATES/ > tmpfile')
        f = open('tmpfile', 'r')
        if os.path.exists('metadata/files' + ymd1[i]):
            os.system('rm metadata/files' + ymd1[i])
        flist = open('metadata/files' + ymd1[i], 'a')
        for line in f.readlines():
            if line.split('.')[-1] != 'bz2\n':
                continue
            if int(line.split('.')[-3]) < int(ymd1[i]) or int(line.split('.')[-3])\
                    > int(ymd2[i]):
                continue
            topofile = line.split('//')[1]
            topofile = topofile.replace('\n', '')
            # Download files
            if not os.path.exists(hdname + topofile.replace('bz2', 'psd')):
                os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
                        http://' + topofile)
            else:
                print 'exists!'
                continue
            flist.write(topofile.replace('.bz2', '.psd') + '\n')  # Names of parsed files are stored
            if os.path.exists(hdname + topofile):
                try:
                    os.system('bunzip2 ' + hdname + topofile)  # Unpack the files
                except:
                    pass
            topofile = topofile.replace('.bz2', '')
            if os.path.exists(hdname + topofile):
                #try:
                os.system('~/Downloads/libbgpdump-1.4.99.11/bgpdump ' +\
                        hdname + topofile + ' > ' + hdname + topofile +\
                        '.psd')# Parse files using zebra parser
                #except:
                    #pass
            if os.path.exists(hdname + topofile + '.psd'):# Parsed files exist
                try:
                    os.system('rm ' + hdname + topofile)# Remove unparsed files
                except:# File has already been removed
                    pass
            if clean[i] == 1:  # file need clean
                os.system('python ~/Downloads/clean.py ' + hdname + topofile +\
                        '.psd')
        f.close()
        flist.close()
        os.system('rm tmpfile')
    return

if __name__ == '__main__':
    get_file()
