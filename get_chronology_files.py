import os
from env import hdname

def get_file():
    for i in range(2003, 2014):
        for j in ['01', '04', '07', '10']:
            os.system('lynx -dump http://archive.routeviews.org/bgpdata/'+\
                    str(i)+'.'+j+'/UPDATES/ > tmpfile')
            f = open('tmpfile', 'r')
            if os.path.exists('metadata/chronology_files'):
                os.system('rm metadata/chronology_files')
            flist = open('metadata/chronology_files', 'a')
            for line in f.readlines():
                if line.split('.')[-1] != 'bz2\n':
                    continue
                if int(line.split('.')[-3]) != int(str(i)+j+'01'):
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
                os.system('python ~/Downloads/clean.py ' + hdname + topofile +\
                        '.psd')
            f.close()
            flist.close()
            os.system('rm tmpfile')
    return

if __name__ == '__main__':
    get_file()
