import os 
from env import *

# Download files
ym = yearmonth[1]
monitor = rv_monitor[2]

os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
        http://archive.routeviews.org/' + monitor + '/bgpdata/' + ym +\
        '/UPDATES/')

# Get file list
os.system('lynx -dump http://archive.routeviews.org/' + monitor + '/bgpdata/' + ym +\
        '/UPDATES/ > filehtml')# filehtml is a tmp file that stores html info
f = open('filehtml', 'r')

# Create the file that stores file list of each monitor for each month
if os.path.exists('metadata/' + monitor + ym):
    os.system('rm metadata/' + monitor + ym)
flist = open('metadata/' + monitor + ym, 'a')

# Get and process info from html page
for line in f.readlines():
    if line.split('.')[-1] != 'bz2\n':# rabbish line
        continue
    topofile = line.split('//')[1]# This line corresponds to topo file
    flist.write(topofile.replace('.bz2', '.psd'))# Names of parsed files are stored
    topofile = topofile.replace('\n', '')# Remove the \n at the end of line
    if os.path.exists(hdname + topofile):# if the .bz file exists
        try:
            os.system('bunzip2 ' + hdname + topofile)#  Unpack the files
        except:
            pass
    topofile = topofile.replace('.bz2', '')# remove the '.bz' suffix
    if os.path.exists(hdname + topofile):# If the unpacked file exists
        try:
            os.system('cat ' + hdname + topofile + ' | time\
                    ~/Downloads/zebra-dump-parser/zebra-dump-parser.pl > ' + hdname + topofile +\
                    '.psd')# Parse files using zebra parser
        except:
            pass
    if os.path.exists(hdname + topofile + '.psd'):# Parsed files exist
        try:
            os.system('rm ' + hdname + topofile)# Remove the corresponding unparsed file
        except:# File has already been removed
            pass

f.close()
flist.close()
os.system('rm filehtml')
'''
####################################IPv6#####################################
# Download files
ym = yearmonth[0]
os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
        http://archive.routeviews.org/route-views6/bgpdata/' + ym +\
        '/UPDATES/')
# Get file list
os.system('lynx -dump http://archive.routeviews.org/route-views6/bgpdata/' + ym +\
        '/UPDATES/ > filehtml6')
f = open('filehtml6', 'r')
if os.path.exists('metadata/6files' + ym):
    os.system('rm metadata/6files' + ym)
flist = open('metadata/6files' + ym, 'a')
for line in f.readlines():
    if line.split('.')[-1] != 'bz2\n':
        continue
    topofile = line.split('//')[1]
    flist.write(topofile.replace('.bz2', '.psd'))# Names of parsed files are stored
    topofile = topofile.replace('\n', '')
    if os.path.exists(hdname + topofile):
        try:
            os.system('bunzip2 ' + hdname + topofile)#  Unpack the files
        except:
            pass
    topofile = topofile.replace('.bz2', '')
    if os.path.exists(hdname + topofile):
        try:
            os.system('cat ' + hdname + topofile + ' | time\
                    ~/Downloads/zebra-dump-parser/zebra-dump-parser.pl > ' + hdname + topofile +\
                    '.psd')# Parse files using zebra parser
        except:
            pass
    if os.path.exists(hdname + topofile + '.psd'):# Parsed files exist
        try:
            os.system('rm ' + hdname + topofile)# Remove unparsed files
        except:# File has already been removed
            pass

f.close()
flist.close()
os.system('rm filehtml6')
####################################IPv4#####################################
# Download files
ym = yearmonth[0]
os.system('wget -e robots=off --connect-timeout=3000 -np -P ' + hdname + ' -c -m -r -A.bz2\
        http://archive.routeviews.org/bgpdata/' + ym +\
        '/UPDATES/')
# Get file list
os.system('lynx -dump http://archive.routeviews.org/bgpdata/' + ym +\
        '/UPDATES/ > filehtml4')
f = open('filehtml4', 'r')
if os.path.exists('metadata/4files' + ym):
    os.system('rm metadata/4files' + ym)
flist = open('metadata/4files' + ym, 'a')
for line in f.readlines():
    if line.split('.')[-1] != 'bz2\n':
        continue
    topofile = line.split('//')[1]
    flist.write(topofile.replace('.bz2', '.psd'))# Names of parsed files are stored
    topofile = topofile.replace('\n', '')
    if os.path.exists(hdname + topofile):
        try:
            os.system('bunzip2 ' + hdname + topofile)#  Unpack the files
        except:
            pass
    topofile = topofile.replace('.bz2', '')
    if os.path.exists(hdname + topofile):
        try:
            os.system('cat ' + hdname + topofile + ' | time\
                    ~/Downloads/zebra-dump-parser/zebra-dump-parser.pl > ' + hdname + topofile +\
                    '.psd')# Parse files using zebra parser
        except:
            pass
    if os.path.exists(hdname + topofile + '.psd'):# Parsed files exist
        try:
            os.system('rm ' + hdname + topofile)# Remove unparsed files
        except:# File has already been removed
            pass

f.close()
flist.close()
os.system('rm filehtml4')
'''
