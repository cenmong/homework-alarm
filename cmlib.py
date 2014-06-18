import os
import urllib
import subprocess
import re
import nltk

def getfile(url, save_loc, filename):
    if not os.path.exists(save_loc+filename):
        urllib.urlretrieve(url+filename, save_loc+filename)
    else:
        pass

def get_unpack_gz(url, save_loc, filename):
    if not os.path.exists(save_loc+re.sub('\.gz$', '', filename)):
        urllib.urlretrieve(url+filename, save_loc+filename)
        subprocess.call('gunzip '+save_loc+filename)
    else:
        pass

def get_unpack_bz2(url, save_loc, filename):
    if not os.path.exists(save_loc+re.sub('\.bz2$', '', filename)):
        urllib.urlretrieve(url+filename, save_loc+filename)
        subprocess.call('bunzip2 -d '+save_loc+filename)
    else:
        pass

def pack_gz(loc_fname):
    if not os.path.exists(loc_fname+'.gz'):
        subprocess.call('gzip '+loc_fname)
    else:
        pass

def make_dir(location):
    if not os.path.isdir(location):
        os.mkdir(location)

def get_weblist(url):
    webhtml = urllib.urlopen(url).read()
    webraw = nltk.clean_html(webhtml)
    return webraw

def parse_mrt(old_loc_fname, new_loc_fname):
    if not os.path.exists(new_loc_fname):
        subprocess.call('~/Downloads/libbgpdump-1.4.99.11/bgpdump -m '+\
                old_loc_fname+' > '+new_loc_fname, shell=True)
    else:  # file already exists
        pass

        
