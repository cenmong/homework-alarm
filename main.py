from env import *
from alarm_class import *
from meliae import scanner
from netaddr import *
scanner.dump_all_objects('memory.json')

import os
import subprocess
import string
import cmlib
import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Program starts!')

#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
#cmlib.combine_cdf()
#dthres = 0.005785

#----------------------------------------------------------------
# Check whether an update contains ilegal char
# Note: this happen rather rarely
def update_is_normal(update):
    allowed_char = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+' '+'{'+'}'+','+'-')
    if set(update).issubset(allowed_char) and len(update.split('|')) > 5:
        return True
    else:
        return False


#-----------------------------------------------------------------
# The major task

for i in [0]:
    cl_list = [] # collectors to use
    # TODO decide the monitors to use, consider time duration length
    # TODO create a combined update file list according to the corresponding collectors
    filelist = datadir+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    cl_first = dict() # collector first existence, True or False
    for cl in cl_list:
        cl_first[cl] = True # initial value: True

    # Do not put the logic of specific missions here!
    #soccur = daterange[i][6] # event occur start
    #eoccur = daterange[i][7] # event occur end
    #des = daterange[i][8] # event description

    #try:
        #peak = daterange[i][9] # the occurrence of HDVP peak
    #except:
        #peak = None

    granu = 10 # granularity in minutes
    sdate = ???
    alarm = Alarm(granu, sdate, cl_list, peak)

    f = open(filelist, 'r')
    for fline in f:
        fline = datadir + fline.replace('\n', '').replace('archive.', '').split('|')[0]
        print 'Reading ' + fline + '...'

        subprocess.call('gunzip -c '+fline+' > '+fline.replace('txt.gz', 'txt'), shell=True) # unpack

        # get current file's collector
        attributes = fline.split('/') 
        site_index = -1
        for a in attributes:
            site_index += 1
            if a == 'data.ris.ripe.net' or a == 'routeviews.org':
                break

        cl = fline.split('/')[site_index + 1]
        if cl == 'bgpdata':  # route-views2, the special case
            cl = ''

        lastline = 'Do not delete me!'
        f2 = open(fline.replace('txt.gz', 'txt'), 'r')
        # this collector appears for the first time
        if cl_first[cl] == True:
            for line in f2:  # get first (ipv4) line
                line = line.rstrip('\n')
                if not update_is_normal(line):
                    continue
                break
            alarm.set_start(cl, line)  # set colllector's starting dt
            alarm.add(line)
            cl_first[cl] = False

        for line in f2:
            line = line.rstrip('\n')
            if not update_is_normal(line):
                continue
            alarm.add(line)
            lastline = line

        f2.close()
        # remove the unpacked file to save space (the original one always remains)
        # Note that sometimes os. may fail to excute
        os.remove(fline.replace('txt.gz', 'txt'))

        try:
            alarm.set_now(cl, lastline)  # set the current collector's latest dt
        except:
            pass
        alarm.check_memo(False) # not the ending check

    alarm.check_memo(True) # the ending check
    f.close()
    
    alarm.output()
    alarmplot(sdate, granu)

    ''' This is the ideal way, totally automatic
    try:
        alarmplot(daterange[i][0], granu)
    except:
        # Read and analyze everything
    '''

logging.info('Program ends!')
