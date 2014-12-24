from period_class import *
from env import *
from alarm_class import *
from meliae import scanner
from netaddr import *
from cStringIO import StringIO
scanner.dump_all_objects('memory.json')

import os
import subprocess
import string
import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Program starts!')

#----------------------------------------------------------------
# Check whether an update contains ilegal char (Note: this happens rather rarely)

# FIXME: this costs too much time. Use try-except instead.
allowed_char = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+' '+'{'+'}'+','+'-')
def update_is_normal(update):
    if set(update).issubset(allowed_char) and len(update.split('|')) > 5:
        return True
    else:
        #logging.info('abnormal update:%s',update)
        return False


#-----------------------------------------------------------------
# From now on we deal with each application separately
# Note: 
# different applications may require different monitor and prefix sets!
# Some applications may use sliding window rather than fixed window
# Microscopic analysis (e.g., case studies) should be in other .py

for i in [27]:

    my_period = Period(i)
    # TODO decide the monitors/collectors to use, consider time duration length
    cl_list = []
    # TODO create a combined update file list according to the corresponding collectors
    #filelist = datadir+'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    filelist = '/media/usb/update_list/20141130_20141201/_list.txt'

    granu = 10 # granularity in minutes
    #alarm = Alarm(granu, i, cl_list)


    #----------------------
    # Define an class that gets and *stores* all information about a duration, given requirements
    # stores: collectors monitors prefixes filelist outputDir granularity...
    # Define other classes that use the previous one as input
    # Get_prefix_info, analyze_middle, plot

    # FIXME put *Reading Lines* into alarm class
    fl = open(filelist, 'r')
    for fline in fl:
        fline = datadir + fline.split('|')[0]
        print 'Reading ' + fline + '...'

        #------------------------------------------------------------------------
        # get current file's collector

        attributes = fline.split('/') 
        j = -1
        for a in attributes:
            j += 1
            if a.startswith('data.ris') or a.startswith('routeviews'):
                break

        cl = fline.split('/')[j + 1]
        if cl == 'bgpdata':  # route-views2, the special case
            cl = ''

        #--------------------------------------------------------------------------
        # Process the updates one by one

        #if os.path.exists(fline.replace('txt.gz', 'txt')): # This happens occassionally
        p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE)
        f = StringIO(p.communicate()[0])
        assert p.returncode == 0

        #lastline = 'Do not delete me!'
        for line in f:
            line = line.rstrip('\n')
            #if not update_is_normal(line):
            #    print line
            #    continue
            #alarm.add(line)
            #lastline = line

        f.close()
        #alarm.set_now(cl, lastline)  # set the current collector's current dt
        #alarm.set_now(cl, line)  # set the current collector's current dt
        #alarm.check_memo()

    fl.close()
    #alarm.output()
    #alarmplot(sdate, granu)

    ''' This is the ideal way, totally automatic
    try:
        alarmplot(daterange[i][0], granu)
    except:
        # Read and analyze everything
    '''

logging.info('Program ends!')
