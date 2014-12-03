import os
import subprocess
import string
import cmlib

from alarm_class import *
from netaddr import *
from env import *

##################################################
# The Analyzer class read and pre-process every update. After that, each update is injected into
# certain object for analysis.
##################################################
class Analyzer():

    def __init__(self, filelist, granu, sdate, peak):
        self.filelist = filelist  # filelist file name 
        self.allowed = set(string.ascii_letters+string.digits+\
                '.'+':'+'|'+'/'+' '+'{'+'}'+','+'-')

        try:
            self.cl_list = cmlib.get_collector(sdate)  # get the collectors for this event
        except:
            self.cl_list = []

        self.cl_first = dict() # collector first existence, True or False
        for cl in collectors:
            self.cl_first[cl[0]] = True # initial value: True

        self.sdate = sdate
        self.granu = granu
        self.alarm = Alarm(granu, sdate, self.cl_list, peak)

    # check whether an update is normal or not
    # we see occational trash characters
    def is_normal(self, update):
        if set(update).issubset(self.allowed) and len(update.split('|')) > 5:
            return True
        else:
            return False

    def parse_updates(self):
        filelist = open(self.filelist, 'r')
        for ff in filelist:
            ff = ff.replace('\n', '')
            ff = ff.replace('archive.', '')
            ff = ff.split('|')[0]
            ff = datadir + ff
            print 'Reading ' + ff + '...'

            # unpack the update file
            subprocess.call('gunzip -c '+ff+' > '+ff.replace('txt.gz', 'txt'), shell=True)

            # get collector

            attributes = ff.split('/') 
            site_index = -1
            for a in attributes:
                site_index += 1
                if a == 'data.ris.ripe.net' or a == 'routeviews.org':
                    break

            cl = ff.split('/')[site_index + 1]
            if cl == 'bgpdata':  # route-views2, the special case
                cl = ''

            lastline = 'Do not delete me!'
            with open(ff.replace('txt.gz', 'txt'), 'r') as f:

                # this collector appears for the first time
                if self.cl_first[cl] == True:
                    for line in f:  # get first (ipv4) line
                        line = line.replace('\n', '')
                        if not self.is_normal(line):
                            continue
                        break
                    self.alarm.set_start(cl, line)  # set colllector's starting dt
                    self.alarm.add(line)
                    self.cl_first[cl] = False

                for line in f:
                    line = line.replace('\n', '')
                    if not self.is_normal(line):
                        continue
                    self.alarm.add(line)
                    lastline = line

            f.close()
            # remove the unpacked file to save space (the original one always remains)
            os.remove(ff.replace('txt.gz', 'txt'))

            try:
                self.alarm.set_now(cl, lastline)  # set collector's current/latest dt
            except:
                pass
            self.alarm.check_memo(False) # not the ending check

        self.alarm.check_memo(True) # the ending check
        filelist.close()
        
        self.alarm.output()
        alarmplot(self.sdate, self.granu)

        return 0
