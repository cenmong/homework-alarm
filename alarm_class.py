import radix # takes 1/4 the time as patricia
import patricia
import datetime
import time as time_lib
import numpy as np
import cmlib
import operator
import string
import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

from netaddr import *
from env import *
from supporter_class import *
from cStringIO import StringIO

class Alarm():

    def __init__(self, period, granu):
        self.period = period

        self.filelist = period.filelist
        print self.filelist

        self.sdate = period.sdate
        self.edate = period.edate 
        self.granu = granu

        self.cl_list = period.co_mo.keys()
        self.max_dt = -1

        self.output_dir = datadir+'output/'+self.sdate+'_'+self.edate+'/'
        cmlib.make_dir(self.output_dir)


        self.monitors = []
        tmp_co_mo = period.co_mo
        for co in tmp_co_mo.keys():
            self.monitors.extend(tmp_co_mo[co])

        self.mcount = len(self.monitors)

        self.mo2index = {} # map monitor ip to an index
        index = 0
        for mo in self.monitors:
            self.mo2index[mo] = index
            index += 1
    
        self.no_prefixes = period.no_prefixes # a trie TODO

        self.pfx_trie = dict() # every dt has a corresponding trie, deleted periodically

        #-----------------------------------------------------
        # For synchronization among collectors and conducting timely aggregation
        # Note: assume all collectors will exist after self.sdate + 1 hour

        self.cl_dt = {}  # The current datetime of every collector, for getting ceiling
        for cl in self.cl_list:
            self.cl_dt[cl] = 0

        tmp_dt = datetime.datetime(int(self.sdate[0:4]),\
                int(self.sdate[4:6]),int(self.sdate[6:8]),0,0)
        # do not fill up the hour to allow for the edge value being analyzed
        tmp_dt = tmp_dt + datetime.timedelta(minutes=58)
        tmp_dt = time_lib.mktime(tmp_dt.timetuple())

        # floor is only for ignoring anything before self.sdate + 1 hour
        self.floor = tmp_dt
        # we output everything below ceiling and above floor
        self.ceiling = self.floor  

        tmp_dt = datetime.datetime(int(self.edate[0:4]),\
                int(self.edate[4:6]),int(self.edate[6:8]),23,59)
        tmp_dt = tmp_dt + datetime.timedelta(minutes=-58)
        tmp_dt = time_lib.mktime(tmp_dt.timetuple())  # Change into seconds int
        self.top_ceiling = tmp_dt # self.ceiling cannot exceed this value

    #----------------------------------------------------------------
    # FIXME: this costs too much time. Use try-except instead.
    def update_is_normal(self, update):
        allowed_char = set(string.ascii_letters+string.digits+'.'+':'+'|'+'/'+' '+'{'+'}'+','+'-')
        if set(update).issubset(allowed_char) and len(update.split('|')) > 5:
            return True
        else:
            #logging.info('abnormal update:%s',update)
            return False

    def readfiles(self):
        fl = open(self.filelist, 'r')
        for fline in fl:
            fline = datadir + fline.split('|')[0]
            print datetime.datetime.now()
            print 'Reading ' + fline + '...'

            # get current file's collector
            attributes = fline.split('/') 
            j = -1
            for a in attributes:
                j += 1
                # XXX be careful when changing RV URL
                if a.startswith('data.ris') or a.startswith('archi'):
                    break

            cl = fline.split('/')[j + 1]
            if cl == 'bgpdata':  # route-views2, the special case
                cl = ''


            #if os.path.exists(fline.replace('txt.gz', 'txt')): # This happens occassionally
            p = subprocess.Popen(['zcat', fline],stdout=subprocess.PIPE)
            f = StringIO(p.communicate()[0])
            assert p.returncode == 0

            for line in f:
                line = line.rstrip('\n')
                #if not self.update_is_normal(line):
                #   print line
                #    continue
                self.add(line)

            f.close()
            self.set_now(cl, line)  # set the current collector's current dt
            self.check_memo()

        fl.close()

    def check_memo(self):
        print 'Checking memory to see if it is appropriate to output and release...'
        # Obtain the ceiling: lowest 'current datetime' among all collectors
        # Aggregate everything before ceiling - granulirity
        # Because aggregating 10:10 means aggregating 10:10~10:10+granularity
        new_ceil = 9999999999
        for cl in self.cl_list:
            if self.cl_dt[cl] < new_ceil:
                new_ceil = self.cl_dt[cl]

        if new_ceil - self.ceiling >= 4 * 60 * self.granu:  # Minimum is 2 *
            self.ceiling = new_ceil - 60 * self.granu
            if self.ceiling > self.top_ceiling:
                self.ceiling = self.top_ceiling
            self.release_memo()

        return 0

    # output everything before ceiling and remove garbage
    def release_memo(self):
        rel_dt = []  # dt list for releasing
        for dt in self.pfx_trie.keys():
            if self.floor <= dt <= self.ceiling:
                rel_dt.append(dt)

        self.middle_output(rel_dt)
        self.del_garbage()

        return 0

    # delete the tires that have already been used
    def del_garbage(self):
        for dt in self.pfx_trie.keys():  # all dt that exists
            if dt <= self.ceiling:
                del self.pfx_trie[dt]
        return 0

    def add(self, update):
        attr = update.split('|') 
        # TODO use try-except to filter out illegal updates
        
        mo = attr[3]
        try:
            index = self.mo2index[mo]
        except: # not a monitor that we have interest in
            return -1

        # change datetime to fit granularity
        intdt = int(attr[1])
        dt = intdt / (60 * self.granu) * 60 * self.granu # -28800 or not?

        # run into a brand new dt!
        if dt > self.max_dt:
            print 'new dt!'
            self.max_dt = dt
            ##self.pfx_trie[dt] = patricia.trie(None)
            self.pfx_trie[dt] = radix.Radix()

        # TODO check illegal prefix
        ##pfx = cmlib.pfx4_to_binary(attr[5])
        ##try:
        try:
            ##self.pfx_trie[dt][pfx][index] += 1
            rnode = self.pfx_trie[dt].search_exact(attr[5])
            rnode.data[0][index] += 1
            if rnode.prefix == '31.13.195.0/24':
                print rnode.data
        except: # prefix node does not exist
            ##self.pfx_trie[dt][pfx] = [0] * self.mcount
            ##self.pfx_trie[dt][pfx][index] = 1
            rnode = self.pfx_trie[dt].add(attr[5])
            rnode.data[0] = [0] * self.mcount
            rnode.data[0][index] = 1
        ##except: # self.pfx_trie[dt] has already been deleted. rarely happen 
        ##    return -1

        if ':' in attr[5] or len(attr[5]) == 1: # IPv6 and a very strange case
            return -1

        # XXX should we record update type?

        return 0

    
    def middle_output(self, rel_dt):
        for dt in rel_dt:
            print 'outputting info in dt:'
            cmlib.print_dt(dt)

            trie = self.pfx_trie[dt]
            outfile = self.output_dir + str(dt) + '.txt'
            f.open(outfile, 'w')
            for pfx in trie:
                if pfx == '': # the root node (the source of a potential bug)
                    continue
                mylist = trie[pfx]
                f.write(pfx+':')
                for i in xrange(0, len(my_list)):
                    f.write(str(mylist[i]) + '|')
                f.write('\n')
            f.close()

        return 0

    def analyze(self):
        self.readfiles()

    # XXX ugly
    # TODO: do not consider the format of plots (e.g. CDF, time-series) when outputing
    def output(self):
        for dt in self.pfxcount[0].keys():
            self.all_pcount_lzero += self.pfxcount[0][dt]

        ##################################################
        # Record the most basic information
        #####################################################
        f = open(self.output_dir+'basic.txt', 'w')
        f.write('Monitor # '+str(self.mcount)+'\n')
        f.write('Monitor AS '+str(len(self.m_as_m.keys()))+':'+str(self.m_as_m)+'\n')
        f.write('Monitor nation '+str(len(self.m_nation_as.keys()))+':'+str(self.m_nation_as)+'\n')
        f.write('# of AS: '+str(self.all_ascount)+'\n')
        stub = 0
        for asn in self.as2cc.keys():
            if self.as2cc[asn] <= 1:
                stub += 1
        f.write('# of stub AS: '+str(stub)+'\n')
        tmp_list = sorted(self.as2rank.iteritems(),\
                key=operator.itemgetter(1), reverse=True)
        rank_count = tmp_list[0][1]
        f.write('# of ranks: '+str(rank_count)+'\n')
        f.write('# of prefix: '+str(self.all_pcount)+'\n')
        f.close()

        ###################################################
        # DV distribution: mean and standard deviation
        # dv,mean,deviation|dv ...
        #####################################################
        print 'Recording DV distribution...'
        for dt in self.dv_distribution.keys(): # dt: DV value: prefix count 
            for dv in self.dv_distribution[dt].keys():
                self.dv_distribution[dt][dv] = \
                        float(self.dv_distribution[dt][dv]) / float(self.pfxcount[0][dt])
            # convert count to cumulative count
            self.dv_cdf[dt] = self.value_count2cdf(self.dv_distribution[dt])

        all_dv_list = [0] # all possible DVs: from 1/mcount to 1
        for i in xrange(1, self.mcount):
            all_dv_list.append(float(i)/float(self.mcount))

        dv_mean_dev = dict() # dv: [mean, deviation] for all dt's
        for dv in all_dv_list:
            values = [] # values of all dt's
            for dt in self.dv_cdf.keys():
                try:
                    my_value = self.dv_cdf[dt][dv]
                except: # no such DV as key
                    latest_dv = 0 
                    for key in self.dv_cdf[dt].keys():
                        if key > latest_dv and key < dv:
                            latest_dv = key
                    my_value = self.dv_cdf[dt][latest_dv]
                values.append(my_value)

            mean = sum(values)/len(values)
            dev = np.std(values)
            dv_mean_dev[dv] = [mean, dev]

        f = open(self.output_dir+'dv_distribution.txt', 'w')
        for dv in all_dv_list:
            mean = dv_mean_dev[dv][0]
            dev = dv_mean_dev[dv][1]
            f.write(str(dv)+','+str(mean)+','+str(dev)+'|')
        f.close()

        ###################################################
        # AS distribution: mean and standard deviation
        # NOTE: it's possible that y cannot reach 100% (we only use top 500 ASes)
        # Or because of the failure of mapping prefix to ASN
        # dv level:AS count,mean,deviation|AS count...\n dv level:...
        #####################################################
        print 'Recording AS distribution...'
        f = open(self.output_dir+'as_distribution.txt', 'w')
        fa = open(self.output_dir+'active_as.txt', 'w')

        for dl in self.dv_level:
            f.write(str(dl)+':')
            fa.write(str(dl)+':')

            active_as = dict() # dt: [[as1,count1],[as2,count2]]
            as_cdf = dict() # dt: as count: prefix count
            for dt in self.dv_dt_asn_pfx[dl].keys():
                # dv_dt_asn_pfx[dl][dt] should have 500 keys
                for a in xrange(1, 501):
                    try:
                        test = self.dv_dt_asn_pfx[dl][dt][a]
                    except:
                        self.dv_dt_asn_pfx[dl][dt][a] = 0
                as_cdf[dt] = self.symbol_count2cdf(self.dv_dt_asn_pfx[dl][dt])
                # get top 5 ASes of each time slot
                tmp_list = sorted(self.dv_dt_asn_pfx[dl][dt].iteritems(),\
                        key=operator.itemgetter(1), reverse=True)
                active_as[dt] = tmp_list[0:5] # [[ASN, pfx count ratio],...]
            for j in xrange(1, 501): # AS count from 1 to 500
                values = [] # values of all dt's
                for dt in as_cdf.keys():
                    values.append(as_cdf[dt][j])
                mean = sum(values)/len(values)
                dev = np.std(values)
                f.write(str(j)+','+str(mean)+','+str(dev)+'|')

            f.write('\n')
            # Record top 10 ASes of this DV level (all time slots)
            as_count = dict() # AS: count
            for dt in active_as.keys():
                for item in active_as[dt]:
                    try:
                        as_count[item[0]] += 1
                    except:
                        as_count[item[0]] = 1

            tmp_list = sorted(as_count.iteritems(),\
                    key=operator.itemgetter(1), reverse=True)
            tmp_list = tmp_list[0:20]
            for item in tmp_list:
                asn = item[0]
                count = item[1]
                asrank = self.as_to_rank(asn)
                nation = self.as_to_nation(asn)
                fa.write(str(asn)+','+str(count)+','+str(asrank)+','+str(nation)+'|')
            fa.write('\n') 
        f.close()
        fa.close()
                
        #################################################
        # quantity of prefixes of high DV ranges in different dt
        # dv level:dt,count|dt,count|...\n dv level...
        #######################################################
        print 'Recording quantity of prefix of dv ranges'
        f = open(self.output_dir+'high_dv.txt', 'w')
        for dl in self.dvrange_dt_pfx.keys():
            f.write(str(dl)+':')
            for dt in self.dvrange_dt_pfx[dl].keys():
                f.write(str(dt)+','+str(self.dvrange_dt_pfx[dl][dt])+'|')
            f.write('\n')

        f.close()

        ###################################
        # Plot prefix count of DV > XX
        # dv level:dt,value|dt,value|...\n dv level...
        ######################################
        f = open(self.output_dir+'HDVP.txt', 'w')
        for dl in self.dv_dt_hdvp.keys():
            f.write(str(dl)+':')
            for dt in self.dv_dt_hdvp[dl].keys():
                f.write(str(dt)+','+str(self.dv_dt_hdvp[dl][dt])+'|')
            f.write('\n')
        f.close()

        #################################################
        # prefix lengthes
        # dv range:length,count|length...\n dv range...
        #######################################################
        print 'Recording length distribution'
        f = open(self.output_dir+'prefix_length_cdf.txt', 'w')
        for dl in self.dv_level:
            f.write(str(dl)+':')
            for i in xrange(1, 33):
                try:
                    test = self.dvrange_len_pfx[dl][i]
                except:
                    self.dvrange_len_pfx[dl][i] = 0
            mydict = self.value_count2cdf(self.dvrange_len_pfx[dl])
            for k in mydict.keys():
                value = mydict[k]
                total_pfx = 0
                for dt in self.pfxcount_range[dl].keys():
                    total_pfx += self.pfxcount_range[dl][dt]
                if total_pfx != 0:
                    value = float(value) / total_pfx
                else:
                    value = 0
                f.write(str(k)+','+str(value)+'|')
            f.write('\n')

        tmp_result = cmlib.get_all_length(self.sdate) # length: prefix count (all)
        all_length = tmp_result[0] # length: prefix count (all)
        tmp_pcount = tmp_result[1] # Number of all prefixes from RIB
        f.write('all:')
        for i in xrange(1, 33):
            try:
                test = all_length[i]
            except:
                all_length[i] = 0

        mydict = self.value_count2cdf(all_length)
        for k in mydict.keys():
            value = mydict[k]
            value = float(value) / float(tmp_pcount)
            f.write(str(k)+','+str(value)+'|')
        f.write('\n')

        f.close()

        ###################################
        # Plot everything about update quantity
        # dt,value|dt,value...
        ######################################
        f = open(self.output_dir+'announce_count.txt', 'w')
        for dt in self.acount.keys():
            f.write(str(dt)+','+str(self.acount[dt])+'|')
        f.close()

        f = open(self.output_dir+'withdraw_count.txt', 'w')
        for dt in self.wcount.keys():
            f.write(str(dt)+','+str(self.wcount[dt])+'|')
        f.close()

        f = open(self.output_dir+'update_count.txt', 'w')
        for dt in self.ucount.keys():
            f.write(str(dt)+','+str(self.ucount[dt])+'|')
        f.close()

        f = open(self.output_dir+'prefix_count.txt', 'w')
        for dt in self.pfxcount[0].keys():
            f.write(str(dt)+','+str(self.pfxcount[0][dt])+'|')
        f.close()

        #myplot.time_series_plot(self.granu, self.wpctg, 'withdraw_percentage')
                
        ###################################
        # Plot before and after event
        # before:dv,count|dv...\n after:dv,count|dv...
        # before:AS count,value|AS count...\n after:AS count,value|...
        ######################################
        if self.compare:
            #DV distribution
            f = open(self.output_dir+'dv_cdf_bfr_aft.txt', 'w')
            mydict_b = self.value_count2cdf(self.cdfbfr)
            f.write('before:')
            for k in mydict_b.keys():
                value = mydict_b[k]
                value = float(value) / float(self.pfxcount[0][self.bfr_start])
                f.write(str(k)+','+str(value)+'|')
            f.write('\n')

            mydict_a = self.value_count2cdf(self.cdfaft)
            f.write('after:')
            for k in mydict_a.keys():
                value = mydict_a[k]
                value = float(value) / float(self.pfxcount[0][self.cdfbound])
                f.write(str(k)+','+str(value)+'|')
            f.close()

            #AS distribution: top AS record
            for dl in self.dv_level:
                f = open(self.output_dir+'event_top_as_'+str(dl)+'.txt', 'w')
                f.write('before:')
                for item in sorted(self.as_bfr[dl].iteritems(),\
                        key=operator.itemgetter(1), reverse=True):
                    asrank = self.as_to_rank(item[0])
                    nation = self.as_to_nation(item[0])
                    f.write(str(item[0])+','+str(item[1])+','+str(asrank)+','+str(nation)+'\n')
                f.write('\n')

                f.write('after:')
                for item in sorted(self.as_aft[dl].iteritems(),\
                        key=operator.itemgetter(1), reverse=True):
                    asrank = self.as_to_rank(item[0])
                    nation = self.as_to_nation(item[0])
                    f.write(str(item[0])+','+str(item[1])+','+str(asrank)+','+str(nation)+'\n')
                f.close()

            #AS distribution: CDF record
            for dl in self.dv_level:
                dict_bfr = self.symbol_count2cdf(self.as_bfr[dl])
                dict_aft = self.symbol_count2cdf(self.as_aft[dl])
                f = open(self.output_dir+'event_as_cdfs_'+str(dl)+'.txt', 'w')
                f.write('before:')
                for k in dict_bfr.keys():
                    f.write(str(k)+','+str(dict_bfr[k])+'|')
                f.write('\n')
                f.write('after:')
                for k in dict_aft.keys():
                    f.write(str(k)+','+str(dict_aft[k])+'|')

                f.close()

        ###########################################
        # record prefix DV > 15% for miltiple times ranking
        #############################################
        f = open(self.output_dir + 'dup_pfx.txt', 'w')
        f.write('0.15:\n')
        my_trie = self.dup_trie
        my_dict = {}
        for key in sorted(my_trie.iter('')):
            if key != '':
                my_dict[key] = my_trie[key]
        del my_trie

        stop = 0 # only get top 20
        for item in sorted(my_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
            stop += 1
            if stop > 20:
                break
            pfx = item[0]
            asn = self.pfx_to_as(pfx)
            asrank = self.as_to_rank(asn)
            ascc = self.as_to_cc(asn)
            nation = self.as_to_nation(asn)
            value = item[1]
            f.write(pfx+','+str(len(pfx))+','+str(value)+','+str(asn)+\
                    ','+str(ascc)+','+str(asrank)+','+str(nation)+'\n')
        f.write('\n')
        f.close()

        return 0

    def value_count2cdf(self, vc_dict): # dict keys are contable values
        cdf = dict()
        xlist = [0]
        ylist = [0]

        for key in sorted(vc_dict): # sort by key
            xlist.append(key)
            ylist.append(vc_dict[key])

        ## y is the number of values that <= x
        for i in xrange(1, len(ylist)):
            ylist[i] += ylist[i-1]

        ## change y into percentage
        giant = ylist[-1] # the largest y value
        if giant == 0: # no value actually
            return {1:1}
        for i in xrange(0, len(ylist)):
            cdf[xlist[i]] = ylist[i]

        return cdf # NOTE: dict are not ordered. Order by key when plotting


    def symbol_count2cdf(self, sc_dict): # dict keys are just symbols
        cdf = dict()
        xlist = [0]
        ylist = [0]

        j = 0
        # large to small
        for item in sorted(sc_dict.iteritems(), key=operator.itemgetter(1), reverse=True):
            j += 1
            xlist.append(j)
            ylist.append(item[1])

        for i in xrange(1, len(ylist)):
            ylist[i] += ylist[i-1]

        for i in xrange(0, len(ylist)):
            cdf[xlist[i]] = ylist[i]

        return cdf # NOTE: dict are not ordered. Order by key when plotting


    def pfx_to_as(self, mypfx):
        try:
            asn = self.pfx2as[mypfx]
            return asn
        except:
            return -1


    def as_to_nation(self, myasn):
        try:
            return self.as2nation[myasn]
        except:
            return -1


    def as_to_cc(self, myasn):
        try:
            return self.as2cc[myasn]
        except:
            return -1

    def as_to_rank(self, myasn):
        try:
            return self.as2rank[myasn]
        except:
            return -1
    
    #-----------------------------------------------------------------
    # change a value to exactly fit to the granularity

    def shang_qu_zheng(self, value, tp):  # 'm': minute, 's': second
        if tp == 's':
            return (value + 60 * self.granu) / (60 * self.granu) * (60 * self.granu)
        elif tp == 'm':
            return (value + self.granu) / self.granu * self.granu
        else:
             return False 

    def xia_qu_zheng(self, value, tp):
        if tp == 's':
            return value / (60 * self.granu) * (60 * self.granu)
        elif tp == 'm':
            return value / self.granu * self.granu
        else:
            return False

    #-----------------------------------------------------------------
    # Set the datetime flag of every collector for synchronization

    def set_now(self, cl, line):
        #self.cl_dt[cl][1] = int(line.split('|')[1]) - 28800 # must -8 Hours
        self.cl_dt[cl] = int(line.split('|')[1]) # WHY not -8H any more?
        return 0
    
