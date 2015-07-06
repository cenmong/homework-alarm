import radix # takes 1/4 the time as patricia
import numpy as np
import cmlib
import operator
import logging
import subprocess
import os
import ast
from sklearn.cluster import DBSCAN
from env import *

from cStringIO import StringIO

class MultiReaper():

    def __init__(self, reaper_list):
        self.rlist = reaper_list


    def get_events_list(self):
        event_dict = dict()
        unix2reaper = dict()
        for reaper in self.rlist:
            path = reaper.get_output_dir_event() + reaper.events_brief_fname
            f = open(path, 'r')
            for line in f:
                line = line.rstrip('\n')
                unix_dt = int(line.split(':')[0])
                content = line.split(':')[1]
                thelist = ast.literal_eval(content)
                rsize = thelist[0]
                if rsize < global_rsize_threshold:
                    continue
                event_dict[unix_dt] = thelist
                unix2reaper[unix_dt] = reaper
            f.close()

        return (event_dict, unix2reaper)

    def all_events_cluster(self):
        pfx_set_dict = dict()
        mon_set_dict = dict()

        event_dict, unix2reaper = self.get_events_list()
        for unix_dt in event_dict:
            reaper = unix2reaper[unix_dt]
            #---------------------------------------------
            # obtain the prefix and monitor(index) sets of the event
            pfx_set = set()
            mon_set = set()

            event_fpath = reaper.get_output_dir_event() + str(unix_dt) + '.txt'
            f = open(event_fpath, 'r')
            for line in f:
                line = line.rstrip('\n')
                if line.startswith('Mo'):
                    mon_set = ast.literal_eval(line.split('set')[1])
                    mon_set = set(mon_set)
                else:
                    pfx_set.add(line.split(':')[0])
            f.close()

            pfx_set_dict[unix_dt] = pfx_set
            mon_set_dict[unix_dt] = mon_set

        # obtain the jaccard distance between these events
        d_matrix = list()
        unix_dt_list = sorted(event_dict.keys()) # sorted list

        for unix_dt in unix_dt_list:
            the_list = list()
            for unix_dt2 in unix_dt_list:
                pset1 = pfx_set_dict[unix_dt]
                pset2 = pfx_set_dict[unix_dt2]
                JD_p = 1 - float(len(pset1&pset2)) / float(len(pset1|pset2)) # jaccard distance

                mset1 = mon_set_dict[unix_dt]
                mset2 = mon_set_dict[unix_dt2]
                JD_m = 1 - float(len(mset1&mset2)) / float(len(mset1|mset2))

                #JD = 0.9 * JD_p + 0.1 * JD_m # XXX note the parameters
                JD = JD_p

                #the_list.append(JD_p)
                the_list.append(JD)

            d_matrix.append(the_list)


        the_ndarray = np.array(d_matrix)

        print unix_dt_list
        print d_matrix
        db = DBSCAN(eps=0.8, min_samples=3, metric='precomputed').fit(the_ndarray)
        print db.core_sample_indices_
        print db.components_
        print db.labels_

        outpath = self.events_cluster_path()
        f = open(outpath, 'w')
        for label in db.labels_:
            f.write(str(label)+'|')
        f.close()

    def events_cluster_path(self):
        return  cmlib.datadir+'final_output/clustering.txt'
