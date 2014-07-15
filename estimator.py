import numpy as np
import scipy.stats
import math

class Estimator():

    def __init__(self):
        self.scale = 0
        self.loc = dict() # loc: exist count

    def train(self, my_list):
        if len(my_list) < 11:
            print 'training list length error'
            return

        # Get scale
        dist_list = []
        my_list = sorted(my_list)
        for i in xrange(0, len(my_list)):
            neighbors = self.get_10_nearest(my_list, i)
            dist = self.get_avg_dist(neighbors, my_list[i])
            dist_list.append(dist)
            
        print dist_list
        sum_dist = 0
        for d in dist_list:
            sum_dist += d
        self.scale = float(sum_dist)/float(len(dist_list))

        # Add norm functions
        for item in my_list:
            self.add_norm(item)

    def get_10_nearest(self, sorted_list, i):
        pre = i-1
        nex = i+1
        count = 0
        nbs = []

        while 1:
            if pre < 0:
                for j in xrange(0, 10-count):
                    nbs.append(sorted_list[nex])
                    nex += 1
                break
            if nex > len(sorted_list)-1:
                for j in xrange(0, 10-count):
                    nbs.append(sorted_list[pre])
                    pre -= 1
                break

            if abs(sorted_list[i]-sorted_list[pre]) >=\
                    abs(sorted_list[i]-sorted_list[nex]):
                nbs.append(sorted_list[nex])
                nex += 1
                count += 1
            else:
                nbs.append(sorted_list[pre])
                pre -= 1
                count += 1

            if count == 10:
                break

        return nbs

    def get_avg_dist(self, nbs, item):
        sum_dist = 0
        for nb in nbs:
            sum_dist += abs(nb-item)

        return float(sum_dist)/float(len(nbs))

    def add_norm(self, my_loc):
        try:
            self.loc[my_loc] += 1
        except:
            self.loc[my_loc] = 1

    def get_likelihood(self, x):
        value = 0
        for key in self.loc.keys():
            value += self.loc[key] * scipy.stats.norm.pdf(x, loc=key,\
                    scale=self.scale)
        return value

    def get_log_likelihood(self, x):
        value = 0
        for key in self.loc.keys():
            value += self.loc[key] * scipy.stats.norm.pdf(x, loc=key,\
                    scale=self.scale)
        return math.log10(value)
