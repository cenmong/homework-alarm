from env import *
from period_class import *

#index_list = [0,1,3,4,5,6,7,8,10,11,13,16,19,20,21,22]
#index_list = [281,282,283,284,285,286,287,288,289,2810]
index_list = [285,286,287,288,289,2810]

sum = 0
for i in index_list:
    total = 0
    my_period = Period(i)
    my_period.get_global_monitors() # decide my_period.monitors
    my_period.rm_dup_mo() # rm multiple existence of the same monitor
    my_period.mo_filter_same_as()
    flist = my_period.get_filelist()
    f = open(flist,'r')
    for line in f:
        line = line.rstrip('\n')
        size = float(line.split('|')[1])
        total += size
    f.close()
    
    sum += total/1000000.0
    print i,':',total/1000000.0,'M'

print 'sum:',sum
