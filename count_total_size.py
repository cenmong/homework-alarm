from env import *
from period_class import *

index_list = [11]

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

    print total/1000000.0,'M'
