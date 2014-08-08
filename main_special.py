from analyzer import *
from env import *

#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
cmlib.combine_cdf()
thres = 0.5785
for i in xrange(0,len(daterange)):
#for i in [0]:
    filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    soccur = daterange[i][6] 
    eoccur = daterange[i][7] 
    des = daterange[i][8]

    ana = Analyzer(filelist, 10, daterange[i][0], 0.3, 1, thres, soccur,\
            eoccur, des)  # Granularity needed
    if ana.direct():
        continue
    #ana.parse_update()
