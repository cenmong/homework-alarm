from analyzer import *
from env import *

thres = 0.6
for i in xrange(0,10):
    filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    soccur = daterange[i][6] 
    eoccur = daterange[i][7] 
    des = daterange[i][8]

    ana = Analyzer(filelist, 10, daterange[i][0], 0.3, 1, thres, soccur,\
            eoccur, des)  # Granularity needed
    if ana.direct():
        continue
    ana.parse_update()
