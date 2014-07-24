from analyzer import *
from env import *

thres = 0.6
for i in [0,1,2]:
    filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    soccur = daterange[i][6] 
    eoccur = daterange[i][7] 

    ana = Analyzer(filelist, 10, daterange[i][0], 0.3, 1, thres, soccur, eoccur)  # Granularity needed
    if ana.direct():
        continue
    ana.parse_update()
