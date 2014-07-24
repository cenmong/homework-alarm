from analyzer import *
from env import *

thres = 0.3
for i in [12, 20]:
    filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    ana = Analyzer(filelist, 10, daterange[i][0], 0.3, 1, thres, soccur, eoccur)  # Granularity needed
    if ana.direct():
        continue
    ana.parse_update()
