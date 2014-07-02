from analyzer import *
from env import *

for i in range(1, 2):
    filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    ana = Analyzer(filelist, 10, daterange[i][0], 0.2, 1)  # Granularity needed
    ana.parse_update()
