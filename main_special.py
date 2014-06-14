from analyzer import *
from getfile import daterange

for i in range(0, 1):
    filelist = 'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    ana = Analyzer(filelist, 10, daterange[i][0], 1)  # Granularity needed
    ana.parse_update()
