from analyzer import *
from getfile import daterange

for i in range(len(daterange)-1, len(daterange)):
    filelist = 'metadata/' + daterange[i][0] + '/updt_filelist_comb'
    ana = Analyzer(filelist, 10, daterange[i][0], 1)  # Granularity needed
    ana.parse_update()
