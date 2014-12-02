from analyzer import *
from env import *
from alarm_class import *

granu = 10 # granularity (in minutes)

for i in [0]:

    filelist = hdname+'metadata/' + daterange[i][0] + '/test_updt_filelist_comb'

    soccur = daterange[i][6] # event occur start
    eoccur = daterange[i][7] # event occur end
    des = daterange[i][8] # event description

    try:
        peak = daterange[i][9] # the occurrence of HDVP peak
    except:
        peak = None

    #alarmplot(daterange[i][0], granu) # Directly plot from existent data

    # If no existent data, manually uncomment the following 5 lines
    ana = Analyzer(filelist, granu, daterange[i][0], '2006-12-25 00:40:00')
    ana.parse_updates()
