from analyzer import *
from env import *
from alarm_class import *
import logging
from meliae import scanner
scanner.dump_all_objects('memory.json')

logging.basicConfig(filename='status.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')

#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
#cmlib.combine_cdf()

#dthres = 0.005785
granu = 10 # granularity (in minutes)

logging.info('Program starts!')

for i in [0]:

    filelist = datadir+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    soccur = daterange[i][6] # event occur start
    eoccur = daterange[i][7] # event occur end
    des = daterange[i][8] # event description

    try:
        peak = daterange[i][9] # the occurrence of HDVP peak
    except:
        peak = None

    #alarmplot(daterange[i][0], granu) # Directly plot from existent data
    # If no existent data, manually uncomment the following 5 lines
    ana = Analyzer(filelist, granu, daterange[i][0], peak)
    ana.parse_updates()

    ''' This is the ideal way, totally automatic
    try:
        alarmplot(daterange[i][0], granu)
    except:
        ana = Analyzer(filelist, granu, daterange[i][0], peak)

        ana.parse_updates()
    '''

logging.info('Program ends!')
