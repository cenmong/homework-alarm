from analyzer import *
from env import *
from alarm_class import *

#TEST = False
TEST = True


#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
#cmlib.combine_cdf()
#dthres = 0.005785
dthres = 0.002
#for i in xrange(0,len(daterange)):
for i in [16,25,1,9,10]:

    if TEST: # shorter list (some events, e.g., 0)
        filelist = hdname+'metadata/' + daterange[i][0] + '/test_updt_filelist_comb'
    else:
        filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    soccur = daterange[i][6] # event occur start
    eoccur = daterange[i][7] # event occur end
    des = daterange[i][8] # event description

    try:
        peak = daterange[i][9] # the occurrence of HDVP peak
    except:
        peak = None

    alarmplot(daterange[i][0], 10) # Directly plot from existent data
    # If no existent data, manually uncomment the following 5 lines
    #if TEST:
        #ana = Analyzer(filelist, 10, daterange[i][0], dthres, '2006-12-25 00:40:00')
    #else:
        #ana = Analyzer(filelist, 10, daterange[i][0], dthres, peak)
    #ana.parse_updates()

    ''' This is the ideal way, totally automatic
    try:
        alarmplot(daterange[i][0], 10)
    except:
        if TEST:
            ana = Analyzer(filelist, 10, daterange[i][0], dthres, '2006-12-25 00:40:00')
        else:
            ana = Analyzer(filelist, 10, daterange[i][0], dthres, peak)

        ana.parse_updates()

        pass
    '''
