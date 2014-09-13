from analyzer import *
from env import *

#TEST = False
TEST = True


#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
#cmlib.combine_cdf()
#dthres = 0.005785
dthres = 0.002
#for i in xrange(0,len(daterange)):
for i in [0]: # change this list according to needs

    if TEST:
        filelist = hdname+'metadata/' + daterange[i][0] + '/test_updt_filelist_comb'
    else:
        filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    soccur = daterange[i][6] # event occur start
    eoccur = daterange[i][7] # event occur end
    des = daterange[i][8] # event description

    ana = Analyzer(filelist, 10, daterange[i][0], 0.1, dthres)

    # plot directly from existent data
    #if ana.direct(thres, soccur, eoccur, des):
    #    continue
    ana.parse_updates()
