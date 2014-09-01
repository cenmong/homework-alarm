from analyzer import *
from env import *

TEST = False
#TEST = True


#cmlib.combine_slot_dvi()
#cmlib.combine_ht()
#cmlib.combine_cdf()
thres = 0.5785
#for i in xrange(0,len(daterange)):
for i in [1]: # change this list according to needs

    # TODO: download supporting files using a supporter object
    if TEST:
        filelist = hdname+'metadata/' + daterange[i][0] + '/test_updt_filelist_comb'
    else:
        filelist = hdname+'metadata/' + daterange[i][0] + '/updt_filelist_comb'

    soccur = daterange[i][6] # event occur start
    eoccur = daterange[i][7] # event occur end
    des = daterange[i][8] # event description

    # 2nd parameter: length of time slot
    # 4th parameter: HDVP threshold
    # 6th parameter: detection threshold
    ana = Analyzer(filelist, 10, daterange[i][0], 0.3, 1, thres, soccur, eoccur, des)

    # plot directly from existent data
    #if ana.direct():
    #    continue
    ana.parse_updates()
