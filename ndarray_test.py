import numpy as np
import operator

mylist = [[1,0,0,0]]
mylist.append([1,1,0,1])
mylist.append([1,1,0,0])
mylist.append([0,1,0,1])
mylist.append([1,1,1,1])

mymatrix = np.array(mylist)

#print mymatrix
#print '-------------'
#mymatrix = np.delete(mymatrix,3,0)
#mymatrix = np.delete(mymatrix,[1,2,3],0)
#mymatrix = np.delete(mymatrix,3,1)
print mymatrix

sum = np.sum(mymatrix)
print 'sum all = ', sum
size = mymatrix.size
print 'size all = ', size
print 'shape all = ', mymatrix.shape
print 'sum of row 1 = ', mymatrix[1].sum()
print 'sum of column 2 = ', mymatrix[:,2].sum()
density = float(sum)/float(mymatrix.size)
print 'density = ', density
print '----------------'


#----------------------------------------------
# the thresholds
thre_den = 0.8
now_den = density

while(now_den<thre_den):
    #-------------------------
    sum = float(np.sum(mymatrix))
    size = float(mymatrix.size)
    density = sum/mymatrix.size

    #-----------------------------------------------
    # obtain candidate prefixes and monitors to delete
    pfx_1 = dict()
    mon_1 = dict()
    height = mymatrix.shape[0]
    width = mymatrix.shape[1]
    for i in xrange(0, height):
        pfx_1[i] = mymatrix[i].sum()
    for i in xrange(0, width):
        mon_1[i] = mymatrix[:,i].sum()

    pfx_1 = sorted(pfx_1.items(), key=operator.itemgetter(1))
    print pfx_1
    min_pfx_1 = pfx_1[0][1]
    candidate_row_del = list()
    for item in pfx_1:
        if item[1] == min_pfx_1:
            candidate_row_del.append(item[0])
        else:
            break

    mon_1 = sorted(mon_1.items(), key=operator.itemgetter(1))
    print mon_1
    min_mon_1 = mon_1[0][1]
    candidate_col_del = list()
    for item in mon_1:
        if item[1] == min_mon_1:
            candidate_col_del.append(item[0])
        else:
            break

    print 'del pfx candi:', candidate_row_del
    print 'del mon candi:', candidate_col_del
    #-------------------------------------------
    # decide which to delete
    pfx_dsize = float(len(candidate_row_del) * width)
    pfx_dsum = 0.0
    for index in candidate_row_del:
        pfx_dsum += mymatrix[index].sum()
    pfx_compare = ((sum-pfx_dsum)/(size-pfx_dsize)-density)/pfx_dsize

    mon_dsize = float(len(candidate_col_del) * height)
    mon_dsum = 0.0
    for index in candidate_col_del:
        mon_dsum += mymatrix[:,index].sum()
    mon_compare = ((sum-mon_dsum)/(size-mon_dsize)-density)/mon_dsize

    print pfx_compare,mon_compare
    if pfx_compare >= mon_compare:
        print 'delete row:', candidate_row_del
        mymatrix = np.delete(mymatrix,candidate_row_del,0)
        now_den = (sum-pfx_dsum)/(size-pfx_dsize)
    else:
        print 'delete col:', candidate_col_del
        mymatrix = np.delete(mymatrix,candidate_col_del,1)
        now_den = (sum-mon_dsum)/(size-mon_dsize)

print mymatrix
