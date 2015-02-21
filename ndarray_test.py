# future TODO record real prefixes and monitors for the resulted block
import numpy as np
import operator
import logging

#--------------------------------
# TODO dynamically obtain thresholds
thre_size = 12
logging.info('size threshold is %d', thre_size)
thre_width = 3
logging.info('width threshold is %d', thre_width)
thre_row_den = XXX
logging.info('row density threshold is %f', thre_row_den)
thre_col_den = XXX
logging.info('col density threshold is %f', thre_col_den)

mylist = [[1,0,0,0]]
mylist.append([1,1,0,1])
mylist.append([1,1,0,0])
mylist.append([0,1,0,1])
mylist.append([1,1,1,1])

self.bmatrix = np.array(mylist)
size = self.bmatrix.size
width = self.bmatrix.shape[1]
if size < thre_size or width < thre_width:
    print 'Stop!'
    # TODO stop; next interval

print '------preprocessing-------'
height = self.bmatrix.shape[0]
width = self.bmatrix.shape[1]
min_row_sum = 1 # TODO according to density thresholds
min_col_sum = 1 # TODO
row_must_del = []
col_must_del = []
for i in xrange(0, height):
    if self.bmatrix[i].sum() <= min_row_sum:
        row_must_del.append(i)
for i in xrange(0, width):
    if self.bmatrix[:,i].sum() <= min_col_sum:
        col_must_del.append(i)

self.bmatrix = np.delete(self.bmatrix,row_must_del,0)
self.bmatrix = np.delete(self.bmatrix,col_must_del,1)

#---------------------
# Decide whether stop here!
size = self.bmatrix.size
width = self.bmatrix.shape[1]
if size < thre_size or width < thre_width:
    print 'Stop!'
    # TODO stop; next interval

sum = np.sum(self.bmatrix)
density = float(sum)/float(size)

print '------processing-------'
#----------------------------------------------
# the thresholds
thre_den = 0.85 # XXX Is it good?
now_den = density
no_more_col_del = False

while(now_den<thre_den):
    #-------------------------
    sum = float(np.sum(self.bmatrix))
    size = float(self.bmatrix.size)
    density = sum/self.bmatrix.size
    height = self.bmatrix.shape[0]
    width = self.bmatrix.shape[1]

    #-----------------------------------------------
    # obtain candidate prefixes to delete
    row_one = dict()
    for i in xrange(0, height):
        row_one[i] = self.bmatrix[i].sum()

    row_one = sorted(row_one.items(), key=operator.itemgetter(1))
    min_row_one = row_one[0][1]

    row_to_del = list()
    for item in row_one:
        if item[1] == min_row_one:
            row_to_del.append(item[0])
        else:
            break

    row_dsize = float(len(row_to_del) * width)
    row_dsum = 0.0
    for index in row_to_del:
        row_dsum += self.bmatrix[index].sum()
    row_del_score = ((sum-row_dsum)/(size-row_dsize)-density)/row_dsize

    new_rsize = size - len(row_to_del) * width
    if new_rsize < thre_size:
        row_del_score = -1

    #-----------------------------------------------
    # obtain candidate monitors to delete
    if no_more_col_del is False:
        col_one = dict()
        for i in xrange(0, width):
            col_one[i] = self.bmatrix[:,i].sum()

        col_one = sorted(col_one.items(), key=operator.itemgetter(1))
        min_col_one = col_one[0][1]

        col_to_del = list()
        for item in col_one:
            if item[1] == min_col_one:
                col_to_del.append(item[0])
            else:
                break

        col_dsize = float(len(col_to_del) * height)
        col_dsum = 0.0
        for index in col_to_del:
            col_dsum += self.bmatrix[:,index].sum()
        col_del_score = ((sum-col_dsum)/(size-col_dsize)-density)/col_dsize

        new_width = width - len(col_to_del)
        if new_width < thre_width:
            col_del_score = -1 # never del col any more
            no_more_col_del = True

        new_csize = size - len(col_to_del) * height
        if new_csize < thre_size:
            col_del_score = -1
    else:
        col_del_score = -1

    #-------------------------------------------
    # decide which to delete
    if row_del_score == -1 and col_del_score == -1:
        print 'Fail to find such submatrix'
        break
    elif col_del_score == -1 or row_del_score >= col_del_score:
        print 'deleted row:', row_to_del
        self.bmatrix = np.delete(self.bmatrix,row_to_del,0)
        now_den = (sum-row_dsum)/(size-row_dsize)
    else:
        print 'deleted col:', col_to_del
        self.bmatrix = np.delete(self.bmatrix,col_to_del,1)
        now_den = (sum-col_dsum)/(size-col_dsize)

print '------submatrix-------'
print self.bmatrix
sum = np.sum(self.bmatrix)
print 'sum all = ', sum
size = self.bmatrix.size
print 'size all = ', size
print 'shape all = ', self.bmatrix.shape
print 'sum of row 1 = ', self.bmatrix[1].sum()
print 'sum of column 2 = ', self.bmatrix[:,2].sum()
density = float(sum)/float(self.bmatrix.size)
print 'density = ', density
