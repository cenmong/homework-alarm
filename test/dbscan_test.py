from sklearn.cluster import DBSCAN

X = [[0,1,10],[1,0,15],[10,15,0]]

db = DBSCAN(eps=1, min_samples=1, metric='precomputed').fit(X)

print db.labels_

list = [1,2,3]
for i in list:
    for j in list:
        print i
        print j
