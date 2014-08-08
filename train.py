from estimator import *
from env import *
'''
em = Estimator(1)
em.add_norm(0)

value1 = em.get_likelihood(5)
value2 = em.get_log_likelihood(5)

print value1
print value2
'''
mylist = []

fname = hdname + 'output/20060601_10_0.3/20060601_10_0.3_dvi(1).txt'
f = open(fname,'r')
for line in f:
    value = float(line.split(',')[1])
    mylist.append(value)

mylist = mylist[:144]
    
emt = Estimator()
emt.train(mylist)
res = emt.get_likelihood(0.6)
print res
res = emt.get_log_likelihood(0.5785)
print res
