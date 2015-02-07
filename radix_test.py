import radix

rt = radix.Radix()
rt.add('1.1.1.0/24')
rnode = rt.add('1.1.1.0/24')

rnode.data['my'] = 1
rnode.data['your'] = 2

mynode = rt.search_exact('1.1.1.0/24')
for k in mynode.data.keys():
    print k
#print mynode.data['my']

i = mynode.data['ok']

for n in rt:
    print n.prefix
