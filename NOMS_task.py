mons = set()
mon2pathlens = dict()

f = open('120_28_62_0_24.txt', 'r')
for line in f:
    attr = line.split('|')
    mon = attr[3]
    aspath = attr[6]

    mons.add(mon)
    length = len(aspath.split())
    mon2pathlens[mon] = length
f.close()

print len(mons)
print mon2pathlens



mon2mingap = dict()
mon2cunix = dict()
for mon in mons:
    mon2mingap[mon] = 999999999
    mon2cunix[mon] = 0

f = open('120_28_62_0_24.txt', 'r')
for line in f:
    attr = line.split('|')
    unix = int(attr[1])
    mon = attr[3]

    pre_unix = mon2cunix[mon] 
    gap = unix - pre_unix
    if gap < mon2mingap[mon]:
        mon2mingap[mon] = gap
    mon2cunix[mon] = unix

f.close()

print mon2mingap



