from meliae import loader
om = loader.load('memory.json')
om.compute_parents()
om.collapse_instance_dicts()
s = om.summarize()
print s
'''
p = om.get_all('list')
print p[0]
print p[0].c
print p[0].p
'''
