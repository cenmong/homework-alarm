#from meliae import scanner
#scanner.dump_all_objects('memory.json')

from guppy import hpy
#import objgraph
#import psutil
#from memprof import *

#@profile
#@memprof
def my_func():
    a = list()
    for i in xrange(0,10000000):
        a.append('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    h = hpy()
    print h.heap()

#objgraph.show_refs(a, 'memo-graph.png')

if __name__=='__main__':
    my_func()


