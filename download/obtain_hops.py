""" Johnson's algorithm for all-pairs shortest path problem.
Reimplemented Bellman-Ford and Dijkstra's for clarity"""
from heapq import heappush, heappop
from datetime import datetime
from copy import deepcopy

from env import *
import os
import urllib
import subprocess
import nltk

graph = { 
    'a' : {'b':-2},
    'b' : {'c':-1},
    'c' : {'x':2, 'a':4, 'y':-3},
    'z' : {'x':1, 'y':-4},
    'x' : {},
    'y' : {},
}

inf = float('inf')
dist = {}
#mydist = {}
myQ = []

def read_graph(file,n):
    graph = dict()
    with open(file) as f:
        for l in f:
            (u, v, w) = l.split()
            if int(u) not in graph:
                graph[int(u)] = dict()
            graph[int(u)][int(v)] = int(w)
    for i in range(n): # Why we need this?
        if i not in graph:
            graph[i] = dict()
    return graph

def dijkstra(graph, s, mydist):
    #n = len(graph.keys())
    #mydist = {}
    #print '2:',mydist
    #myQ = []
    
    for v in graph:
        mydist[v] = inf
    mydist[s] = 0
    
    heappush(myQ, (mydist[s], s))

    while myQ:
        d, u = heappop(myQ)
        if d < mydist[u]:
            mydist[u] = d
        for v in graph[u]:
            if mydist[v] > mydist[u] + graph[u][v]:
                mydist[v] = mydist[u] + graph[u][v]
                heappush(myQ, (mydist[v], v))
    #print '3:',mydist
    return mydist

def initialize_single_source(graph, s):
    for v in graph:
        dist[v] = inf
    dist[s] = 0
    
def relax(graph, u, v):
    if dist[v] > dist[u] + graph[u][v]:
        dist[v] = dist[u] + graph[u][v]

def bellman_ford(graph, s):
    initialize_single_source(graph, s)
    edges = [(u, v) for u in graph for v in graph[u].keys()]
    number_vertices = len(graph)
    for i in range(number_vertices-1):
        for (u, v) in edges:
            relax(graph, u, v)
    for (u, v) in edges:
        if dist[v] > dist[u] + graph[u][v]:
            return False # there exists a negative cycle
    return True

def add_extra_node(graph):
    graph[0] = dict()
    for v in graph.keys():
        if v != 0:
            graph[0][v] = 0

def reweighting(graph_new):
    add_extra_node(graph_new)
    if not bellman_ford(graph_new, 0):
        # graph contains negative cycles
        return False
    for u in graph_new:
        for v in graph_new[u]:
            if u != 0:
                graph_new[u][v] += dist[u] - dist[v]
    del graph_new[0]
    return graph_new

def johnsons(graph_new):
    print 'reweighting...'
    #graph = reweighting(graph_new)
    if not graph:
        return False
    final_distances = {}
    length = len(graph)
    count = 0
    for u in graph:
        count += 1
        print count,'/',length,':',u
        mydist={}
        #print '1:',mydist
        myQ=[]
        final_distances[u] = dijkstra(graph, u, mydist)
        print len(final_distances)
        del myQ
        del mydist

    for u in final_distances:
        for v in final_distances[u]:
            final_distances[u][v] += dist[v] - dist[u]
    return final_distances
            
if __name__ == "__main__":

    for i in range(0, 1):  # loop over events
        location = hdname+'as_hops/'+daterange[i][0]+'/'
        # mkdir if not exist
        if not os.path.isdir(location):
            os.makedirs(location)

        # get AS link files if not exist -- control plane
        print 'Downloading control plane links...'
        cp_fname = daterange[i][0][:6] + '.link.v4'  # YYYYMM
        if not os.path.exists(location+cp_fname):
            urllib.urlretrieve('http://irl.cs.ucla.edu/topology/ipv4/monthly/'+\
                    cp_fname+'.gz', location+cp_fname+'.gz')
            subprocess.call('gunzip -c '+location+cp_fname+'.gz > '+\
                    location+cp_fname, shell=True)
            os.remove(location+cp_fname+'.gz')

        # get AS link files if not exist -- data plane
        print 'Downloading data plane links...'
        dp_fname = 'skitter_as_links.'+daterange[i][0]
        if int(daterange[i][0][:6]) < 200710: # skitter data
            if not os.path.exists(location+dp_fname):
                year = daterange[i][0][:4] # YYYY
                month = daterange[i][0][4:6] # MM
                urllib.urlretrieve('http://data.caida.org/datasets/topology/skitter/as-links/'+year+\
                        '/'+month+'/'+dp_fname+'.gz', location+dp_fname+'.gz')
                subprocess.call('gunzip -c '+location+dp_fname+'.gz > '+\
                        location+dp_fname, shell=True)
                os.remove(location+dp_fname+'.gz')
        else: # ark data from 3 teams
            pass # TODO

        # create graph from these AS link files
        print 'creating graph from these AS link files...'
        graph = {} # ASN: {ASN, weight}. Directed Graph!
        cpf = open(location+cp_fname, 'r')
        for line in cpf:
            as1 = int(line.split()[0])
            as2 = int(line.split()[1])
            if as1 not in graph:
                graph[as1] = dict()
            graph[as1][as2] = 1
            if as2 not in graph:
                graph[as2] = dict()
            graph[as2][as1] = 1
        cpf.close()

        dpf = open(location+dp_fname, 'r')
        for line in dpf:
            if line[0] in ['#', 'T', 'M', 'I'] or '_' in line or '{' in line or\
                    ',' in line:
                continue
            try:
                as1 = int(line.split()[1])
                as2 = int(line.split()[2])
                if as1 not in graph:
                    graph[as1] = dict()
                graph[as1][as2] = 1
                if as2 not in graph:
                    graph[as2] = dict()
                graph[as2][as1] = 1
            except:
                continue
        dpf.close()

        # get pfx2as file by the way (only after 200506)
        print 'get pfx2as file by the way (only after 200506)...'
        year = daterange[i][0][:4] # YYYY
        month = daterange[i][0][4:6] # MM
        webloc = 'http://data.caida.org/datasets/routing/routeviews-prefix2as' +\
                        '/' + year + '/' + month + '/'
        webhtml = urllib.urlopen(webloc).read()
        webraw = nltk.clean_html(webhtml)
        for line in webraw.split('\n'):
            if not daterange[i][0] in line:
                continue
            if os.path.exists(hdname+location+line.split()[0].replace('.gz',\
                        '')):
                break
            urllib.urlretrieve(webloc+line.split()[0], location+line.split()[0])
            subprocess.call('gunzip -c '+location+line.split()[0]+' > '+\
                    location+line.split()[0].replace('.gz', ''), shell=True)
            os.remove(location+line.split()[0])

        print 'running johnson ...'
        # graph = read_graph("graph.txt", 1000)
        graph_new = deepcopy(graph)
        t1 = datetime.utcnow()
        final_distances =  johnsons(graph_new)
        if not final_distances:
            print "Negative cycle"
        # write the result into a new file
        '''
        resf = open(location+'as_hops', 'w')
        for i in final_distances:
            for j in final_distances[i]:
                resf.write(str(i)+' '+str(j)+' '+str(final_distances[i][j])+'\n')
        resf.close()
        '''
        for i in final_distances:
            for j in final_distances[i]:
                print str(i)+' '+str(j)+' '+str(final_distances[i][j])
        print datetime.utcnow() - t1
