import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt 
import matplotlib.dates as mpldates
import subprocess
import ast

from matplotlib.dates import HourLocator, DayLocator
from matplotlib.patches import Ellipse, Rectangle
from matplotlib import cm
from matplotlib import ticker
from matplotlib.colors import LogNorm
from cStringIO import StringIO

def plot_matrix(floc, saveloc):
    p = subprocess.Popen(['zcat', floc],stdout=subprocess.PIPE)
    f = StringIO(p.communicate()[0])
    assert p.returncode == 0

    lists = list()
    dvs = list()
    high = 0

    for line in f:
        line = line.rstrip('\n')
        if line == '':
            continue
        data = line.split(':')[1]
        data = ast.literal_eval(data) 
        
        count = 0
        for d in data:
            if d > 0:
                count += 1

        if count > 2: # we do not plot dv that is too small
            dvs.append(count)
            lists.append(data)
            now_max = max(data)
            if now_max > high:
                high = now_max

    f.close()


    lists = [x for (y,x) in sorted(zip(dvs, lists))]
    pfx_quantity = len(lists)

    fig = plt.figure(figsize=(10,16))
    ax = fig.add_subplot(111)
    cax = ax.imshow(lists, interpolation='nearest', aspect='auto', cmap=cm.jet, norm=LogNorm(1,high))
    lvls = np.logspace(0, 3, 10)
    cbar = fig.colorbar(cax, ticks=lvls)

    yticklist = []
    count = 0
    while True:
        if pfx_quantity/200 >= 1:
            count += 1
            yticklist.append(count*200)
            pfx_quantity -= 200
        else:
            #yticklist.append(count*200+pfx_quantity)
            break

    plt.yticks(yticklist)
    plt.savefig(saveloc, bbox_inches='tight')
    plt.clf() # clear the figure
    plt.close()

#if __name__ == '__main__':
#    plot_matrix('/media/usb/output/20141130_20141201/1417420800.txt.gz')
