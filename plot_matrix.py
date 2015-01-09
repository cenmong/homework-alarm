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

def plot_matrix(fname):
    tag = fname.split('/')[-1].split('.')[0]

    p = subprocess.Popen(['zcat', fname],stdout=subprocess.PIPE)
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
        now_max = max(data)
        if now_max > high:
            high = now_max
        lists.append(data)
        
        count = 0
        for d in data:
            if d > 0:
                count += 1
        dvs.append(count)

    f.close()

    lists = [x for (y,x) in sorted(zip(dvs, lists))]

    fig = plt.figure(figsize=(10,16))
    ax = fig.add_subplot(111)
    cax = ax.imshow(lists, interpolation='nearest', aspect='auto', cmap=cm.cool, norm=LogNorm(1,high))
    lvls = np.logspace(0, 3, 10)
    cbar = fig.colorbar(cax, ticks=lvls)
    #cbar.ax.set_yticklabels(['0', str(high)])

    plt.savefig(tag+'matrix.pdf', bbox_inches='tight')

if __name__ == '__main__':
    #matrixes = []
    #matrixes.append('/media/usb/output/20141130_20141201/1417308000.txt.gz') # add matrix file
    #for m in matrixes:
    #    plot_matrix(m)
    plot_matrix('/media/usb/output/20141130_20141201/1417372800.txt.gz')


