from get_special_files import clean, ymd1
from env import hdname
import os

for i in range(0, len(ymd1)):
    if clean[i] == 0:
        continue

    filelist = open('metadata/files' + ymd1[i], 'r')
    for line in filelist.readlines():
        print line
        os.system('python clean.py ' + hdname + line)
