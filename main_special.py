from analyzer import *
from get_special_files import ymd1

#filelist = 'metadata/test-files20050828'
#filelist = 'metadata/hk-localtest'
for i in range(len(ymd1)-1, len(ymd1)):
#for i in [17, 1, 2, 3, 4, 5, 6, 7]:
    filelist = 'metadata/files' + ymd1[i]
    ana = Analyzer(filelist, 10, ymd1[i], 1)  # Granularity needed
    ana.parse_update()
'''    
ana = Analyzer(filelist, 10, 'TEST')  # Granularity needed
ana.parse_update()
'''
