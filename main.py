from analyzer import *
from get_special_files import ymd1

#filelist = 'metadata/test-files20050828'
#filelist = 'metadata/hk-localtest'
for i in range(len(ymd1)-6, len(ymd1)):
    filelist = 'metadata/files' + ymd1[i]
    ana = Analyzer(filelist, 10, ymd1[i])  # Granularity needed
    ana.parse_update()
'''    
ana = Analyzer(filelist, 10, 'TEST')  # Granularity needed
ana.parse_update()
'''
