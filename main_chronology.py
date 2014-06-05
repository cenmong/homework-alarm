from analyzer import *

#filelist = 'metadata/test_chronology_files'
filelist = 'metadata/chronology_files.tmp'
ana = Analyzer(filelist, 10, 'chronology', 2)  # Granularity needed
ana.parse_update()
