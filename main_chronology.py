from analyzer import *

filelist = 'metadata/chronology_files'
ana = Analyzer(filelist, 10, 'chronology', 2)  # Granularity needed
ana.parse_update()
