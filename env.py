import os
from os.path import expanduser

hdname = ''
if os.path.isdir('/media/cm/'):
    hdname = '/media/cm/4F4D-9698/'
elif os.path.isdir('/media/sxr/'):
    hdname = '/media/sxr/MyBook/'
else:
    pass

homedir = expanduser('~') + '/'

# 0: routeviews; 1: ripe ris
collectors = [('', 0, '20011101'), ('rrc00', 1, '19991101'), ('rrc01', 1,\
            '20000801'), ('rrc03', 1, '20010201'),\
             ('rrc04', 1, '20010501'), ('rrc05', 1, '20010701'), ('rrc06', 1,\
                     '20010901'), ('rrc07', 1, '20020501'),\
             ]

# number of days in total
daterange = [('20061225', 4, '2006 taiwan cable cut', 0),\
            ('20081218', 4, '2008 mediterranean cable cut 2', 1),\
            ('20030813', 4, '2003 east coast blackout', 2),\
            ('20050911', 4, 'LA blackout', 3),\
            ('20050828', 4, 'Hurricane Katrina', 4),\
            ('20080129', 4, '2008 mediterranean cable cut 1', 5),\
            ('20100226', 4, '2010 Chile earthquake', 6),\
            ('20110310', 4, '2011 Japan Tsunami', 7),\
            ('20121021', 4, '2012 Hurricane Sandy', 8),\
            ('20130317', 4, '2013 Spamhaus DDoS', 9),\
            ]
