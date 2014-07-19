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
daterange = [('20061225', 4, 168, '2006 taiwan cable cut', 0, 11),\
            ('20081218', 4, -1, '2008 mediterranean cable cut 2', 1, 11),\
            ('20030813', 4, -1, '2003 east coast blackout', 2, 11),\
            ('20050911', 4, -1, '2005 LA blackout', 3, 11),\
            ('20050828', 4, -1, '2005 Hurricane Katrina', 4, 11),\
            ('20080129', 4, -1, '2008 mediterranean cable cut 1', 5, 11),\
            ('20100226', 4, -1, '2010 Chile earthquake', 6, 11),\
            ('20110310', 4, -1, '2011 Japan Tsunami', 7, 10),\
            ('20121021', 4, -1, '2012 Hurricane Sandy', 8, 10),\
            ('20130317', 4, -1, '2013 Spamhaus DDoS', 9, 10),\
            ('20140601', 7, 188, 'for CDF in intro 2014', 10, 01),\
            ('20060601', 7, 154, 'for CDF in intro 2006', 11, 00),\
            ('20130207', 4, -1, '2013 Northeastern U.S. Blackout', 12, 01),\
            ('20100413', 4, -1, '2010 Sea-Me undersea cable cut', 13, 00),\
            ('20120221', 4, -1, 'Australia route leakage', 14, 00),\
            ('20120807', 4, -1, 'Canada route leakage', 15, 00),\
            ('20030124', 4, -1, '2003 Slammer worm', 16, 10),\
            ('20130321', 4, -1, '20130322 EASSy/SEACOM Outages', 17, 10),\
            ('20130213', 4, -1, '20130214 SEACOM Outages', 18, 10),\
            ('20110327', 4, -1, '20110328 Caucasus cable cut', 19, 00),\
            ('20121222', 4, -1, '20121223 Georgia-Russia cable cut', 20, 00),\
            ('20120224', 4, -1, '20120225 0913 TEAMS cable cut in east Africa', 21, 00),\
            ('20120425', 4, -1, '20120426 0904 TEAMS cable cut again in east\
                    Africa', 22, 00),\
            ('20110824', 4, -1, '201108 hurricane Irene in east U.S.', 23, 00),\
            ]
