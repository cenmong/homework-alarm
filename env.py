import os

hdname = ''
if os.path.isdir('/media/cm/'):
    hdname = '/media/cm/4F4D-9698/'
elif os.path.isdir('/media/sxr/'):
    hdname = '/media/sxr/MyBook/'
else:
    pass


# 0: routeviews; 1: ripe ris
collectors = [('', 0, '20011101'), ('rrc00', 1, '19991101'), ('rrc01', 1,\
            '20000801'), ('rrc03', 1, '20010201'),\
             ('rrc04', 1, '20010501'), ('rrc05', 1, '20010701'), ('rrc06', 1,\
                     '20010901'), ('rrc07', 1, '20020501'),\
             ]

# number of days in total
daterange = [('20061225', 4, '2006 taiwan cable cut'),\
            ('20081218', 4, '2008 mediterranean cable cut 2'),\
            ('20050911', 4, 'LA blackout'),\
            ('20050828', 4, 'Hurricane Katrina'),\
            ]
