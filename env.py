from os.path import expanduser

datadir = '/media/usb/'
homedir = expanduser('~') + '/'

rv_root = 'archive.routeviews.org/' # routeviews.org does not support v6
rrc_root = 'data.ris.ripe.net/'

pub_spt_dir = datadir + 'support/public/'
rib_info_dir = datadir+'rib_info/'

all_collectors = {
    '': '20011101', 
    'route-views4': '20081201',
    'route-views.eqix': '20040601',
    'route-views.isc': '20031201',
    #'route-views.kixp': '20051101', # RIB size too small
    'route-views.linx': '20040401',
    'route-views.wide': '20030801',
    'rrc00': '19991101', 
    'rrc01': '20000801', 
    'rrc03': '20010201',
    'rrc04': '20010501', 
    'rrc05': '20010701', 
    #'rrc06': '20010901', # too few peers with global table
    #'rrc07': '20020501', # too few peers with global table
    'rrc10': '20031201',
    'rrc11': '20040301',
    'rrc12': '20040801',
    'rrc13': '20050501',
    'rrc14': '20050101',
    'rrc15': '20060101',
}

daterange = {
    0:('20061224','20061230'), #taiwan cable cut | SG
    1:('20081217','20081223'), #mediterranean cable cut 2 | SG
    2:('20030812','20030818'), #east coast blackout | Fail to parse one file!
    3:('20050910','20050916'), #LA blackout | SG
    4:('20050823','20050905'), #Hurricane Katrina | SG
    5:('20080128','20080203'), #mediterranean cable cut 1 | SG
    6:('20100225','20100303'), #Chile earthquake | SG
    7:('20110309','20110315'), #Japan Tsunami |
    8:('20121020','20121102'), #Hurricane Sandy |
    9:('20130317','20130320'), # Spamhaus DDoS attack | WD TODO longer XXX downloading in WD
    10:('20080510','20080513'), #Sichuan Earthquake | SG
    11:('20110824','20110827'), #Hurricane Irene | both
    12:('20130207','20130210'), #Northeastern US blackout | SG XXX downloading in WD
    13:('20100413','20100416'), #Sea-Me undersea cable cut | both
    14:('20120221','20120224'), #Australia route leakage | WD
    15:('20120807','20120810'), #Canada route leakage | WD
    16:('20030124','20030127'), #Slammer worm |WD
    17:('20130321','20130324'), #EASSy/SEACOM outages | WD XXX downloading in WD
    18:('20130213','20130216'), #SEACOM outage | WD XXX downloading in WD
    19:('20110327','20110330'), #Caucasus cable cut | WD
    20:('20121222','20121225'), #Georgia-Russia cable cut | SG
    21:('20120224','20120227'), #TEAMS cable cut in east Africa | SG
    22:('20120425','20120428'), #TEAMS cable cut in east Africa again | SG
    27:('20141130','20141201'), # test only
    28:('20130101','20131231'), # XXX downloading in WD
}

# [4th parm] order (for easier coding),[5th parm] 10:westdata HDD only 01:seagate HDD only 11: both 00: none
# [6th parm] event start date time,[7th parm] (opt) event end date time
# [8th parm] event discription (for plotting),[9th parm] curve peak date time (for plotting)
old_daterange = [('20061225', 4, 177, '2006 taiwan cable cut', 0, 11,\
                '2006-12-26 12:25:00', '', 'Earthquake\nhappened'),
            ('20081218', 4, 181, '2008 mediterranean cable cut 2', 1, 11,\
                '2008-12-19 07:28:00', '', 'First\ncable\ncut', '2008-12-19 07:30:00'),
            ('20030813', 4, 113, '2003 east coast blackout', 2, 11,\
                '2003-08-14 20:10:39', '2003-08-15 03:00:00', 'Blackouts\nduration'),
            ('20050911', 4, 135, '2005 LA blackout', 3, 11,\
                '2005-09-12 20:00:00', '', 'Blackouts\nbegan'),
            ('20050828', 4, 166, '2005 Hurricane Katrina', 4, 11,\
                '2005-08-28 18:00:00', '', 'Reached\npeak\nstrength'),
            ('20080129', 4, 156, '2008 mediterranean cable cut 1', 5, 11,\
                '2008-01-30 04:30:00', '', 'Cable\ncut'),
            ('20100226', 4, 177, '2010 Chile earthquake', 6, 11,\
                '2010-02-27 06:34:00', '', 'Earthquake\nhappened'),
            ('20110310', 4, 179, '2011 Japan Tsunami', 7, 10,\
                '2011-03-11 05:46:00', '', 'Tsunami\nhappened'),
            ('20121021', 4, 173, '2012 Hurricane Sandy', 8, 10,\
                '2012-10-24 19:00:00', '', 'Landfall\non\nJamaica'),
            ('20130317', 4, 190, '2013 Spamhaus DDoS', 9, 10,\
                '2013-03-18 00:00:00', '2013-03-19 00:00:00', 'Attack\nbegan', '2013-03-20 09:00:00'),
            ('20140601', 14, 186, 'Reference 2014', 10, 01, '', '', ''),
            ('20060601', 7, 152, 'for CDF in intro 2006', 11, 01, '', '', ''),
            ('20130207', 4, 191, '2013 Northeastern U.S. Blackout', 12, 01,\
                '2013-02-08 21:15:00', '2013-02-09 23:59:59', 'Several\nregions\nblackouts'),
            ('20100413', 4, 180, '2010 Sea-Me undersea cable cut', 13, 11, '', '', ''),
            ('20120221', 4, -1, 'Australia route leakage', 14, 10, '', '', ''),
            ('20120807', 4, -1, 'Canada route leakage', 15, 10, '', '', ''),
            ('20030124', 4, 168, '2003 Slammer worm', 16, 10,\
                '2003-01-25 05:30:00', '', 'Worm\nstarted', '2003-01-25 05:30:00'),
            ('20130321', 4, 185, '20130322 EASSy/SEACOM Outages', 17, 10,\
                '', '', ''),
            ('20130213', 4, 192, '20130214 SEACOM Outages', 18, 10,\
                '2013-02-14 11:59:00', '', 'Outage\nconfirmed'),
            ('20110327', 4, 179, '20110328 Caucasus cable cut', 19, 10,\
                '2011-03-28 13:00:00', '', 'Cable\ncut'),
            ('20121222', 4, 177, '20121223 Georgia-Russia cable cut', 20, 01,\
                '2012-12-23 00:00:00', '2012-12-23 23:59:59', 'Cable\ncut'),
            ('20120224', 4, -1, '20120225 0913 TEAMS cable cut in east Africa', 21, 01,\
                '', '', ''),
            ('20120425', 4, -1, '20120426 0904 TEAMS cable cut again in east\
                    Africa', 22, 01,\
                '', '', ''),
            ('20110824', 4, 196, '201108 hurricane Irene in east U.S.', 23, 11,\
                '', '', ''),
            ('20090601', 14, -1, 'Reference 2009', 24, 10,\
                '', '', ''),
            ('20040601', 14, -1, 'Reference 2004', 25, 10,\
                '', '', ''),
            ('20080510', 4, -1, '20080512 Sichuan earthquake', 26, 01,\
                '', '', ''),
            ]
