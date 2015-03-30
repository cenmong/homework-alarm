from os.path import expanduser

datadir = '/media/usb/' # TODO add 'bgp_project/' at end and really change the directories
homedir = expanduser('~') + '/'
projectdir = expanduser('~') + '/alarm/'

#---------------------------------------------------------
rv_root = 'archive.routeviews.org/' # routeviews.org does not support v6
rrc_root = 'data.ris.ripe.net/'

#--------------------------------------------------------
spt_dir = datadir + 'support/'
pub_spt_dir = datadir + 'support/public/'
rib_info_dir = datadir+'rib_info/'
reset_info_dir = datadir+'reset_info/'
blank_indo_dir = datadir+'blank_info/'
update_list_dir = datadir + 'update_list/' 

#------------------------------------------------------
# according to http://en.wikipedia.org/wiki/Tier_1_network
tier1_asn = [3320,3356,3549,1,2914,5511,1239,6453,6762,12956,1299,701,702,703,1273,2828,6461]

#-------------------------------------------------------
beacon4 = {
    '84.205.64.0/24': ['20110112', '20130610'],
    '84.205.65.0/24': ['20110112', '20130610'],
    '84.205.67.0/24': ['20110112', '20130610'],
    '84.205.68.0/24': ['20110112', '20130610'],
    '84.205.69.0/24': ['20110112', '20130610'],
}

#-------------------------------------------------------
# XXX Note: do not change this list
all_collectors = { # 17 in total
    '': '20011101', 
    'route-views4': '20081201',
    'route-views.eqix': '20040601',
    'route-views.isc': '20031201',
    'route-views.linx': '20040401',
    'route-views.wide': '20030801',
    'rrc00': '19991101', 
    'rrc01': '20000801', 
    'rrc03': '20010201',
    'rrc04': '20010501', 
    'rrc05': '20010701', 
    'rrc10': '20031201',
    'rrc11': '20040301',
    'rrc12': '20040801',
    'rrc13': '20050501',
    'rrc14': '20050101',
    'rrc15': '20060101',
    #'route-views.kixp': '20051101', # RIB size too small
    #'rrc06': '20010901', # too few peers with global table
    #'rrc07': '20020501', # too few peers with global table
}

# TODO omitting co in whole period abandoned. Rerun the affected results if necessary
co_blank = {
    'rrc14': ['20090324', '20100326'],
    'rrc10': ['20110718', '20110914'],
    '': ['20110217','20110224'],
    'rrc11': ['20130130','20130417'],
    #'rrc00': ['20130310','20130313'],
    #'rrc00': ['20130329','20130331'],
}

#-------------------------------------------------------
daterange = {
    0:('20061224','20061230'), #taiwan cable cut | SG
    1:('20081217','20081223'), #mediterranean cable cut 2 | SG
    2:('20030812','20030818'), #east coast blackout | SG
    3:('20050910','20050916'), #LA blackout | SG
    4:('20050823','20050905'), #Hurricane Katrina | SG
    5:('20080128','20080203'), #mediterranean cable cut 1 | SG
    6:('20100225','20100303'), #Chile earthquake | SG
    7:('20110309','20110315'), #Japan Tsunami | SG (Note that 2011 data always cost time!)
    8:('20121020','20121102'), #Hurricane Sandy | SG
    9:('20130316','20130329'), # Spamhaus DDoS attack | WD XXX contained in 28
    10:('20080510','20080516'), #Sichuan Earthquake | SG
    11:('20110819','20110901'), #Hurricane Irene | SG
    12:('20130206','20130212'), #Northeastern US blackout | WD XXX contained in 28
    13:('20100412','20100418'), #Sea-Me undersea cable cut | SG
    14:('20120220','20120226'), #Australia route leakage | todo: download updates + delete reset
    15:('20120806','20120812'), #Canada route leakage | todo: download updates + delete reset
    16:('20030123','20030129'), #Slammer worm | SG
    17:('20130321','20130324'), #EASSy/SEACOM outages | WD XXX contained in 28
    18:('20130213','20130216'), #SEACOM outage | WD XXX contained in 28
    19:('20110326','20110401'), #Caucasus cable cut | SG
    20:('20121221','20121227'), #Georgia-Russia cable cut | SG
    21:('20120223','20120229'), #TEAMS cable cut in east Africa | SG
    22:('20120424','20120430'), #TEAMS cable cut in east Africa again | SG
    23:('20110219','20110225'), # new zealand | SG
    24:('20041224','20041230'), # Indian Ocean earthquake and Tsunami

    28:('20130101','20131231'), # WD

    281:('20130101','20130131'),282:('20130201','20130228'),283:('20130301','20130331'),
    284:('20130401','20130430'),285:('20130501','20130531'),286:('20130601','20130630'),
    287:('20130701','20130731'),288:('20130801','20130831'),289:('20130901','20130930'),
    2810:('20131001','20131031'),2811:('20131101','20131130'),2812:('20131201','20131231'),

    29:('20050101','20051231'), # Downloading in TSB

    # give up 2005 whole year
    291:('20050101','20050131'),292:('20050201','20050228'),293:('20050301','20050331'),
    294:('20050401','20050430'),295:('20050501','20050531'),296:('20050601','20050630'),
    297:('20050701','20050731'),298:('20050801','20050831'),299:('20050901','20050930'),
    2910:('20051001','20051031'),2911:('20051101','20051130'),2912:('20051201','20051231'),

    300:('20050901','20050930'),301:('20060901','20060930'),302:('20070901','20070930'),
    303:('20080901','20080930'),304:('20090901','20090930'),305:('20100901','20100930'),
    306:('20110901','20110930'),307:('20120901','20120930'),308:('20140901','20140930'),

    9001:('20141130','20141201'), # test only
    9002:('20141115', '20141116'), # test only
}
# Blaster worm(2003); first noticed and started spreading on August 11, 2003.
#the number of infections peaked on August 13, 2003
#The 2004 Indian Ocean earthquake occurred at 00:58:53 UTC on 26 December
# Haiti Earthquake in 2010
# Cyclone Nargis (2008)
# Moscow blackout(2005)
# Pakistan Earthquake (2005)
# Witty worm

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
