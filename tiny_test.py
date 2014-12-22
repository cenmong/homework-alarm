import cmlib
import logging
logging.basicConfig(filename='main.log', filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Program starts!')

cmlib.get_peer_info('/media/usb/data.ris.ripe.net/rrc00/2014.11/bview.20141130.1600.gz.txt.gz')

logging.info('Program end!')
