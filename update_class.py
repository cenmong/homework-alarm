from datetime import datetime
from netaddr import *
import time

# We ignore: 1) an update announces and withdraws the same prefix
# 2) an update announces the same prefix twice

class Update():

    def __init__(self, update_line):
        self.time = None
        #self.next_hop = None
        self.announce = ''
        self.withdraw = ''
        #self.as_path = []
        #self.communities = None
        #self.origin = None
        self.protocol = None  # For modifing prefix format
        self.pfx_type = -1  # A or W or None

        attr = update_line
        attr = attr.split('|')
        if attr[2] == 'A':  # A
            self.announce = self.pfx_to_binary(attr[5])
            self.pfx_type = 0
        elif attr[2] == 'W':  # W
            self.withdraw = self.pfx_to_binary(attr[5])
            self.pfx_type = 1
        else:
            print 'Wrong pfx type!'

        self.time = attr[1]

        from_addr = IPAddress(attr[3]).bits()
        if len(from_addr) > 40:# IPv6 addr
            self.from_ip = from_addr.replace(':', '')
            self.protocol = 6
        else:
            self.from_ip = from_addr.replace('.', '')
            self.protocol = 4 


    def pfx_to_binary(self, content):
        length = None
        pfx = content.split('/')[0]
        try:
            length = int(content.split('/')[1])
        except:
            pass
        if self.protocol == 4:
            addr = IPAddress(pfx).bits()
            addr = addr.replace('.', '')
            if length:
                addr = addr[:length]
            return addr
        elif self.protocol == 6:
            addr = IPAddress(pfx).bits()
            addr = addr.replace(':', '')
            if length:
                addr = addr[:length]
            return addr
        else:
            print 'protocol false!'
            return 0
    '''
    def equal_to(self, u):# According to Jun Li, do not consider prefix
        # May be incomplete.
        if self.next_hop == u.next_hop and self.as_path == u.as_path and\
        self.communities ==u.communities and self.origin == u.origin:
            return True
        else:
            return False

    def has_same_path(self, u):
        if self.as_path == u.as_path:
            return True
        else:
            return False
    '''
