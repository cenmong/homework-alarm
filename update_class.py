from datetime import datetime
from netaddr import *
import time

# We ignore: 1) an update announces and withdraws the same prefix
# 2) an update announces the same prefix twice

class Update():

    def __init__(self, string):
        self.time = None
        #self.next_hop = None
        self.announce = []
        self.withdrawn = []
        #self.as_path = []
        #self.communities = None
        #self.origin = None
        self.protocol = None  # For modifing prefix format
        self.pfx_type = -1  # A or W or None

        string = string.split('@@@')
        for line in string:
            if line == '':# May exist at the end
                continue
            line = line.replace('|', '') # 6 and some 4 updates has '|'

            if line[0].isdigit() and '.' in line:
                if self.pfx_type == 0:  # A
                    self.announce.append(self.pfx_to_binary(line))
                elif self.pfx_type == 1:  # W
                    self.withdrawn.append(self.pfx_to_binary(line))
                else:
                    print 'Wrong pfx type!'
                continue

            if line == 'ANNOUNCE':
                self.pfx_type = 0
                continue
            if line == 'WITHDRAW':
                self.pfx_type = 1
                continue

            header = line.split(': ')[0]
            try:
                content = line.split(': ')[1]
            except:
                continue
            
            if header == 'TIME':
                self.time = content
            elif header == 'FROM':
                content = content.split()[0]
                addr = IPAddress(content).bits()
                if len(addr) > 40:# IPv6 addr
                    self.from_ip = addr.replace(':', '')
                    self.protocol = 6
                else:
                    self.from_ip = addr.replace('.', '')
                    self.protocol = 4 

            #elif header == 'NEXT_HOP':
                #content = content.split('  (')[0]
                #if content == '': # can happen
                    #self.next_hop = ''
                #else:
                    #self.next_hop = self.pfx_to_binary(content)
            #elif header == 'ASPATH':  # don't care about it now
                #self.as_path = content.split()
            #elif header == 'COMMUNITY':  # don't care now
                #self.communities = content# Store string really OK?
            #elif header == 'ORIGIN':
                #self.origin = content
            else:
                pass

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

    def get_time(self):
        return self.time

    def get_from_ip(self):
        return self.from_ip

    def get_announce(self):
        return self.announce

    def get_withdrawn(self):
        return self.withdrawn

    def get_protocol(self):
        return self.protocol

    def is_abnormal(self):
        aset = set(self.announce)
        if len(aset) < len(self.announce):
            return True
        wset = set(self.withdrawn)
        if len(wset) < len(self.withdrawn):
            return True
        bset = aset.intersection(wset)
        if bset:
            return True

        return False

    def print_attr(self):
        print self.time
        #print self.next_hop
        print self.announce
        print self.withdrawn
        #print self.as_path
        #print self.communities
        #print self.origin
        print self.protocol
