from slac_ci.datasources import Data

from re import search

class Dhcp(Data):
    """ retrieve list of dhcp ip addresses """

    def __init__( self, path=None, **kwargs ):
        self.path = path
        self.dhcp = {}
        
    def get_dhcp(self):
        dhcps = {}
        with open( self.path, 'r' ) as f:
            for i in f.readlines():
                m = search( r'^\s*(?P<name>\S+)\s+IN A\s+(?P<ip>\d+\.\d+\.\d+\.\d+)', i)
                # logging.error("I: %s" % (i,))
                if m:
                    d = m.groupdict() 
                    self.dhcp[d['ip']] = True
        # for d in dhcps:
        #     yield d
    