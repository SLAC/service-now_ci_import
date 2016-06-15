from slac_ci.datasources.postgres import Postgres

import ipaddress
from re import search

class Subnets( Postgres ):

    def get_subnets( self ):
        self.cursor.execute( """SELECT *
        FROM
          subnets
        """ )
        self.subnets = {}
        for d in self.cursor:
            i = d['prefix'] + '/' + d['netmask']
            # print ' %s' % (i,)
            try:
                k = ipaddress.ip_network(i)
                s = search( r'vlan\s*(?P<vlan>\d+)', d['description'], IGNORECASE )
                d['vlan'] = None
                if s:
                    d['vlan'] = s.groupdict()['vlan']
                # logging.error("VLAN: %s" % (d,))
                self.subnets[k] = d
            except:
                pass
        return True
        # return subnets
