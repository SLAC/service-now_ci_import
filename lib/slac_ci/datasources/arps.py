from slac_ci.datasources.postgres import Postgres

from slac_ci.util import mac_address

import logging
LOG = logging.getLogger(__name__)

class Arps(Postgres):
    
    def __iter__( self ):
        self.cursor.execute( """SELECT
          context->'mac_address' AS mac_address,
          context->'ip_address' AS ip_address,
          ARRAY_AGG( updated_at ) AS updated_at
        FROM
          arps__arps
        GROUP BY context->'mac_address', context->'ip_address';
        """ )

        # WHERE
        # GROUP BY mac_address, ip_address
        #   updated_at > (now() - '1 days'::interval)

        def give( d ):
            # logging.debug( "%s" % d['updated_at'] )
            # 
            d['mac_address'] = mac_address( d['mac_address'] )
            d['updated_at'] = max(d['updated_at'])

            if d['mac_address'] in ( '00:00:00:00:00:00', ):
                del d['mac_address']
            if d['ip_address'] in ( '0.0.0.0' ):
                del d['ip_address']
           
            if 'ip_address' in d:
                # LOG.warn( "DHCP: %s" % (self.dhcp))
                if ( self.dhcp and d['ip_address'] in self.dhcp.dhcp ) or d['ip_address'].startswith( '198.129' ) or d['ip_address'].startswith( '134.79.84' ):
                    d['dhcp_ip_address'] = d['ip_address']
                    del d['ip_address']
                elif d['ip_address'].startswith( '192.168' ):
                    del d['ip_address']
                else:
                    d['ip_address'] = d['ip_address']

            return d

        # dedup
        table = {}
        for this in self.cursor:
            if this['mac_address'] and this['ip_address']:
                d = give( this )
                # assume those without ip are dhcp
                if not 'ip_address' in d:
                    d['dhcp'] = True
                # s = stringify( d, exclude=['updated_at',] )
                if 'mac_address' in d:
                    s = d['mac_address']
                    if not s in table:
                        table[s] = d
                    if d['updated_at'] > table[s]['updated_at']:
                        # table[s]['updated_at'] = d['updated_at']
                        table[s] = d

        for s,d in table.iteritems():
            this = {
                'updated_at': d['updated_at'],
                'port': {}
            }
            for i in ( 'ip_address', 'mac_address', 'dhcp', 'dhcp_ip_address' ):
                if i in d:
                    this['port'][i] = d[i]
            # print("S: %s" % (this,))
            yield this