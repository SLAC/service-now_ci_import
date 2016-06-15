from slac_ci.datasources import Data

import urllib2
import requests
from requests.auth import HTTPBasicAuth

import logging
LOG = logging.getLogger(__name__)

class Casper( Data ):

    def __init__( self, uri='https://casper.slac.stanford.edu:8443', user=None, password=None, **kwargs ):
        self.uri = uri
        self.user = user
        self.password = password


    def __iter__( self ):
        # get list of machines
        url = self.uri + '/JSSResource/computers'
        headers = {'content-type': 'application/json', 'Accept': 'application/json' }
        auth = HTTPBasicAuth( self.user, self.password )
        r = requests.get( url, headers=headers, auth=auth )
        
        try:
            data = r.json()

            # logging.error(" IDS: %s" % ([ d['id'] for d in data['computers'] ],))
            for c in [ d['id'] for d in data['computers'] ]:
                # request in seriaal
                url = '%s%s/%s'% (uri, '/JSSResource/computers/id', c)
                # logging.error(" URL %s" % (url,))
                d = requests.get( url , headers=headers, auth=auth ).json()['computer']
                # logging.error("  R: %s" % (json.dumps(d),))
                out = {
                    'nodename': d['general']['name'].upper(),
                    'ip_address': d['general']['ip_address'],
                    'serial': d['general']['serial_number'],
                    'model': d['hardware']['model'],
                    'manufacturer': d['hardware']['make'],
                    'os': {
                        'name': d['hardware']['os_name'],
                        'version': d['hardware']['os_version']
                    },
                    'port': [ {
                        'mac_address': d['general']['mac_address'],
                        'ip_address': d['general']['ip_address']
                    }],
                    'cpu': {
                        'arch': d['hardware']['processor_architecture'],
                        'cores': int(d['hardware']['number_processors']),
                        'id': d['hardware']['processor_type'],
                    },
                    'memory': d['hardware']['total_ram_mb'],
                    # 'disk': d['disk'] if 'disk' in d else None
                    'username': d['location']['username'],
                    'updated_at': d['general']['last_contact_time'] if not d['general']['last_contact_time'] == '' else None
                }
        
                # add alt mac
                if 'alt_mac_address' in d['general']:
                    out['port'].append( { 'mac_address': d['general']['alt_mac_address'] } )
                yield out
            
        except Exception, e:
            LOG.error("Error: %s - %s" % (e,r))
            
