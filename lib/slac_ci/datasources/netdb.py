from slac_ci.datasources import Data

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
    
import subprocess

from re import search

import logging
LOG = logging.getLogger()

class Netdb( Data ):
            
    def __init__( self, update_bin=None, yaml=None, **kwargs):
        self.update_bin = None
        self.yaml = yaml

    def __iter__( self ):
        docs = {}

        # hack!
        if self.update_bin:
            LOG.info('updating netdb')
            subprocess.call( [ self.update_bin, ] )

        with open( self.yaml, 'r' ) as f:
            docs = load(f, Loader=Loader)

    
        for name, doc in docs.iteritems():
            # logging.error("DOC: %s" % (doc,))
            ports = []
            for i in doc['interfaces']:
                p = {
                    # 'interface': ''
                    'mac_address': i['ethernet'],
                }
                if 'ipnum' in i:
                    for ip in i['ipnum']:
                        p['ip_address'] = ip
                        p['hostname'] = i['name'].upper()
                        ports.append( p )
    
            loc = {
                'building': doc['bldg'] if not doc['bldg'] == 'Not found' else None,
                'room': str(doc['room']) if not doc['room'] == 'UNKNOWN' else None
            }
            if loc['room'] and ';' in str(loc['room']):
                a = loc['room'].split(';')
                loc['room'] = a.pop(0).strip()
                for i in a:
                    try:
                        k,v = i.strip().split('=')
                        loc[k] = v
                    except:
                        pass

            # usernames
            try:
                fullname = None
                username = None
                fullname, username = doc['users'][0].split( ' (' )
                username = username[:-1] # strip )
            except:
                pass
            user = {}
            # print "  -> %s " % (users_by_username, )
            if self.users and username in self.users.by_username:
                # logging.error("username: %s\t%s" % (username,username in users_by_username))
                user = self.users.by_username[username]
                # r['user']['username'] = r['username'].lower()
     
            # OS
            m = search( '^(?P<name>.*) (?P<version>\d+\.\d+(\.\d+)?)', doc['os'][0] )
            os = {
                'name': doc['os'][0]
            }
            if m:
                os = m.groupdict()
                # temp: to ease migration
                del os['version']
     
            # admin teams
            admin = None
            for a in doc['admins']:
                if a.endswith(';'):
                    admin = a
     
            # model
            branding = {}
            # logging.error('MODEL: %s' %(doc['model'],))
            if not doc['model'] == 'UNKNOWN: any (UNKNOWN)':
                m = search( r'^(?P<make>.*): (?P<model>.*) \((?P<device_type>.*)\)$', doc['model'] )
                if m:
                    branding = m.groupdict()
                    branding['device_type'] = branding['device_type'].lower()
                # logging.error(" OT: %s" % branding)
            this = {
                'id': doc['id'],
                'nodename': str(doc['node']).upper(),
                'location': loc,
                "status": doc['state'],
                'manufacturer': branding['make'] if 'make' in branding else None,
                'model': branding['model'] if 'model' in branding else None,
                'device_type': branding['device_type'] if 'device_type' in branding else None,
                'os': os,
                'full_name': fullname,
                'username': username,
                'user': user,
                'admin_group': admin,
            }
        
            if this['model'] in ( 'System Configuration: Not Available    X4500', ):
                del this['model']
        
        
            # assets
            if 'tags' in doc:
                for tag in doc['tags']:
                    for t,v in tag.iteritems():
                        if t == 'Asset':
                            # logging.error("%s\t-> %s" % (doc['node'],v) )
                            if v:
                                for a in str(v).split(';'):
                                    # strip leading space
                                    a = a.lstrip()
                                    if '=' in a:
                                        try:
                                            k,x = a.split('=')
                                            # logging.error("  -- %s\t%s" % (k,x))
                                            this[k] = x
                                        except:
                                            logging.error("could not parse tag %s -> %s" % (doc['node'],a) )
                                            pass
                                    else:
                                        logging.error("could not parse tag %s -> %s" % (doc['node'],v) )

            # logging.error(" >> %s" % (this,))

            # yield this
            for y in ports:
                this['port'] = y
                if '_id' in this:
                    del this['_id']
                # logging.debug(">> %s" % (o,))
                yield this
    