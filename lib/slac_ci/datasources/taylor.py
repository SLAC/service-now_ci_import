from slac_ci.datasources import Data

from re import compile, match
import os
from datetime import datetime

from slac_ci.util import mac_address
from copy import deepcopy

import logging
LOG = logging.getLogger()


class Taylor(Data):
    
    """ Taylor data from afs """

    def __init__( self, path='/afs/slac/g/scs/systems/system.info/', files=('taylor.environ','nicinfo') , **kwargs ):
        self.path = path
        self.files = files
        
    def __iter__(self):
        
        network_re = compile( '^(?P<interface>.*)_(?P<key>(mac|ip))$' )
        for i in [name for name in os.listdir(self.path) if os.path.isdir(os.path.join(self.path, name))]:

            d = {}
            for file in self.files:
                try:
                    fp = '%s/%s/%s' % (self.path,i,file)
                    # LOG.debug( "FP: %s" % (fp,) )
                    # d['updated_at'] = time.ctime(os.path.getmtime(fp))
                    t = os.path.getmtime(fp)
                    d['updated_at'] = datetime.fromtimestamp( t )
                    # LOG.debug(" t: %s\t%s"%(t,d['updated_at']))

                    with open( fp ) as f:
                        for l in f.readlines():
                            try:
                                k,v = l.strip().split('=')
                                d[k] = v
                            except:
                                pass
                except Exception,e:
                    LOG.warn(" could parse %s: %s" % (file,e))

            # df
            try:
                with open( '%s/%s/%s' % (self.path,i,'df') ) as f:
                    header = True
                    all_output = ''
                    for l in f.readlines():
                        if header:
                            header = False
                            continue
                            # stupid output may stretch across two lines, so let's just concat all together and iterate
                        all_output = all_output + '\t' + l.strip()
                    # just do summary for now
                    d['disk'] = { 'capacity': 0, 'used': 0 }
                    for x in findall( '\s*(?P<mount>\/\S+)\s+(?P<capacity>\d+)\s+(?P<used>\d+)\s+(?P<available>\d+)\s+(?P<percent>\d+)%\s+/\S+', all_output ):
                        # LOG.error("  %s" % (x,))
                        for n,v in enumerate( ('capacity','used') ):
                            # LOG.error("mount %s %s %s -> %s" % (n,v,x, x[n+1]) )
                            d['disk'][v] = d['disk'][v] + int(x[n+1])
                        # LOG.error("    %s" % (d['disk'],))
                        
                    for v in d['disk'].keys():
                        d['disk'][v] = d['disk'][v] / ( 1024*1024 )
                    # LOG.error("TOTAL %s %s" % (i,d['disk'],) )
            except:
                pass

            if not 'HOSTNAME' in d:
                LOG.debug(" skipping...")
                continue
            # LOG.debug("D: %s" % (d,))
            out = {
                'nodename': d['HOSTNAME'].upper(),
                'ip_address': d['IPADDR'] if 'IPADDR' in d else None,
                'serial': d['SERIAL'].upper() if 'SERIAL' in d else None,
                'model': d['MODEL'] if 'MODEL' in d else None,
                'manufacturer': d['BIOSVENDOR'] if ('BIOSVENDOR' in d and not d['BIOSVENDOR'] in ( '<BAD INDEX>', 'American Megatrends Inc.', 'Intel Corporation') ) else None,
                'os': {
                    'name': d['VENDOR'],
                    'version': "%s%s" % (d['VERSION'],'.%s'%(d['UPDATE'],) if 'UPDATE' in d else ''),
                },
                'updated_at': d['updated_at'],
                'port': [],
                'cpu': {
                    'arch': d['ARCH'] if 'ARCH' in d else None,
                    'cores': int(d['CPU']),
                    # 'cores': int(d['NCPU']) if 'NCPU' in d else None,
                    'id': d['CPUID'] if 'CPUID' in d else None,
                },
                'disk': d['disk'] if 'disk' in d else None
            }

            if d['MEMORY'].endswith('MB'):
                out['memory'] = int(d['MEMORY'].replace('MB',''))

            if 'DEPT' in d and len(d['DEPT']):
                out['user'] = {
                    'department': d['DEPT'],
                }
        
            if out['serial'] in ( 'empty', 'Not Specified', '0000000000', 'To Be Filled By O.E.M.', '0', '1234567890', '0123456789', 'xxxxxxxxxx', 'DELL', 'AK000VK000', ".....", 'AK000VK0', ):
                del out['serial']
            # mailgate01 serial
            elif out['serial'] == '0838QAR08F':
                out['serial'] = '0827QAR142'
            # dnsmaster
            elif out['serial'] == '0840QAR03F':
                out['serial'] = '0953QAR062'
            # dnsmaster
            elif out['serial'] == '0749QAT00B':
                out['serial'] = '0819QAT0ES'
            # confjira01
            elif out['serial'] == '0849QAR0A2':
                out['serial'] = '0932QAR039'
            # ppa-pc74907-l
            elif out['serial'] == 'CH70P71':
                out['serial'] = '85LNXC1'
            elif out['serial'] == '.. ..':
                del out['serial']
            elif out['serial'] == 'Not Available':
                del out['serial']
            
            
            # determine if vm etc
            if 'model' in out  and out['model']:
                if out['model'] == 'KVM':
                    out['manufacturer'] = 'RedHat'
                    out['is_vm'] = True
                elif out['model'] == 'VMware Virtual Platform':
                    out['manufacturer'] = 'VMware'
                    out['is_vm'] = True
                elif out['model'] == 'Virtual Machine':
                    out['manufacturer'] = 'Microsoft'
                    out['is_vm'] = True
                elif out['model'] == 'VirtualBox':
                    out['manufacturer'] = 'Oracle Corporation'
                    out['is_vm'] = True
                elif out['model'] == 'Seabios':
                    out['manufacturer'] = 'RedHat'
                    out['manufacturer'] = 'oVirt'
                    out['is_vm'] = True
                elif out['model'] == 'OpenStack Nova':
                    out['is_vm'] = True
                
                # remove
                elif out['model'] in ( '..', 'System Configuration: Not Available        Not Available' ):
                    del out['model']
            
                for i in ( 'ASUS', ):
                    if 'model' in out and out['model'] and out['model'].startswith(i):
                        a = out['model'].split()
                        out['manufacturer'] = a.pop()
                        out['model'] = ' '.join(a)
            
            # mac addresses etc.
            interfaces = {}
            for k in d:
                # # LOG.error("== %s" % (k,))
                if k.startswith('MAC_'):
                    # LOG.error("%s\t%s\t%s" % (opt,k,d[k]))
                    m = match( r'^MAC_(?P<int>.*)', k ).groupdict()['int']
                    if not m in interfaces:
                        interfaces[m] = {}
                    interfaces[m]['mac_address'] = d[k].lower()
                else:
                    pass

                m = network_re.match( k )
                if m:
                    t = m.groupdict()
                    if not t['interface'] in interfaces:
                        interfaces[t['interface']] = {}
                    key = '%s_address' % (t['key'],)
                    v = d[k].lower()
                    if '/' in v:
                        a = v.split('/')
                        v = a.pop(0)
                        if len(a):
                            interfaces[t['interface']]['prefix_len'] = a.pop(-1)
                    # LOG.error("V: %s\t%s" % (v,type(v)))
                    interfaces[t['interface']][key] = v
                    if key == 'mac_address':
                        interfaces[t['interface']][key] = mac_address( v )
                    elif key == 'ip_address':
                        # skip certain ips
                        if interfaces[t['interface']][key].startswith('192.168.') or \
                            interfaces[t['interface']][key].startswith('198.168.') or \
                            interfaces[t['interface']][key] in ( '172.23.209.204', '172.23.209.205', '172.23.209.206', '172.23.210.204', '172.23.210.205', '172.23.210.206', '172.23.214.204', '172.23.214.205', '172.23.214.206', '172.23.20.72', '172.23.20.73', '172.23.10.14', '172.23.10.15', '134.79.129.43', '134.79.129.46', '172.23.20.72', '172.23.20.73', '134.79.129.53', '134.79.129.42' ): # lb nodes
                            interfaces[t['interface']][key] = None
                
                    LOG.debug(" set %s %s %s" % (t['interface'],key,interfaces[t['interface']][key]))

            # for each interface
            for k,v in out.iteritems():
                try:
                    if v == '':
                        out[k] = None
                except:
                    pass
                
            # setup the ip address of nic if unknown (guess)
            # LOG.debug("INT: %s" % (interfaces,) )
            these_interfaces = interfaces.keys()
            if len( these_interfaces ) == 1:
                this_interface = these_interfaces.pop(0)
                if not 'ip_address' in interfaces[this_interface]:
                    interfaces[this_interface]['ip_address'] = out['ip_address']
                    del out['ip_address']
            else:
                for this_interface in ( 'eth0', 'em1' ):
                    # LOG.error("THIS: %s\t%s" % (this_interface, out))
                    if this_interface in interfaces and not 'ip_address' in interfaces[this_interface]:
                        interfaces[this_interface]['ip_address'] = out['ip_address']
                        del out['ip_address']
                        break

            o = deepcopy( out )
            for interface in ( 'lo', 'sit0' ):
                if interface in interfaces:
                    del interfaces[interface]

            for interface, items in interfaces.iteritems():
                # bad mac address reporting
                # LOG.debug("ITEMS: %s %s" % (interface,items,))
                if 'mac_address' in items and items['mac_address'] in ( '00:00:00:00:00:00', ):
                    continue
                items['interface'] = interface
                o['port'] = items
                if '_id' in o:
                    del o['_id']
                yield o
            #     yield
            #     .append( items )
            #
            # # yield o
            # for y in out['port']:
            #     if '_id' in o:
            #         del o['_id']
            #     o['port'] = y
            #     LOG.debug(">> %s" % (o,))
            #     yield o

