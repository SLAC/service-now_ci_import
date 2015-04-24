from collate import *


def dump( mongo, extra_headers=['state','number_errors','error_type','errors'], null_char='', **kwargs ):
    """ output results to stdout """
    
    # header
    these_fields = ['admin_group', ] + kwargs['fields']
    print '\t'.join(extra_headers+these_fields+['strategy','number sources','sources'])

    bad = 0
    good = 0

    for state, item, errors, error_type in report( mongo, **kwargs ):

        for p in item['_ports_']:
            this = item
            # logging.error("DUMP: %s " % (this,))
            stuff = []
            # copy
            for k,v in p.iteritems():
                this[k] = v
            for f in these_fields:
                v = this[f]
                try:
                    if v == None:
                        v = null_char
                    if isinstance( v, list ):
                        v = '; '.join( '%s'%i for i in v)
                    elif isinstance( v, bool ) or isinstance( v, int ) or isinstance( v, datetime ):
                        v = '%s' % v
                except:
                    pass
                # logging.debug(" f: %s\t %s\t%s" % (f,v,type(v)))
                if v:
                    v = v.encode('ascii','ignore')
                stuff.append( v if v else '%s'%null_char )
            
            sorted_errors = [ e for e in format_errors( errors ) ]
            if state:
                good = good + 1
            else:
                bad = bad + 1

            print '\t'.join( ['okay' if state else 'bad',] + \
                ['%s'%len(sorted_errors),error_type if error_type else null_char,'; '.join(sorted_errors) if len(sorted_errors) else null_char] + \
                stuff 
            )
            # + \
            # [strategy,'%s'%len(sources),'; '.join(sources,)])
    
    logging.info("PERCENT BAD: %spc\t(%s/%s)" % (bad*100/good if good > 0 else 100,bad,good))
    
    
    
def netdb( mongo, **kwargs ):
    """ output the data in netdb batch file format """
    
    good = 0
    bad = 0
    not_slac = 0
    seen = { 'port:ip_address': {}, 'port:mac_address': {} }
    
    for state, node, errors, error_type in report( mongo, **kwargs ):

        name = node['nodename']
        
        # skip certain nodes
        # if name in ( '' ):
        #     logging.warn( "skipping %s" % (name,))
        #     continue
            
        # logging.error("FORCE : %s" % (kwargs['force'],))
        if errors and ('force' in kwargs and not kwargs['force'] == True):
            
            bad = bad + 1
            logging.error("  skipping error'd node: %s" % (name,))
            
        else:
            
            good = good + 1
            logging.debug("=> node (%s): %s" % (name,node,))
            
            # work out how many ports first to determine the type of node to clone
            uniq = { 'port:ip_address': {}, 'port:mac_address': {}, 'port:hostname': {} }
            ports = []
            for port in node['_ports_']:
                # logging.error("PORT: %s" % (port,))
                try:
                    p = flatten(port)
                    for i in ( 'port:ip_address', 'port:mac_address', 'port:hostname' ):
                        if not p[i] in uniq[i]:
                            uniq[i][ p[i] ] = 0
                        uniq[i][ p[i] ] = uniq[i][ p[i] ] + 1
                    ports.append(p)
                except Exception,e:
                    logging.error("ERROR: %s" % (e,))
            
            # logging.error("UNIQ: %s" % (uniq,))

            normal_node = True
            # if not ip's then assume normal node
            if len(uniq['port:ip_address']) == 1 and None in uniq['port:ip_address']:
                logging.debug("all dhcp normal node")
                normal_node = True
                
            # if many different mac adress for one ip, then ignore mac
            elif len(ports) > 1 and len( uniq['port:ip_address'] ) == 1 and not None in uniq['port:ip_address']:
                ports = [ ports[0], ]
                ports[0]['port:mac_address'] = None
                normal_node = True
            
            # if only one hostname
            elif len(ports) > 1 and len( uniq['port:hostname'] ) == 1:
                normal_node = True
            
            # TODO: multiple ports with same ip? TODO: stupid 'F' PTR only....
            elif len(ports) > 1:
                normal_node = False

            # global tally
            for p in ports:
                # logging.error("PORT: %s" % (p,))
                for i in ( 'port:ip_address', 'port:mac_address' ):
                    if not p[i] in seen[i]:
                        seen[i][ p[i] ] = 0
                    seen[i][ p[i] ] = seen[i][ p[i] ] + 1

            # determine template to clone
            type = 'TEMPLATE-NODE'
            try:
                if node['nodename'].startswith('FIRE-') or node['nodename'].startswith('FIREGPU-') or node['nodename'].startswith('SIMES0') or  node['nodename'] in ( 'PPA-MATHEMATICA', 'NXSERV' ):
                    type = 'TEMPLATE-ADVANCEDNODE.NoDomain'
                    name = node['nodename'] + '.NoDomain'

                elif normal_node:
                    # check to see if port hostname is same as nodename
                    if not ports[0]['port:hostname'] == name:
                        type = 'TEMPLATE-ADVANCEDNODE.NoDomain'
                # force with nodomain names
                else:
                    name = node['nodename'] + '.NoDomain'
                    type = 'TEMPLATE-ADVANCEDNODE.NoDomain'
                if node['device_type'] in ( 'router', 'switch', 'load balancer' ):
                    type = 'TEMPLATE-ROUTER.NoDomain'
            except:
                type = 'TEMPLATE-ADVANCEDNODE.NoDomain'
                    
            print 'node clone --template=%s --name=%s' % (type, name,)
            for a in node['admin_group']:
                if not a == 'Network':
                    print 'node group --add=%s --remove=Network %s' % (a,name)
                    print 'node admin --add=%s: --remove=Network: %s' % (a,name)
                
            # or?
            if node['manufacturer'] and node['model']:
                m = node['model']
                if not m:
                    m = 'any'
                t = node['device_type']
                if isinstance(t,str) and 'server' in t:
                    t = 'server'
                print "node model --set '%s:%s%s' %s" % (node['manufacturer'],node['model'],':%s'%t if t else '',name)
                # TODO test other permutations

            if node['os:name'] and not node['os:name'] == 'builtin':
                # TODO add verions; append to string (netdb doesn't support versions)
                print "node os --add '%s%s' --remove 'builtin' %s" % (node['os:name'],' %s'%(node['os:version'],) if node['os:version'] else '',name)

            if node['user:directorate']:
                directorate = node['user:directorate']
                if isinstance(directorate,list):
                    directorate = directorate.pop(0)
                print 'node department --set "%s" %s' % (directorate,name)

            if node['user:username']:
                print 'node user --add %s %s' % (node['user:username'],name)

            # assets
            assets = []
            for s in ( 'serial', 'PC', 'PO' ):
                if node[s]:
                    try:
                        this = str(node[s]).encode('utf-8')
                        if isinstance( node[s], list ):
                            this = '/'.join(node[s]).encode('utf-8')
                        assets.append( '%s=%s' % (s,this))
                    except:
                        pass
            if len(assets):
                try: print "node custom --add Asset='%s' %s" % ('; '.join(assets),name)
                except: pass

            # TODO add custodian and owner fields... into assets?
            # TODO department

            # location
            if node['location:building']:
                b = node['location:building']
                # TODO: hmm.. hack!
                if isinstance(b,list):
                    if len(b) == 0:
                        b = None
                    else:
                        b = b.pop(0)

                if b == None:
                    pass
                elif b == 'OSU':
                    b = 'Off campus'
                elif not b.startswith('B'):
                    b = 'B%s'%b
                
                print 'node location --set "%s:%s%s%s" %s' % (
                    b if b else 'Not found',
                    '%s'%node['location:room'].replace('#','') if node['location:room'] else '',
                    '; rack=%s'%node['location:rack'] if node['location:rack'] else '',
                    '; ru=%s'%node['location:ru'] if node['location:ru'] else '',
                    name)

            if node['updated_at']:
                print 'node expiration --set "%s" %s' % (node['updated_at']+timedelta(weeks=78),name)


            for p in ports:
                hostname = p['port:hostname']
                if not hostname or type == 'TEMPLATE-NODE':
                    hostname = None
                    
                if p['port:mac_address'] == None and p['port:ip_address'] == None:
                    logging.error("host %s can't have both null values for ip and mac" % (hostname,))
                    continue
                
                # logging.error("HOSTNAME: %s\t NAME: %s" % (hostname, name))
                print 'node interface --add=%s %s %s %s %s' % ( \
                    p['port:mac_address'],
                    '--ip=%s'% p['port:ip_address'] if p['port:ip_address'] else '',
                    '--dhcp=on --roam' if p['port:dhcp'] else '',
                    '--name=%s'%hostname if hostname and not hostname == name else '',
                    name)


            print

    for i in ( 'port:ip_address', 'port:mac_address' ):
        for v in seen[i]:
            if seen[i][v] > 2:
                logging.warn('%s seen %s times' % (v,seen[i][v]))

    logging.info("PERCENT BAD: %spc\t(%s/%s), NOT SLAC: %s" % (bad*100/good if good > 0 else 100,bad,good,not_slac))
