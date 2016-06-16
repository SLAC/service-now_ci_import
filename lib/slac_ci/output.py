from collate import *

import datetime

def report( mongo, clear_states=True, process_main=True, process_others=False, content_remaps={}, **kwargs ):
    """ return unique nodes; nodes with valid multiple interfaces will have len(_ports_) > 1 """
    # copy db over
    db = get_mongo_collection( mongo, kwargs['db_name'] )

    # remove all 'done' fields
    if clear_states:
        logging.debug("clearing done states")
        db.update( { 'done': True }, { '$unset': { 'done': False } }, upsert=False, multi=True )

    order_by = ('nodename',ASCENDING)

    # go through list twice to determine unique real nodenames
    nodenames = {}
    search = {}
    if len(kwargs['nodenames']) == 0:
        process_others = True
        nodenames = {}
    else:
        search = { 'nodename.value': kwargs['nodenames'].pop(0) }
        nodenames = {}

    logging.info("searching for %s" % (search,))

    # get pc numbers to do match also
    for i in db.find( search, { 'nodename.value': 1, 'PC.value': 1, '_id': 0 } ):
        logging.debug("%s " % (i,))
        if 'nodename' in i:
            for node in i['nodename']:
                v = node['value']
                if not ( ';' in v or '/' in v or '#' in v ):
                    if not v in nodenames:
                        nodenames[v] = []
                    if 'PC' in i:
                        for p in i['PC']:
                            pc = p['value']
                            if not pc in nodenames[v]:
                                nodenames[v].append(pc)
    # logging.debug("NODENAMES: %s" % (nodenames,))

    if process_main:
        for n in nodenames:
            # ignore special case of null hostnames til later
            if n == None:
                continue
            search = [ { 'nodename.value': n } ]
            if isinstance( nodenames, dict ):
                # ignore none's
                for pc in nodenames[n]:
                    if pc:
                        search.append( { 'PC.value': pc } )
            # logging.error("SEARCH: %s" % (search,))
            for a,b,c,d in report_item( db, { '$or': search }, fields=kwargs['fields'], subnets=kwargs['subnets'], order_by=order_by, content_remaps=content_remaps ):
                yield a,b,c,d

    # deal with all not yet processes (ie those without a hostname)
    if process_others:
        others = {}
        for i in db.find( { '$and': [ { 'nodename.value': None }, { 'done': None } ] } ):
            # logging.error("I: %s" % (i,))
            for p in i['port']:
                others[ p['mac_address'] ] = True
        # logging.error("MAC: %s" % (others,))
        for m in others:
            for a,b,c,d in report_item( db, { 'port.mac_address': m }, fields=kwargs['fields'], subnets=kwargs['subnets'], order_by=order_by, content_remaps=content_remaps ):
                yield a,b,c,d
                
    # do a post scan? ie distinct( 'port.hostname' )?
    # for n,i in enumerate(db.find( { 'done': None } )):
    #     logging.error("LEFT NOT DONE: %s\t%s " % (n,i))



def construct_group_error( group_errors, field, values, data ):
    logging.debug(" aggregating group errors with %s\t%s" % (field,values))
    s,t = split_field( field )
    if not s in group_errors:
        group_errors[s] = {}
    if not t in group_errors[s]:
        group_errors[s][t] = {}
    for val in values:
        if not val in group_errors[s][t]:
            group_errors[s][t][val] = []
        # logging.debug("DATA: %s" % (data,))
        for d in data:
            if s in d:
                for i in d[s]:
                    if t in i and i[t] == val:
                        # logging.debug("> %s in db %s" % (i[t],i['db']))
                        if not i['db'] in group_errors[s][t][val]:
                            logging.debug('  adding group error: source %s\t%s:%s\t%s' % (i['db'],s,t,val))
                            group_errors[s][t][val].append( i['db'] )


def del_errors( to_delete, k, x, errors, node ):
    for n in to_delete:
        try:
            logging.debug("   deleting %s %s %s" % (k,x,n,))
            del errors[k][x][n]
            if len(errors[k][x]) == 1:
                del errors[k][x]
        except:
            logging.debug("   hmmm..")

        key = '%s%s'%(k,':%s'%x if not x == 'value' else '')
        logging.debug("      remove from %s data %s key=%s, %s" % (x,n,key,node[key]))
        if isinstance( node[key], list):
            try:
                node[key].remove(n)
            except:
                pass
            logging.debug("     got: %s" % (node[key],))
            if len(node[key]) == 1:
                node[key] = node[key].pop(0)
            logging.debug("     got: %s" % (node[key],))

def remove_source_errors( source , k,s, errors, node):
    """ remove entries if other values exist for source k """
    if k in errors:
        logging.debug("  remove_source_errors on %s: %s" % (k,errors[k],))
        # determine if dhcp is part
        for x in errors[k].keys():
            y = errors[k][x]
            logging.debug("   X: %s, Y: %s" % (x,y))
            to_delete = []
            for z,values in y.iteritems():
                if len(values) == 0:
                    to_delete.append(z)
                logging.debug("    source: %s\tvalues: %s" % (source,values,))
                # if source in values and len(values) == 1:
                if source in values:
                    to_delete.append(z)
                logging.debug('    to_delete: %s' % (to_delete,))
            del_errors( to_delete, k, x, errors, node )
        if len(errors[k]) == 0:
            del errors[k]

def take_source( source , k,s, errors, node):
    """ remove entries if other values exist for source k """
    if k in errors:
        logging.error("  take_source (%s) %s" % (k,errors,))
        # determine if dhcp is part
        for x in errors[k].keys():
            y = errors[k][x]
            logging.debug("   X: %s, Y: %s" % (x,y))
            to_delete = []
            for z,values in y.iteritems():
                if not source in values:
                    to_delete.append(z)
            del_errors( to_delete, k, x, errors, node )
        if len(errors[k]) == 0:
            del errors[k]
                    
def clean_error( k,s,v, errors, node ):
    # logging.debug("cleaning %s %s %s:\t%s" % (k,s,v,errors))
    if k in errors and s in errors[k] and errors[k][s] > 1 and v in errors[k][s]:
        logging.debug('removing %s %s = %s' % (k,s,v) )
        # clean up errors
        del errors[k][s][v]
        if len( errors[k][s] ) == 1:
            del errors[k]
        key = '%s%s'%(k,':%s'%s if not s == 'value' else '')
        # clean up node
        # logging.error("KEY: %s\t%s = %s" % (key,node,v))
        node[key].remove(v)
        if len(node[key]) == 1:
            node[key] = node[key].pop(0)

def go_with_popular( k,s,v, errors, node ):
    # logging.error("HERE %s %s %s: %s" % (k,s,v,errors))
    if k in errors and s in errors[k] and errors[k][s] > 1:
        keep_value = None
        all_values = []
        c = 0
        for v in errors[k][s]:
            all_values.append(v)
            if len( errors[k][s][v] ) > c:
                keep_value = v
        # logging.error("KEEP: %s %s %s %s" % (k,s,keep_value,all_values))
        for a in all_values:
            if not a == keep_value:
                clean_error( k,s,a, errors, node )



def format_errors( errors, order=( 'serial', 'PC', 'nodename', 'manufacturer', 'model', 'PO', 'admin', 'user', ) ):
    def give( k,v ):
        for x,y in v.iteritems():
            k_prime = '%s%s' % (k,'' if x == 'value' else ':%s' % x)
            # logging.error("  x: %s\tk: %s\ty: %s" % (x,k_prime,y))
            yield '*%s*: %s' %(k_prime,y)
    if errors:
        for o in order:
            if o in errors:
                for i in give( o,errors[o] ):
                    yield i
                    # logging.error("ERRORS %s: %s"%(o,errors,))
                    if o in errors:
                        del errors[o]
        for k in errors:
            for i in give( k,errors[k] ):
                yield i



def report_item( db, search, 
        fields=(), 
        subnets={}, 
        order_by=('nodename.value',ASCENDING), 
        unique_fields=( 'port:ip_address', 'port:mac_address', 'port:hostname', 'port:dhcp', 'subnet' ),
        ignore_fields=(),
        max_value_fields=( 'updated_at', ),
        multivalue_fields=( 'admin_group', '_ports_', 'admin:username', 'admin:id', 'admin:lastname' ),
        content_remaps={}
    ):
    # yield unique device which may have multiple network interface ports
    
    seen = {}
    logging.debug( "-----------" )
    logging.debug( "reporting %s" % (search,) )
    local_errors = {}   # errors for just singel entry
    data = []
    
    nodenames = []
    
    already_processed = 0
    
    last_seen = None
    
    for item, state, strategy, errors, sources, d, ignore_done in collate( db.find( search ), order_by, fields=fields, subnets=subnets, content_remaps=content_remaps ):

        logging.debug( " [%s] %s" % ( 'X' if 'done' in d else 'O', item,) )
        data.append( d )
        
        if errors:
            logging.debug("local err: %s" % (errors,))

        # as long as it looks good...
        if ignore_done or not 'done' in d:

            logging.debug("ITEM 1: %s" % (item,))

            # assume the search worked to link related records, so use name
            # logging.error("NODENAME: %s \t of %s" % (item['nodename'],nodenames))
            if item['nodename']:
                for n in item['nodename']:
                    if not n in nodenames:
                        nodenames.append( n )
                
            elif not item['nodename']:
                if len(nodenames):
                    item['nodename'] = nodenames
                else:
                    # none for nodename - eg devices not seen by central systems
                    # logging.error("NO NODENAME! for %s" % (item,))
                    item['nodename'] = [ None, ]

            for n in item['nodename']:
                # logging.error("THISN: %s" % (n,))
                if not n in seen:
                    seen[n] = {}
                    for f in multivalue_fields:
                        seen[n][f] = []
                    for f in item:
                        if not f in unique_fields:
                            seen[n][f] = []

                for k,v in item.iteritems():
                            
                    if not k in unique_fields:
                        # if v and not v in seen[n][k]:
                        if v:
                            for x in v:
                                if not x in seen[n][k]:
                                    logging.debug( '  setting %s\t%s \t %s' % (n,k,x))
                                    seen[n][k].append(x)

                # for a in determine_admin_group( item ):
                #     if not a in seen[n]['admin_group']:
                #         seen[n]['admin_group'].append( a )

                port = {}
                # logging.debug("ITEM 2: %s" % (item,))
                for f in unique_fields:
                    # logging.debug( '  port: %s\t%s' % (f,item[f]))
                    if f in item:
                        port[f] = item[f]
                    else:
                        port[f] = None
                # double check that only single values exist for ports

                # for k,v in port.iteritems():
                #     logging.error("PORT: %s %s" % (k,v))
                #     if isinstance(v,list) and len(v) > 1:
                #         pass

                logging.debug('  adding _port_ %s' % (port,))
                seen[n]['_ports_'].append(port)

                for f in max_value_fields:
                    # logging.error("MAX: %s" % (seen[n][f],))
                    if len( seen[n][f] ):
                        dt = max(seen[n][f])
                        seen[n][f] = [dt,]
            
            # mark in db that this has been used
            d['done'] = True
            db.save( d )
            
        else:
            logging.debug("already processed: %s" % (item,))
            already_processed = already_processed + 1

        # aggregate multiple errors together from each
        if errors:
            logging.debug('aggregating local errors')
            for k,b in errors.iteritems():
                # logging.debug(" multi value error: k=%s\tb=%s" % (k,b))
                if not k in local_errors:
                    local_errors[k] = {}
                for subject,c in b.iteritems():
                    # logging.debug("  k=%s\tv=%s\ta=%s" % (k,subject,a))
                    if not subject in local_errors[k]:
                        local_errors[k][subject] = {}
                    for v,f in c.iteritems():
                        if not v in local_errors[k][subject]:
                            local_errors[k][subject][v] = []
                        local_errors[k][subject][v] = []
                        for x in f:
                            if not x in local_errors[k][subject][v]:
                                # logging.debug( "VTYPE: %s %s" % (v,type(v)))
                                logging.debug("  adding local error: source %s\tto %s:%s\tvalue %s" % (x,k,subject,v))
                                local_errors[k][subject][v].append(x)
            logging.debug("local errors: %s" % (local_errors,))
        else:
            logging.debug('no local errors')

    if already_processed > 0:
        logging.debug('  already processed: %s' % (already_processed,))

    # flatten and yield for each unique nodename
    for n,node in seen.iteritems():

        this_node = {}
        group_errors = {}  # errors for this grouping of entries for this node
        
        logging.debug("reporting node: %s \t%s" % (node['nodename'], node,))
        processed_ports = []
        
        for field,values in node.iteritems():
            
            # collapse
            # logging.error("  K: %s\tV: %s" % (k,v))
            if not field in multivalue_fields:
                if len(values) > 1:
                    # set values
                    this_node[field] = [ this_node[field], ] if field in this_node else []
                    for val in values:
                        if not val in this_node[field]:
                            this_node[field].append( val )

                    construct_group_error( group_errors, field, values, data )

                else:
                    logging.debug(" no group errors with %s\t%s" % (field,values))
                    if len(values) == 1:
                        this_node[field] = values[0]
                        try: # remove stupid chars if string
                            this_node[field] = sub( r'\n', ' ', this_node[field] )
                        except:
                            pass
                    elif len(values) == 0:
                        this_node[field] = None

            else:
                # logging.error("FIELD 1: %s \t%s"%(field,node[field]) )
                this_node[field] = node[field]
                # logging.error("FIELD 2: %s \t%s"%(field,this_node[field]) )

            if field == '_ports_':
                # report on bad ports
                for f, v in port.iteritems():
                    if v == None:
                        pass
                    elif len(v) > 1:
                        construct_group_error( group_errors, f, v, data )

                # determine uniq ports on device
                uniq = {}
                for port in values:
                    # if it fails, add upstream
                    add = True
                    try:
                        # logging.info("PORT %s" % (port,))
                        p = flatten(port)
                        s = stringify( p )
                        if not s in uniq:
                            uniq[s] = 0
                        else:
                            add = False
                        uniq[s] = uniq[s] + 1
                    except:
                        pass
                    finally:
                        if add:
                            processed_ports.append(port)

        # set final
        this_node['_ports_'] = processed_ports

        logging.debug("reporting: %s" % (this_node,))

        # if not nodename, then use the pc number if exists
        # logging.error("NAME: %s" % (this_node['nodename'],))
        if not this_node['nodename']:
            if 'PC' in this_node and this_node['PC']:
                this_node['nodename'] = this_node['PC']
            else:
                this_node['nodename'] = 'unk-%s'%(this_node['_ports_'][0]['port:mac_address'][0].replace(':',''),)
                # not_slac = not_slac + 1
        # logging.error(" -> NAME: %s" % (this_node['nodename'],))
        
        
        if not this_node['device_type']:
            this_node['device_type'] = 'computer'
        
        
        # logging.error("IN LOCAL ERRORS: %s" % (local_errors,))
        # logging.error("IN GLOBAL ERRORS: %s" % (group_errors,))
        
        for e in ( group_errors, local_errors ):
            remove_source_errors('bis', 'device_type', None, e, this_node)
            remove_source_errors('rackwise', 'device_type', None, e, this_node)
            remove_source_errors('rackwise', 'model', None, e, this_node)
            remove_source_errors('rackwise', 'nodename', None, e, this_node)
            
        
        
        if len(group_errors) > 0:

            # let's reduce common issues
            clean_error('owner','value','SLAC', group_errors, this_node)
            # clean_error('device_type','value','computer', group_errors, this_node)
            # clean_error('device_type','value','server', group_errors, this_node)
            # clean_error('device_type','value','switch', group_errors, this_node)

            # remove_dhcp_errors('user',None,None, group_errors, this_node)

            # trust ptolemy device models
            # take_source( 'ptolemy_device', 'model', None, group_errors, this_node )

            go_with_popular('location','room', None, group_errors, this_node)

            # if 'device_type' in group_errors:
            #     vals = group_errors['device_type']['value'].keys()
            #     if 'laptop' in vals and 'unix server' in vals:
            #         del group_errors['device_type']['value']['unix server']
            #     if len( group_errors['device_type']['value'].keys() ) == 1:
            #         del group_errors['device_type']


        if len(local_errors) > 0:

            pass
            # deal with stupid device types
            # if 'device_type' in local_errors:
            #     vals = local_errors['device_type']['value'].keys()
            #     if len(vals) > 1 and 'computer' in vals:
            #         del local_errors['device_type']['value']['computer']
            #     if ('unix server' in vals or 'windows server' in vals) and 'server' in vals:
            #         del local_errors['device_type']['value']['server']
            #     if ( 'router' in vals ) and 'switch' in vals:
            #         del local_errors['device_type']['value']['switch']
            #     if 'laptop' in vals and 'unix server' in vals:
            #         del local_errors['device_type']['value']['unix server']
            #     if 'storage server' in vals and 'server' in vals:
            #         del local_errors['device_type']['value']['server']
            #     if ( 'server' in vals ) and 'router' in vals:
            #         del local_errors['device_type']['value']['router']
            #
            #     if len( local_errors['device_type']['value'].keys() ) == 1:
            #         del local_errors['device_type']
            #     # logging.error("HERE %s\t%s" % (local_errors,this_node))
                        
            # take_source( 'ptolemy_device', 'model', None, local_errors, this_node )
        
        # determine if we have same mac address and or same ip addresses repeated for this node        
        if len(local_errors) > 0:
            logging.debug("LOCAL ERRORS %s" % (local_errors,))
            yield False, this_node, local_errors, 'local'

        elif len(group_errors) > 0:
            logging.debug("GROUP ERRORS %s" % (group_errors,))
            yield False, this_node, group_errors, 'group'

        else:
            logging.debug("NO ERRORS")
            yield True, this_node, None, None


def collate( cursor, sort_by, fields=[], subnets={}, null_char='', content_remaps={} ):

    for i in cursor:
        
        logging.debug("")
        logging.debug( "collating %s" % (i,) )
        for summary, ignore_done in collate_item( i, remap=content_remaps ):
            # logging.debug("summarising %s" % summary )

            # merge subnet
            try:
                # logging.error("SUBNET: %s" % (subnets,))
                ip = ','.join( summary['port']['data']['ip_address'].keys() )
                summary['subnet']['data'] = { 'value': { ip_in_subnet(ip,subnets)['name']: ['post',] } }
                summary['subnet']['status'] = True
            except:
                summary['subnet']['status'] = False

            # try:
            #     # try to determine type of device
            #     category = None
            #     sources = {}
            #
            #     # assume anything in rackwise is a server
            #     for x,y in summary['nodename']['data'].iteritems():
            #         # logging.warn("Y: %s" % (y,))
            #         for z, a in y.iteritems():
            #             for s in a:
            #                 sources[s] = True
            #             # logging.warn("Z: %s \t%s" % (z,a))
            #             if z.startswith('RTR-LB'):
            #                 category = 'load balancer'
            #             elif z.startswith( 'SWH-'):
            #                 category = 'switch'
            #             elif z.startswith( 'RTR-') or z.startswith( 'RTRG-'):
            #                 category = 'router'
            #             elif z.startswith( 'AP-' ):
            #                 category = 'access point'
            #             elif 'rackwise' in a:
            #                 category = 'server'
            #             if category:
            #                 break
            #
            #     # narrow down if server
            #     if category == 'server':
            #         if 'taylor' in sources:
            #             category = 'unix server'
            #         elif 'goliath' in sources:
            #             category = 'windows server'
            #
            #     # if subnet indicates type
            #     if category == None:
            #         for x,y in summary['subnet']['data'].iteritems():
            #             for k in y:
            #                 # logging.error("Y %s" % (k,))
            #                 if k.startswith( 'FARM' ):
            #                     category = 'unix server'
            #                 elif k.startswith( 'SERV' ) or k.startswith( 'NETHUB'):
            #                     category = 'server'
            #                 elif k.startswith( 'DEVCTL-' ) or k in ( 'LTDA-VM' ):
            #                     category = 'unix server'
            #                 elif 'WINMGMT' in k:
            #                     category = 'windows server'
            #                 elif 'PRINTER' in k:
            #                     category = 'printer'
            #                 if category:
            #                     break
            #
            #     if category:
            #         if category in summary['device_type']['data']['value']:
            #             summary['device_type']['data']['value'][type].append( 'post' )
            #             for x in summary['device_type']['data']['value'].keys():
            #                 if not x == category:
            #                     del summary['device_type']['data']['value'][x]
            #         else:
            #             summary['device_type']['data']['value'] = { category: ['post',] }
            #         summary['device_type']['status'] = True
            #
            # except:
            #     summary['device_type']['status'] = False
        
        
            # logging.debug( "summary: %s" % (summary,) )

            out = {}
        
            # first field is if the summary is ok
            state = []
            bad = {}
            
            # rest of data
            # logging.debug("== %s" % (summary,))
            for f in fields:
                g, sub = split_field( f )
                # logging.debug(" analysing %s.%s: %s" % (g,sub,summary[g]))
                value = None
                try:

                    this = summary[g]['data'][sub]
                    a = this.keys()
                    value = a #'; '.join( "%s"%i for i in a )
                    ok = True

                    if len(a) > 1:
                    
                        good = False
                    
                        # logging.error("LEN: %s %s " % (g,a,))
                        if g == 'nodename':

                            # deal with rackwise and blade chassis
                            rackwise_nodenames = []
                            other_names = []
                            # get name in rackwise
                            for x,z in this.iteritems():
                                if 'rackwise' in z:
                                    if not x in rackwise_nodenames:
                                        rackwise_nodenames.append(x)
                                else:
                                    if not x in other_names:
                                        other_names.append(x) 

                            # logging.error("RACK: %s" % (rackwise_nodenames,))
                            if len(rackwise_nodenames):
                            
                                # if we have common names, no problem
                                this_name = list(set(rackwise_nodenames).intersection(other_names) )
                                if len(this_name):

                                    value = [ this_name[0], ]
                                    good = True

                                else:

                                    for r in rackwise_nodenames:
                                        logging.debug( "rackwise name... %s" % (r,))

                                        # range of names
                                        m = search( '^(?P<name>\D+)(?P<fm>\d{3,})\-(?P<to>\d{3,})$', str(r) )
                                        ranges = []
                                        if m:
                                            d = m.groupdict()
                                            # same leading digits
                                            ranges = [ "%s%s"%(d['name'],str(n).zfill(len(d['fm']))) for n in xrange( int(d['fm']), int(d['to'])+1 ) ]
                                        elif match( r'\/ \S+$', str(r) ):
                                            logging.error( 'BULLET NAME!')
                                        else:
                                            # array of names
                                            ranges = compile(r'\s*(,|;|\n)\s*').split(r)
                                            # logging.error("RANGE %s" % (ranges,))

                                        # inside?
                                        logging.debug("rackwise nodename range: %s -> %s of %s" % (r,ranges,other_names))
                                        this_name = list(set(other_names).intersection(ranges) )
                                        if len(this_name):
                                            logging.debug(' nodename checks out okay in range of hosts in rackwise')
                                            value = [ this_name[0], ]
                                        
                                            good = True
                                        
                                        # just do plain text search
                                        elif len(rackwise_nodenames) == 1:
                                            logging.debug("plain text rackwise string search %s" % (this,))
                                            for o in other_names:
                                                if o in rackwise_nodenames[0]:
                                                    value = [o,]
                                                    good = True
                                        # delete rackwise entry
                                        # if good:
                                        #     logging.error("DEL %s" % )
                                        
                    
                        # elif g == 'port':
                            # logging.error("HERE HERE %s" % (this,))
                        #     if isinstance( this, dict ) and len( this ) > 1:
                        #         logging.error("ERROR HERE")
                    
                        if not good:
                            logging.debug(" not good for some reason: %s" % (this,))
                            ok = False
                            # bad[g] = s
                            # logging.error("BAD: %s\t%s" % (g,summary[g]['data'],))
                            if not g in bad:
                                bad[g] = {}
                            for k,b in summary[g]['data'].iteritems():
                                # logging.debug("  g: %s\tk: %s\tb: %s" % (g,k,b))
                                if not k in bad[g]:
                                    bad[g][k] = {}

                                for v,a in b.iteritems():
                                    # logging.debug("    g: %s, k: %s, v: %s\ta: %s" % (g,k,v,a))
                                    if not v in bad[g][k]:
                                        bad[g][k][v] = []
                                    # logging.error("    add: %s" % a)
                                    for c in a:
                                        if not c in bad[g][k][v]:
                                            bad[g][k][v].append(c)
                                            logging.debug("  setting as bad: %s:%s value \'%s\' source %s" % (g,k,v,c,) )
                            # logging.error("BAD: %s" % (bad,))
                    # logging.error("ADDING %s" % (bad,))
                    state.append( ok )
                except TypeError,e:
                    # if e:
                    #     logging.error("type error: %s" % e)
                    value = None
                except KeyError,e:
                    # logging.warn("No key %s for field %s on %s" % (e,f,summary[g]['data'] ))
                    value = None
                logging.debug(" + summarised field %s\tas %s" % (f,value))
                if isinstance(value,list):
                    c = len(value)
                    # if c == 1:
                    #     out[f] = value[0]
                    if c == 0:
                        out[f] = None
                    # elif c > 1:
                    else:
                        out[f] = value
                else:
                    out[f] = value
                    
            # logging.info("OUT: %s" % (out,))
            yield out, \
                state, \
                "%s"%i['strategy'] if 'strategy' in i else null_char, \
                bad, \
                datasources(i), \
                i, \
                ignore_done




def tsv( mongo, extra_headers=['state','number_errors','error_type','errors'], null_char='', content_remaps={}, **kwargs ):
    """ output results to stdout """
    
    # header
    these_fields = ['admin_group', ] + kwargs['fields']
    print '\t'.join(extra_headers+these_fields+['strategy','number sources','sources'])

    bad = 0
    good = 0

    for state, item, errors, error_type in report( mongo, content_remaps=content_remaps, **kwargs ):

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
                    # try:
                    if isinstance( v, datetime.datetime ):
                        v = str(v)
                    v = v.encode('latin-1','ignore')
                    # except:
                    #     pass
                stuff.append( v if v else '%s'%null_char )
            
            sorted_errors = [ e for e in format_errors( errors ) ]
            if state:
                good = good + 1
            else:
                bad = bad + 1

            print '\t'.join( ['okay' if state else 'bad',] + \
                ['%s'%len(sorted_errors), error_type if error_type else null_char, '; '.join(sorted_errors) if len(sorted_errors) else null_char] + \
                stuff 
            )
            # + \
            # [strategy,'%s'%len(sources),'; '.join(sources,)])
    
    logging.error("OUPUT PERCENT BAD: %spc\t(%s/%s)" % (bad*100/good if good > 0 else 100,bad,good))
    
    
def flat( mongo, extra_headers=['state','number_errors','error_type','errors'], null_char='', content_remaps={}, header_remaps={}, **kwargs ):
    """ output results to json """
    
    bad = 0
    good = 0

    def normalise( k, v, header_remaps ):
        if k in header_remaps:
            key = header_remaps[k]
            this[key] = v
            if id(type) and type(v) in ( datetime.datetime, datetime.date ):
                return key, str(v)
            elif isinstance( v, list ):
                if len(v) == 1:
                    return key, v[0]
                elif len(v) == 0:
                    return key, None
        return None, None

    for state, item, errors, error_type in report( mongo, content_remaps=content_remaps, **kwargs ):

        this = {}
        # stupid datetime in json
        for k,v in item.iteritems():
            if k in ( '_ports_', ):
                continue
            key, value = normalise( k,v, header_remaps )
            if key:
                this[key] = value

        for p in item['_ports_']:

            for k,v in p.iteritems():
                key, value = normalise( k,v, header_remaps )
                # logging.error("K: %s\t%s" % (k,key))
                if key:
                    this[key] = value

            if state:
                good = good + 1
                # strip building prepend
                # print this['u_location_building']
                if this['u_location_building']:
                    this['u_location_building'] = sub( r'^B', '', this['u_location_building'] )
                yield this
            else:
                bad = bad + 1
    
    logging.error("OUPUT PERCENT BAD: %spc\t(%s/%s)" % (bad*100/good if good > 0 else 100,bad,good))
    



    
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
