#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import ASCENDING, DESCENDING

from copy import deepcopy

import os.path, time
from datetime import datetime, timedelta
from math import ceil

import unicodedata
import cPickle as pickle

from re import match, search, compile, sub, IGNORECASE
import os

import logging

from util import mac_address, parse_number
# from connect import get_mongo_collection
from slac_ci.datasources import get_mongo_collection

# CONTENT_REMAP =


# map to admin
admin_map = [
    {
        'admin:username': {
            'bartelt': 'Unix',
            'renata': 'Unix',
            'ksa': 'Unix',
            'yemi': 'Unix',
            'shirley': 'Unix',
            'amay': 'Unix',
            'lnakata': 'Unix',
            'jonl': 'Unix',
            'ian': 'Unix',
            
            'bchoate': 'Facilities',
            'rafael': 'Facilities',

            'ovrahim': 'Site-Security',

            'jdomingo': 'ITDS',
            'nixx': 'ITDS',
            'sryan': 'ITDS',

            'antony': 'Network',
            'gcx': 'Network',
            'toddmtz': 'Pulse',
            
            'wchan': 'Windows',
            'neal': 'Windows',
            'timm': 'Windows',
            'rodwin': 'Windows',
            'ssink': 'Windows',
            'jcpierre': 'Windows',
            'mattc': 'Windows',
            
            'hlwin': 'Email',
            'rkau': 'Email',
            'juliyana': 'ERP',
            
            'jingchen': 'MCC',
            'brobeck': 'MCC',
            'cxg': 'MCC',
            
            'perazzo': 'PCDS',
            'ling': 'PCDS',

            'ramirez': 'SSRL',
            'thomas.eriksson': 'SSRL',
            'wermelsk': 'SSRL',
            'winston': 'SSRL',

            'fransham': 'Babar',
            'luitz': 'Babar',

            'becla': 'LSST',

        } 
    },
    { 
        'subnet': {
            compile('FACET.*'): 'MCC',
            'ACCTESTFAC': 'MCC',
            compile('LCLS*'): 'MCC',
            'SLCLAVC': 'MCC',
            'LEB': 'MCC',
            compile('BBRMMS-*'): 'Babar',
            compile('FWSSRL1-.*'): 'SSRL', 
            'APMGMT-SSRL': 'SSRL',
            'SSRL': 'SSRL',
            'SSRLA': 'SSRL',
            compile('B084F1-LAT.*'): 'PCDS',
            'DEVCTL-PCDSN': 'PCDS',
            'DEVCTL-B050': 'Unix',
            compile('LTDA.*'): 'Babar',
            compile('FARM.*'): 'Unix',
            compile('PCDSN-.*'): 'PCDS',
            compile('REG-L.*'): 'PCDS',
            compile('B\w+-PRINTER'): 'ITDS',
            compile('B\w+-PUBLIC'): 'ITDS',
            compile('B\w+-PRIVATE'): 'ITDS',
            'PUB4': 'ITDS',
            'BSD': 'ITDS',
            'KAVLI': 'ITDS',
            'SSRL-PRINTER': 'ITDS',
            compile('.*-SITESEC-.*'): 'Site-Security',
            compile('.*-POWER'): 'Facilities',
            'FW-HVAC': 'Facilities',
            'FW-HVAC-ACC': 'Facilities',
            compile('EPN.*'): 'ERP',
            compile('^ERP.*'): 'ERP',
            'DEVCTL-PULSE': 'Unix',
            'ESATESTFAC': 'ITDS',
            compile('NETMGMT.*'): 'Network',
            'NETMGMT-SSRL': 'SSRL',
            compile('NLCTA.*'): 'NLCTA',
            compile('PBX.*'): 'Telecoms',
            compile('SERV01-SEC-.*'): 'Cyber-Security',
            'SERV01-CPP-SENSOR': 'Cyber-Security',
            compile('SERV01-SEC-PRIV\d+'): 'Cyber-Security',
        }
    },
    {
        'port:hostname': {
            compile('(SP-)?WAIN\d*'): 'Unix',
            compile('SP-.*'): 'Unix',
            compile('SEC-.*'): 'Cyber-Security',
            compile('NET-.*'): 'Network',
            compile('.*SSRL.*'): 'SSRL',
        }
    },
    { 
        'device_type': {
            # 'unix server': 'unix-admin',
            'power': 'Facilities',
            # 'windows server': 'windows-admin',
            'router': 'Network',
            'switch': 'Network',
            'load balancer': 'Network',
        } 
    }
]


def determine_admin_group( item, strategies=admin_map ):
    groups = []
    logging.debug("determine admin")
    for i,strategy in enumerate(strategies):
        for k,d in strategy.iteritems():
            # logging.error("K: %s, item %s" % (k,item))
            if k in item:
                a = item[k]
                if not isinstance(a,list):
                    a = [ a, ]
                for v in a:
                    # logging.debug(" using strategy %s: %s, value %s" % (i,k,v))
                    if v in strategy[k]:
                        # logging.debug( " matching on static key: %s" % (v,))
                        if not strategy[k][v] in groups:
                            logging.debug(' found %s by static map using %s' % (strategy[k][v],i))
                            groups.append( strategy[k][v] )
                        continue
                    else:
                        for r in strategy[k]:
                            if not isinstance(r,str) and v:
                                # logging.debug(' matching on regex key: %s' %(r.pattern))
                                if r.match( v ):
                                    logging.debug("  found %s by regex %s using %s" % (strategy[k][r],r.pattern,i))
                                    if not strategy[k][r] in groups:
                                        groups.append( strategy[k][r])
                                    continue
    # logging.error("GROUP: %s" % (groups,))
    if len(groups) == 0:
        # assume ITDS
        groups.append('ITDS')
    if 'Network' in groups:
        groups = [ 'Network', ]
    elif 'MCC' in groups:
        groups = [ 'MCC', ]
    return groups


###
# tools
###
def mac_address( m, format='host' ):
    m = m.replace('.','').replace(':','')
    if format == 'host':
        a = [ m[i:i+2] for i in xrange(0,len(m),2) ]
        return ':'.join(a)
    return m


def merge_dict( old, new, name, ignore=() ):
    ignored = {}
    for k,ref in new.iteritems():
        v = {}
        try:
            for i,j in ref.iteritems():
                v[i] = j
                v['db'] = name
        except:
            v = { 'value': ref, 'db': name }

        if k in ignore:
            if not k in ignored:
                ignored[k] = []
            ignored[k].append( v )

        else:
            if k and not k in old:
                old[k] = []
            old[k].append( v )
            
    return old, ignored


def get_search_list( field, d ):
    # find all unique values of d from field
    # logging.error("D %s: %s" % (field,d))
    v = []
    for i in d[field[0]]:
        f = 'value'
        try:
            f = field[1]
        except:
            pass
        if f in i:
            if i[f] not in v:
                if not i[f] == None:
                    v.append( i[f])
    if len(v) == 1:
        return v.pop()
    elif len(v) == 0:
        raise LookupError, 'not matching fields'

    return { '$in': v }

    # raise NotImplementedError('need IN search for %s = %s' % (field,v))



def merge_item( dbs, db, fields, d, recent_only=False, ignore=None ):
    matched_count = 0
    try:
        # found matches
        count = 0

        v = get_search_list( fields, d )
        search_field = '.'.join(fields)
        s = { search_field: v }
    
        # logging.debug(" + looking for %s" % (s,))
        found = [ o for o in dbs[db].find( s ) ]
        count = len(found)
        logging.debug(' + %-9s had %s\trecord with %s' % (db,count,s))
        # if recent_only, then pick the most recent if more than one found
        if count > 1 and recent_only:
            # logging.error("RECENT ONLY")
            t = None
            for n,f in enumerate(found):
                # logging.debug("  - %s %s" % (db,f,))
                if t == None:
                    t = f
                if f['updated_at'] > t['updated_at']:
                    t = f
            found = [t,]
            # found = found.pop(0)
            
        # branch for special cases where we only want subset of data
        branched = False
        logging.debug("    found: %s" % (found,))
        if len(found) and ignore and isinstance( ignore, dict ):
            # merge into a sub data structure
            # logging.debug("      filter: %s" % (ignore))
            for x,y in ignore.iteritems():
                field,sub = split_field(x)
                logging.debug("      ignore x=%s (field=%s, sub=%s), y=%s" % (x,field,sub,y))
                if field in d:
                    logging.debug("      d contains x=%s => d[field]=%s" % (x,d[field]))
                    for a,b in y.iteritems():
                        logging.debug("     a=%s\tb=%s: %s: %s" % (a,b,field,d[field]))
                        for n in d[field]:
                            logging.debug("     d[field]=%s, a=%s, b=%s" % (n,a,b))
                            if sub in n and n[sub] == a:
                                logging.debug("      ignoring... %s" % (b,))
                                for f in found:
                                    d, ignored = merge_dict( d, f, db, ignore=b )
                                    d = remove_dups( d )
                                logging.debug("   reduced to: %s" % (d,))
                                branched = True
            # stupid hack to get back the hostname
            for f in found:
                this_d = {
                    'port': { 'hostname': f['port']['hostname'] }
                }
                if 'ip_address' in f['port']:
                    this_d['port']['ip_address'] = f['port']['ip_address']
                logging.debug("  get back vm hostname from %s" % (this_d,))
                d, ignored = merge_dict( d, this_d, 'post' )

        # otherwise just do a full merge
        if not branched and len(found):
            for f in found:
                # logging.debug("  - %s %s" % (db,f,))
                d, ignored = merge_dict( d, f, db )
                matched_count = matched_count + 1

    except LookupError, e:
        pass
    except NotImplementedError, e:
        logging.error("ERR: %s" % (e,))

    # logging.error("OUT: %s" % (d,))
    return matched_count, d

def stringify(dict, exclude=[]):
    return ';'.join( [ "%s=%s" % (f,v) for f,v in dict.iteritems() if not f in exclude ] )

def unstringify( s ):
    d = {}
    for k,v in s.split(';').iteritems():
        d[k] = v
    return d

def remove_dups( item ):
    """ goes through each field and determines remove anon arrays content duplicates """
    out = {}
    # logging.error("IN: %s" % item )
    for k,array in item.iteritems():
        out[k] = []
        this_seen = {}
        # use a string to hold hash
        for d in array:
            s = ';'.join( [ "%s=%s" % (f,v) for f,v in d.iteritems() ] )
            if not s in this_seen:
                out[k].append( d )
            this_seen[s] = True
    # logging.error("OUT: %s\n" % out )
    return out

def merge( mongo, ips=[], subnets={}, null_char='', db_names=[], ensure_indexes=( 'nodename.value', 'port.mac_address', 'port.ip_address', 'serial.value', 'PC.value' ), content_remaps={}, strategies={} ):
    """ 
    use referential transparency to simplify the merging
    basically keep the same datastructure for all sources of data in the form dict = { field1: [], field2: []...}
    each pass of a tactic will append onto the relevant field a dict containing all relevant values for that field
    it must also append a 'db' key as part of this hash to identify it's source
    we then store it back in to the document database for later collation
    """
    
    dbs = {}
    for i in db_names:
        logging.debug("initiating db %s" % (i,))
        dbs[i] = get_mongo_collection( mongo, i )
    
    # determine ips to filter for if requested
    search = [ { 'port.ip_address': i } for i in ips ]
    
    logging.info('merging items with %s' % (search,))
    good = 0
    bad = 0
    total = 0
    
    print "Bad Records:"
    print
    
    print "nodename\tadmin\tuser\thostname\tdhcp\tdhcp_ip_address\tip_address\tmac_address\tsubnet\tupdated_at\tage"
    
    now = datetime.now()
    
    for name, strategy in strategies.iteritems():

        logging.debug("strategy: %s %s" % (name, strategy))
        # clear staging db
        dbs[name] = get_mongo_collection( mongo, name )
        dbs[name].remove()

        for i in ensure_indexes:
            dbs[name].ensure_index( i, 1  )

        it = dbs[strategy['start_db']].find()
        if len(search):
            it = dbs[strategy['start_db']].find( { '$or': search } )

        for i in it:

            total = total + 1

            d, ignored = merge_dict( {}, i, strategy['start_db'] )
            logging.debug( 'begin merge %s' % (d,) )

            okay = False
            for n, tactic in enumerate(strategy['tactics']):
                try:
                    r = False
                    r = tactic['time']
                except:
                    pass
                try:
                    v = False
                    v = tactic['filter']
                except:
                    pass
                # logging.debug("   ignore is %s" % (v,))
                c, d = merge_item( dbs, tactic['db'], tactic['field'], d, recent_only=r, ignore=v )
                # logging.error("C: %s\tD: %s" % (c,d) )
                if c > 0:
                    d = remove_dups( d )
                    okay = True

            # make sure we clear the id - wil cause insert issues otherwise
            del d['_id']
            # logging.error("d (%s): %s" % (okay,d,))

            if okay:

                good = good + 1

                # lookout for funny duplicate entries for wifi card and etherenet on like laptops
                # this comes out as having two mac addresses, one of which only have dhcp but not nodename as
                # this is not stored as part of the dhcp
                mac_addresses = {}
                if 'port' in d:
                    for p in d['port']:
                        if 'mac_address' in p:
                            mac_addresses[p['mac_address']] = True
                try:
                    del mac_addresses[None]
                except:
                    pass
                if len(mac_addresses) > 1:
                    # logging.error("POSSIBLE MAC ADDRESSES %s" % (mac_addresses,))
                    # do a search of the final database to see if we already have it somewhere
                    search = [ { 'port.mac_address': m } for m in mac_addresses.keys() ]
                    # f = False
                    for j in dbs[name].find( { '$or': search } ):
                        # logging.error("==> J: %s" % (j,))
                        for x,y in j.iteritems():
                            if x == '_id':
                                continue
                            if not x in d:
                                d[x] = y
                            else:
                                for z in y:
                                    d[x].append( z )
                        # f = True
                        dbs[name].remove( j )
                        
                    d = remove_dups( d )

                if len(search):
                    logging.debug("merged: %s" % (d,))
                dbs[name].insert( d )

            else:

                bad = bad + 1
                subnet = null_char
                try:
                    subnet = ip_in_subnet(d['port'][0]['ip_address'],subnets)['name']
                except:
                    pass
                # logging.error("D: %s" % (d,))
                try:
                    days_ago = null_char
                    ceil( (now - d['updated_at'][0]['value']).days / 7 )
                except:
                    pass
                    
                print "%s\t%s, %s\t%s, %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
                    d['nodename'][0]['value'] if 'nodename' in d else null_char,
                    d['admin'][0]['lastname'] if 'admin' in d else null_char, d['admin'][0]['firstname'] if 'admin' in d else null_char,
                    d['user'][0]['lastname'] if 'user' in d else null_char, d['user'][0]['firstname'] if 'user' in d else null_char,
                    d['port'][0]['hostname'] if 'port' in d and 'hostname' in d['port'][0] else null_char,
                    d['port'][0]['dhcp'] if 'port' in d and 'dhcp' in d['port'][0] else null_char,
                    d['port'][0]['dhcp_ip_address'] if 'port' in d and 'dhcp_ip_address' in d['port'][0] else null_char,
                    d['port'][0]['ip_address'] if 'port' in d and 'ip_address' in d['port'][0] else null_char,
                    d['port'][0]['mac_address'] if 'port' in d and 'mac_address' in d['port'][0] else null_char,
                    subnet,
                    d['updated_at'][0]['value'] if 'updated_at' in d else null_char,
                    days_ago
                )
            logging.debug("\n")

    logging.info("MERGE PERCENT BAD: %s (%s/%s)" % (bad*100/total, bad, total) )


def merge_other( mongo, frm='cando', to='by_mac_address', subnets={}, init_search={ 'port.ip_address': ['port','ip_address'] }, merge_into_search={ 'nodename.value': ['nodename',] }, seen_key=[ ['port', 'ip_address'], ['nodename',], [ 'port', 'hostname' ] ] ):
    # do a final scan through cando to get entries not matched
    dbs = {}
    dbs[frm] = get_mongo_collection( mongo, frm )
    dbs[to] = get_mongo_collection( mongo, to )

    # dbs['staging'] = get_mongo_collection( mongo, 'staging' )
    # dbs['staging'].remove()
    
    need_to_merge = 0
    need_to_add = 0
    total = 0
    skipped = 0
    seen = {}
    
    def string_tuple( r ):
        out = []
        for s in seen_key:
            this = r
            for i in s:
                # logging.error("%s\t%s" % (i,this))
                this = this.get(i)
            out.append(this)
        # return '%s\t%s\t%s' % (r['port']['ip_address'], r['nodename'], r['port']['hostname'])
        return ':'.join(out)
        
    def construct_search( r, d ):
        search = {}
        for k,v in d.iteritems():
            this = r
            for i in v:
                this = this.get(i)
            search[k] = this
        return search
    
    for r in dbs[frm].find():
        
        total = total + 1 

        # logging.info("=> %s\t%s" % (ip,hostname))

        # look up the ip to see if we have a matching one in 'to' db
        search = construct_search( r, init_search )
        # search[k] = 'port.ip_address': r['port']['ip_address'] }

        found = False
        for d in dbs[to].find( search ):
            found = True

        if found == False:
            
            # remove dups
            s = string_tuple(r)
            if s in seen:
                skipped = skipped + 1
                continue

            seen[s] = True

            action = None
            merge = []

            # check up against the nodename, if exists then add as interface
            # search = { 'nodename.value': r['nodename'] }
            search = construct_search( r, merge_into_search )
            # logging.error("SEARCH: %s" % (search,))
            merge = [ d for d in dbs[to].find( search ) ]
            if len(merge):
                action = 'merge'

            # create collapsed data structure for storage
            m, ignored = merge_dict( {}, r, frm )
            del m['_id']

            if action == 'merge':
                
                # for merge ndoes, we don't know for sure that the nodenames are good
                # as we actually need to perform the collate/report first to get a good idea of the 
                # real nodenames.
                need_to_merge = need_to_merge + 1
                logging.debug("MERGE    %s" % (m,))
                dbs[to].insert( m )

            else:

                # for new nodes, we have a high confidence that's it's not already there, so just
                # add it to the to db.
                need_to_add = need_to_add + 1
                logging.debug("NEW NODE %s" % (m,))
                dbs[to].insert( m )

    logging.info("%s: total=%s, add=%s (%s), merge=%s (%s), skipped=%s" % (frm, total, need_to_add, need_to_add*100/total, need_to_merge, need_to_merge*100/total, skipped) )



def ip_in_subnet( ip, subnets ):
    # logging.debug("ip subnet lookup: %s\t%s" % (ip,subnets))
    for k,d in subnets.iteritems():
        if ipaddress.ip_address(ip) in k:
            return d
    return None


def collapse( l ):

    # logging.error("  L: %s" %l)
    this = {}
    for d in l:
        # logging.error("  D %s" %d)
        for k,v in d.iteritems():
            if not k in this:
                this[k] = {}
            
            if not isinstance( v, list ):
                v = [ v ]
            for w in v:
                logging.debug("      collapse THIS[%s] = %s,\t w=%s,\t l=%s" % (k,this[k],w,l))
                if not w in this[k]:
                    this[k][w] = []
                if not d['db'] in this[k][w]:
                    this[k][w].append( d['db'] )
    del this['db']
    
    return this
    
    
def collate_item( item, null_char='',
        fields=( 'device_type', 'is_vm', 'port', 'nodename', 'subnet', 'user', 'admin', 'custodian', 'owner', 'disk', 'memory', 'cpu', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location', 'warranty', 'os', 'updated_at' ),
        remap={}
    ):
    
    """ from the merge() output, flatten the values for each field and determine if there are any conflicts """
    summary = {}
    # logging.error("FIELD: %s" % (item,))
    for s in fields:

        summary[s] = { 'data': None, 'status': None }

        try:
            # logging.error("S: %s" % (s,))
            summary[s]['data'] = collapse( item[s] )
        except KeyError, e:
            # logging.debug("ERROR: %s" % (e,))
            pass

        # add PC number if name kinda looks right
        if s == 'PC':
            try:
                logging.debug(" post parsing pc number for %s" % (summary['nodename']['data']['value'],))
                for k in summary['nodename']['data']['value']:
                    if search( r'-PC\d+$', k ):
                        n = parse_number( k, prepend='PC', digits=5 )
                        if n:
                            # logging.error(" THIS: %s", summary[s]['data'] )
                            if not summary[s]['data']:
                                summary[s]['data'] = { 'value': {} }
                            if not n in summary[s]['data']['value']:
                                summary[s]['data']['value'][n] = []
                            summary[s]['data']['value'][n].append( 'post' )
            except:
                pass
                
        elif s == 'updated_at': # and len(summary[s]['data']['value'].keys()):
            # treat updated_at - use most recent value
            try:
                values = [ n for n in summary[s]['data']['value'].keys() if n ]
            except:
                values = []
            if len(values):
                dt = max( values )
                # logging.error("DATETIME: %s (%s)" % (dt,summary[s]['data']['value'].keys()))
                summary[s]['data']['value'] = {}
                summary[s]['data']['value'][dt] = [ 'post', ]


        if summary[s]['data']:

            # logging.debug(" collating %s:\t %s" % (s,summary[s]['data']))
            status = True

            # two phase due to remapping
            # remap the values to consistent normalised texts
            for k in summary[s]['data']:
                
                v = summary[s]['data'][k]
                for i in v.keys():

                    j = v[i]
                    if i == None:
                        del summary[s]['data'][k][None]
                    
                    try:
                        z = i.upper()
                    except:
                        z = i
                    # logging.debug("    i: %s (%s)\tj: %s" % (z,i,j))
                    # if s in remap:
                    #     logging.debug("      remap: %s" % (remap[s],))
                    # remap common similar values
                    t = s
                    if not k == 'value':
                        t = '%s:%s' % (s,k)
                        # logging.error("T: %s %s - %s %s" % (k,t, t in remap,z))
                    
                    if t in remap and z in remap[t]:
                        # logging.debug("      remap!! %s" % (remap[s],))
                        # see if we have dest value already
                        new = remap[t][z]
                        if new in summary[s]['data'][k]:
                            logging.debug("    appending %s -> %s \t%s" % (i,new,summary[s]['data'][k]))
                            # logging.error("A: %s\t%s" % ('',summary[s]['data'][k][i]))
                            for a in summary[s]['data'][k][i]:
                                if not a in summary[s]['data'][k][new]:
                                    # logging.error(" HERE")
                                    summary[s]['data'][k][new].append( a )
                        else:
                            logging.debug("    setting %s -> %s" % (i,new))
                            summary[s]['data'][k][new] = summary[s]['data'][k][i]
            
                        if not i == new:
                            del summary[s]['data'][k][i]

                # logging.debug("   out -> %s" % (summary[s]['data'],))
            
            for k,v in summary[s]['data'].iteritems():

                # logging.debug( "  k: %s\tv: %s" % (k,v))
                these_keys = v.keys()
                len_these_keys = len(these_keys)

                if s == 'port':

                    # if port has multiple ip's, and sccm is in all of them,  ignore the singel sccm value
                    if k == 'ip_address':
                        # logging.error("UPDATED: %s %s" % (summary,summary[s]['data'],))
                        w = summary[s]['data'][k].keys()
                        if len( w ) == 2:
                            logging.debug("multiple sccm ip addresses")
                            sccm = 0
                            j = None
                            for i in w:
                                # logging.error("W: %s" % i)
                                if 'sccm' in summary[s]['data'][k][i]:
                                    sccm = sccm + 1
                                    if len(summary[s]['data'][k][i]) == 1:
                                        j = i
                            if sccm == 2 and j:
                                logging.debug("removing sccm ip address %s" % (j,))
                                del summary[s]['data'][k][j]
                        
                    # if we have both dhcp true and false, assume tru
                    if 'dhcp' in summary[s]['data']:
                        # logging.error("HERE: %s" % (summary[s]['data']['dhcp'],))
                        if True in summary[s]['data']['dhcp'] and False in summary[s]['data']['dhcp']:
                            del summary[s]['data']['dhcp'][False]
                
                # owner's SLAC is rather useless
                elif s == 'owner' and len_these_keys > 1:
                    logging.debug("  many owners")
                    # delete SLAC
                    try:
                        del summary[s]['data'][k]['SLAC']
                    except:
                        pass
                        
                # cando models and users shouldn't be trusted if we have other sources
                elif s in ( 'model', 'user', 'manufacturer', 'PC', 'os', 'location' ) and len_these_keys > 1:
                    logging.debug("  many %s's found" % (s,))
                    bis_room = []
                    for i in v.keys():
                        # trust bis location information
                        if s == 'location' and k == 'room': # and 'bis' in v[i]:
                            #logging.error("BIS ROOM i %s k %s d %s" % (i,k,summary[s]['data'][k],))
                            if 'bis' in v[i]:
                                bis_room.append(i)
                        if 'cando' in v[i] and len(v[i]) == 1:
                            logging.debug("   ignoring cando %s value %s from %s" % (s,i,summary[s]['data'][k]))
                            del summary[s]['data'][k][i]
                    if len(bis_room):
                        to_del = list( set(v.keys()) - set(bis_room) )
                        #logging.error("BIS %s (%s) -> %s: %s" % (bis_room,v.keys(),to_del,summary[s]['data']['room']))
                        for i in to_del:
                            del summary[s]['data']['room'][i]
                            
                # dont' trust rackwise if it's the only entry
                elif s == 'device_type':
                    for i in v.keys():
                        if len_these_keys > 1 and 'rackwise' in v[i] and len(v[i]) == 1:
                            del summary[s]['data'][k][i]
                            
                # logging.warn("   fi: %s\t%s\t%s" % (s,k,v))
                if len( summary[s]['data'][k].keys() ) > 1:
                    status = False
                    break

            summary[s]['status'] = status
            logging.debug(" + collated: %s\t%s" % (s,summary[s],))

    # add timestamps to ports
    # logging.info("TIME: %s" % (summary,))

    
    # deal with funny hosts with multiple ip's as each hostname may be expressed out as a nodename by the various databases
    try:
        if summary['nodename']['status'] == False:
            nodenames = summary['nodename']['data']['value'].keys()
            hostnames = summary['port']['data']['hostname'].keys()
            # hardcode stupid bullets to be excluded
            if len(nodenames) == 2 and len(hostnames) == 1 and not ( hostnames[0].startswith('BULLET') or hostnames[0].startswith('SIMES') ):
                # hmmm
                ok = True
                for n in nodenames:
                    if null_char in n or ';' in n:
                        ok = False
                if ok and hostnames[0] in nodenames:
                    logging.debug( 'dealing with multiple nodenames NODES: %s\t HOSTS: %s' % (nodenames,hostnames) )
                    good = list( set( nodenames ) - set( hostnames ) ).pop(0)
                    # it's fine, just remove and add database to good value tally
                    for i in summary['nodename']['data']['value'][hostnames[0]]:
                        summary['nodename']['data']['value'][good].append( i )
                    del summary['nodename']['data']['value'][hostnames[0]]
                    summary['nodename']['status'] = True
    except:
        pass


    # dhcp databae custodians and admins are crap, delete if other sources available
    try:
        for f in ( 'admin', 'user', 'custodian' ):
            for sub in summary[f]['data'].keys():
                # logging.debug("F: %s, SUB: %s" % (f,sub))
                keys = summary[f]['data'][sub].keys()
                # logging.debug("  keys: %s, %s" % (keys,summary[f]['data'][sub]))
                if len(keys) > 1:
                    for k in keys:
                        if 'dhcp' in summary[f]['data'][sub][k]:
                            # logging.debug("deleting bad dhcp entries for f=%s sub=%s k=%s, %s" % (f,sub,k,summary[f]['data'][sub][k]))
                            del summary[f]['data'][sub][k]
    except:
        pass


    # logging.error("NODE: %s" % ( summary,))

    nodenames = []
    try:
        nodenames = summary['nodename']['data']['value'].keys()
        # logging.error("NODENAMES: %s"  % (nodenames,) )
    except:
        pass
        
    # logging.error("SUMMARY: %s" % (summary,))
    
    has_port_data = False
    if 'port' in summary and 'data' in summary['port'] and summary['port']['data']:
        has_port_data = True
    
    if len(set( ['ERP-FDEV-WEB01', 'ERP-FDEV-WEBX01', 'ERP-FDEV-WEB02', 'ERP-FUAT-WEB01', 'ERP-FUAT-WEB02', 'ERP-FPRD-WEBX01', 'ERP-FPRD-WEBX01', 'ERP-HDEV-WEB01', 'ERP-HDEV-WEB02', 'ERP-HPRD-WEB01', 'ERP-HPRD-WEB02', 'ERP-HUAT-WEBX01', 'PSHR-WEB01', 'PSHR-WEB02', 'PSOFT-ORACLE09', 'PSOFT-ORACLE10', 'PSOFT-ORACLE08', 'PSOFT-ORACLE11', 'SLAC-ORACLE01', 'COBRA-18', 'MYSQL01', 'MYSQL03', 'PSOFT-WEB01', 'PSOFT-WEB02' ] ).intersection( nodenames ) ) > 0:
    
        # logging.error("THIS: %s" % (item,))
        try:
    
            all_ip = summary['port']['data']['ip_address'].keys()
            for m in summary['port']['data']['ip_address'].keys():
                this = deepcopy(summary)
                for o in set( all_ip ) - set( [m,] ):
                    if o in this['port']['data']['ip_address']:
                        # logging.error( " delete IP: %s" % (o,))
                        del this['port']['data']['ip_address'][o]
                    hostname_for_ip = []
                    for p in item['port']:
                        # logging.error("  port: %s" % (p,))
                        if 'ip_address' in p and p['ip_address'] == o:
                            if 'hostname' in p:
                                hostname_for_ip.append( p['hostname'] )
                    logging.debug("  delete this hostname: %s" % (hostname_for_ip,))
                    # logging.error("    FROM: %s" % (this,) )
                    for h in hostname_for_ip:
                        if h in this['port']['data']['hostname']:
                            del this['port']['data']['hostname'][h]
                this['port']['data']['mac_address'] = {}
                name = this['port']['data']['hostname'].keys().pop()
                this['nodename']['data']['value'] = { name: [ 'post', ]}
                # logging.error("IP: %s\t%s" % (m,name))
                logging.debug("collated multiple erp addresses: %s" % (name,) )
            
                yield this, True

        except:
            pass
    
    
    elif ( has_port_data and 'mac_address' in summary['port']['data'] and  summary['port']['data']['mac_address'] and len(summary['port']['data']['mac_address']) == 1 and 'ip_address' in summary['port']['data'] and summary['port']['data']['ip_address'] and len(summary['port']['data']['ip_address']) > 1 ):
            
            # assume each ip is separate interface on device
            all_ip = summary['port']['data']['ip_address'].keys()
            for i in all_ip:
                this = deepcopy(summary)
                if 'interface' in summary['port']['data']:
                    del summary['port']['data']['interface']
                for o in set( all_ip ) - set( [i,] ):
                    del this['port']['data']['ip_address'][o]
                    # need to determine appropriate hostname to delete too
                    # logging.error("delete %s from: %s" % (o,this['port'],))
                    hostname_for_ip = []
                    for p in item['port']:
                        # logging.error("  port: %s" % (p,))
                        if 'ip_address' in p and p['ip_address'] == o:
                            if 'hostname' in p:
                                hostname_for_ip.append( p['hostname'] )
                    logging.debug("  delete this hostname: %s" % (hostname_for_ip,))
                    # logging.error("    FROM: %s" % (this,) )
                    for h in hostname_for_ip:
                        if h in this['port']['data']['hostname']:
                            del this['port']['data']['hostname'][h]
                    # delete mac address if not same nodename not same as hostname
                if 'hostname' in this['port']['data'] and len(set( this['nodename']['data']['value'].keys() ).intersection( this['port']['data']['hostname'].keys() ) ) == 0:
                    this['port']['data']['mac_address'] = {}
                    
                logging.debug("collated multiple ip addresses %s" % (this,) )
                yield this, True

    
    # deal with entries with two mac addresses due to dhcp merge; assume they are valid
    elif has_port_data and 'mac_address' in summary['port']['data'] and len(summary['port']['data']['mac_address']) > 1:

        all_mac = summary['port']['data']['mac_address'].keys()
        for m in all_mac:
            this = deepcopy(summary)
            for o in set( all_mac ) - set( [m,] ):
                del this['port']['data']['mac_address'][o]
            logging.debug("collated multiple mac addresses" ) #% (this,) )
            yield this, False



    elif has_port_data and 'nodename' in summary and 'data' in summary['nodename'] and summary['nodename']['data'] and summary['nodename']['data']['value'] and len(set( ['WEB07','WEB01'] ).intersection( nodenames )):
        
        this = summary['port']['data']['hostname'].keys().pop()
        if this.startswith( 'WEB07-' ) or this.startswith( 'WEB01-' ):
            # logging.error("GOTCHA %s" % this)
            del summary['nodename']['data']['value']
            summary['nodename']['data']['value'] = { this: [ 'post', ] }
                        
        yield summary, False


    else:

        logging.debug("collated single" ) #": %s" % (summary,))
        yield summary, False




def split_field( f ):
    g = f
    sub = 'value'
    if ':' in f:
        g,_,sub = f.partition(':')
    return g, sub

def datasources( summary ):
    found = {}
    # logging.debug("SOURCES: %s" % (summary,))
    for f in summary.keys():
        # logging.error("THIS: %s" % (summary[f],))
        if isinstance( summary[f], list ):
            for x in summary[f]:
                if 'db' in x:
                    found[x['db']] = True
    return sorted( [ '%s'%f.encode('utf-8') for f in found.keys() ] )


def flatten( d ):
    out = {}
    for k,v in d.iteritems():
        out[k] = v
        if v:
            if len(v) == 1:
                out[k] = v[0]
            else:
                raise Exception('error in data %s' % (v,))
    return out
    