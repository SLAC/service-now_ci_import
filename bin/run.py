#!/bin/env python

# boot strap libs

import sys, os
pathname = os.path.abspath( os.path.dirname(sys.argv[0]) )
LIB_PATH =  pathname + '/../lib/'
sys.path.append( LIB_PATH )
ETC_PATH = pathname + '/../etc/'

from slac_ci.connect import *
from slac_ci.collate import *
from slac_ci.output import *
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
    
import argparse
import logging

DB_NAMES = ['taylor','rackwise','ptolemy_arp','bis','sccm','netdb']
OUTPUT_DB = 'testing'

OUTPUT_FIELDS = [ 'nodename', 'device_type', 'is_vm', 'port:hostname', 'port:dhcp', 'port:ip_address',  'port:mac_address', 'user:id', 'user:username', 'user:directorate', 'custodian:id', 'custodian:username', 'owner', 'os:name', 'os:version', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location:building', 'location:room', 'location:rack', 'location:ru', 'cpu:cores', 'memory', 'disk:capacity', 'warranty:start', 'warranty:end', 'updated_at' ]


def update_db( mongo, kwargs ):
    if 'all' in kwargs['db']:
        kwargs['db'] = DB_NAMES
    while len(kwargs['db']):
        db = kwargs['db'].pop()
        pull_db( mongo, db, accounts=kwargs['accounts'] )


if __name__ == '__main__':

    # fields = [ 'nodename', 'device_type', 'is_vm', 'port:hostname', 'port:dhcp', 'port:ip_address', 'subnet', 'port:mac_address', 'user:id', 'user:username', 'user:lastname', 'user:directorate', 'admin:id', 'admin:username', 'admin:lastname', 'custodian:id', 'custodian:username', 'custodian:lastname', 'owner', 'os:name', 'os:version', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location:building', 'location:room', 'location:rack', 'location:ru', 'warranty:start', 'warranty:end', 'updated_at' ]

    parser = argparse.ArgumentParser(description='Collate numerous SLAC Asset databases.')
    parser.add_argument('-v', '--verbose', help='verbose', default=False, action='store_true' )
    
    # configs
    parser.add_argument('--accounts', help='account configurations', default=ETC_PATH+'accounts.yaml' )
    parser.add_argument('--strategies', help='merging strategy configurations', default=ETC_PATH+'strategies.yaml' )
    parser.add_argument('--content_remaps', help='remapping values', default=ETC_PATH+'remaps.yaml' )

    subparsers = parser.add_subparsers( help='sub-command help' )
    
    # pull
    a = subparsers.add_parser( 'pull', help='pull information from data source' )
    a.set_defaults( action='pull' )
    a.add_argument( 'db', choices=DB_NAMES+['all',], nargs="+" )
    # merge
    b = subparsers.add_parser( 'merge', help='merge data from datasources' )
    b.set_defaults( action='merge' )
    b.add_argument( 'ips', nargs="*" )

    for i in ( 'dump', ):
        c = subparsers.add_parser( i, help='output final data to %s format' % (i,) )
        c.set_defaults( action=i )
        c.add_argument( 'nodenames', nargs="*" )
        c.add_argument( '--regex', default=False, action='store_true' )
        c.add_argument( '--nogen', default=True, action='store_false' )
        c.add_argument( '--null_char', default='-' )
        c.add_argument( '--force', default=False, action='store_true' )
    
    kwargs = vars(parser.parse_args())
    
    level = logging.DEBUG if kwargs['verbose'] else logging.INFO
    logging.basicConfig( level=level, format="%(levelname)-7s %(lineno)4d %(message)s" )

    # logging.error("KWARGS: %s" % (kwargs,))
    
    # load configs
    kwargs['accounts'] = load( open(kwargs['accounts'],'r'), Loader=Loader )
    kwargs['strategies'] = load( open(kwargs['strategies'],'r'), Loader=Loader )
    kwargs['content_remaps'] = load( open(kwargs['content_remaps'],'r'), Loader=Loader )
    
    # setup cache for searching etc
    mongo = get_mongo( **kwargs['accounts']['mongo'] )

    # pre
    p = get_postgres( **kwargs['accounts']['ptolemy_arp'] )
    subnets = {}
    if kwargs['action'] in ( 'merge', 'dump' ):
        subnets = get_subnets( p )

    # regex force
    if 'nodenames' in kwargs:
        kwargs['nodenames'] = [ n.upper() for n in kwargs['nodenames'] ]
    if 'regex' in kwargs and kwargs['regex'] and 'nodenames' in kwargs and len(kwargs['nodenames']):
        kwargs['nodenames'] = [ compile(i, IGNORECASE) for i in kwargs['nodenames'] ]

    if kwargs['action'] == 'pull':
        update_db( mongo, kwargs )

    elif kwargs['action'] == 'merge':
        merge( mongo, subnets=subnets, db_names=DB_NAMES, strategies=kwargs['strategies'], ips=kwargs['ips'] )
        
    elif kwargs['action'] == 'dump':
        tsv( mongo, subnets=subnets, nodenames=kwargs['nodenames'], db_name=OUTPUT_DB, pre_collate=kwargs['nogen'], fields=OUTPUT_FIELDS, null_char=kwargs['null_char'], force=kwargs['force'], content_remaps=kwargs['content_remaps'] )

