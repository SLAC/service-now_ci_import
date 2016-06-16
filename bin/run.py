#!/usr/bin/python


# boot strap libs
import sys, os

pathname = os.path.abspath( os.path.dirname(sys.argv[0]) )
LIB_PATH =  pathname + '/../lib/'
sys.path.append( LIB_PATH )
ETC_PATH = pathname + '/../etc/'


from slac_ci.datasources import pull_db, get_mongo
from slac_ci.collate import merge

# old
from slac_ci.output import *


from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from json import dumps as json_dumps, load as json_load

from re import sub

import urllib2
import base64
from datetime import datetime 
import argparse

from importlib import import_module

import logging
LOG = logging.getLogger(__name__)


DB_NAMES = []
OUTPUT_DB = 'testing'

OUTPUT_FIELDS = [ 'nodename', 'device_type', 'is_vm', 'port:hostname', 'port:dhcp', 'port:ip_address',  'port:mac_address', 'user:id', 'user:username', 'user:directorate', 'custodian:id', 'custodian:username', 'owner', 'os:name', 'os:version', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location:building', 'location:room', 'location:rack', 'location:ru', 'cpu:cores', 'memory', 'disk:capacity', 'warranty:start', 'warranty:end', 'updated_at' ]


def update_db( mongo, kwargs ):
    if 'all' in kwargs['db']:
        kwargs['db'] = DB_NAMES

    # need users class for lookups
    klass = getattr( import_module('slac_ci.datasources.users'), 'Users' )
    users = klass( **kwargs['accounts']['support']['users'] )
    users.get_users()
    # lookup for if a host is dhcp or not
    # dhcp_table = get_dhcp_ips( **accounts['dhcp'] )
    klass = getattr( import_module('slac_ci.datasources.dhcp'), 'Dhcp' )
    dhcp = klass( **kwargs['accounts']['support']['dhcp'] )
    dhcp.get_dhcp()
    
    while len(kwargs['db']):
        db = kwargs['db'].pop()
        pull_db( mongo, db, accounts=kwargs['accounts']['assets'], users=users, dhcp=dhcp )

def get_subnets( kwargs ):
    klass = getattr( import_module('slac_ci.datasources.subnets'), 'Subnets' )
    subnets = klass( **kwargs )
    subnets.get_subnets()
    return subnets.subnets

def _upload( url, user, password, data ):
    auth = base64.encodestring('%s:%s' % (user,password)).replace('\n','')
    req = urllib2.Request( url, data, { 'Content-Type': 'application/json' } )
    req.add_header("Authorization", "Basic %s" % auth )
    f = urllib2.urlopen(req)
    # resp = f.read()
    for i in json_load(f)['records']:
        yield i
    # print "RESP: %s" % (resp,)
    f.close()

def upload( url, user, password, data ):
    for i in _upload( url, user, password, data ):
        status = True if i['__status'] == 'success' else None
        if not i['sys_import_state_comment'] in ( '', 'No field values changed' ):
            status = False
        yield i['u_nodename'], status, i['sys_import_state_comment'], i


def load_configs( kwargs ):
    kwargs['accounts'] = load( open(kwargs['accounts'],'r'), Loader=Loader )
    kwargs['strategies'] = load( open(kwargs['strategies'],'r'), Loader=Loader )
    kwargs['content_remaps'] = load( open(kwargs['content_remaps'],'r'), Loader=Loader )
    return kwargs

if __name__ == '__main__':

    # fields = [ 'nodename', 'device_type', 'is_vm', 'port:hostname', 'port:dhcp', 'port:ip_address', 'subnet', 'port:mac_address', 'user:id', 'user:username', 'user:lastname', 'user:directorate', 'admin:id', 'admin:username', 'admin:lastname', 'custodian:id', 'custodian:username', 'custodian:lastname', 'owner', 'os:name', 'os:version', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location:building', 'location:room', 'location:rack', 'location:ru', 'warranty:start', 'warranty:end', 'updated_at' ]

    parser = argparse.ArgumentParser(description='Collate numerous SLAC Asset databases.', add_help=False)
    parser.add_argument('-v', '--verbose', help='verbose', default=False, action='store_true' )
    
    # configs
    parser.add_argument('--accounts', help='account configurations', default=ETC_PATH+'accounts.yaml' )
    parser.add_argument('--strategies', help='merging strategy configurations', default=ETC_PATH+'strategies.yaml' )
    parser.add_argument('--content_remaps', help='remapping values', default=ETC_PATH+'remaps.yaml' )

    kwargs = vars(parser.parse_known_args()[0])

    # load configs
    kwargs = load_configs( kwargs )

    DB_NAMES = kwargs['accounts']['assets'].keys()

    parser.add_argument("-h", "--help", action="help", help="show this help message and exit")


    # create subparsers
    subparsers = parser.add_subparsers( help='sub-command help' )
    
    # pull
    a = subparsers.add_parser( 'pull', help='pull information from data source' )
    a.set_defaults( action='pull' )
    a.add_argument( 'db', choices=DB_NAMES+['all',], nargs="+" )
    
    # merge
    b = subparsers.add_parser( 'merge', help='merge data from datasources' )
    b.set_defaults( action='merge' )
    b.add_argument( '--ip', nargs="*" )
    b.add_argument( '--mac', nargs="*" )

    for i in ( 'dump', 'upload' ):
        c = subparsers.add_parser( i, help='output final data to %s format' % (i,) )
        c.set_defaults( action=i )
        c.add_argument( 'nodenames', nargs="*" )
        c.add_argument( '--regex', default=False, action='store_true' )
        c.add_argument( '--nogen', default=True, action='store_false' )
        c.add_argument( '--null_char', default='-' )
        c.add_argument( '--force', default=False, action='store_true' )
    
    d = subparsers.add_parser( 'sccm-sw', help='upload sccm sw cis' )
    d.set_defaults( action='sccm-sw' )
    
    # parse final args
    kwargs = vars(parser.parse_args())
    
    level = logging.DEBUG if kwargs['verbose'] else logging.INFO
    logging.basicConfig( level=level, format="%(levelname)-7s %(lineno)4d %(message)s" )

    kwargs = load_configs( kwargs )
    # LOG.debug("KWARGS: %s" % (kwargs,))
        
    # setup cache for searching etc
    mongo = get_mongo( **kwargs['accounts']['main']['mongo'] )

    # pre
    
    ###

    # regex force
    if 'nodenames' in kwargs:
        kwargs['nodenames'] = [ n.upper() for n in kwargs['nodenames'] ]
    if 'regex' in kwargs and kwargs['regex'] and 'nodenames' in kwargs and len(kwargs['nodenames']):
        kwargs['nodenames'] = [ compile(i, IGNORECASE) for i in kwargs['nodenames'] ]


    ###
    # get data from various datasources
    ###
    if kwargs['action'] == 'pull':
        update_db( mongo, kwargs )

    ###
    # merge data from the various datasources
    ###
    elif kwargs['action'] == 'merge':
        subnets = get_subnets( kwargs['accounts']['support']['subnets'] )
        merge( mongo, subnets=subnets, db_names=DB_NAMES, strategies=kwargs['strategies'], ips=kwargs['ip'], macs=kwargs['mac'] )
        
    ###
    # output the merged data
    ###
    elif kwargs['action'] == 'dump':
        subnets = get_subnets( kwargs['accounts']['support']['subnets'])
        tsv( mongo, subnets=subnets, nodenames=kwargs['nodenames'], db_name=OUTPUT_DB, pre_collate=kwargs['nogen'], fields=OUTPUT_FIELDS, null_char=kwargs['null_char'], force=kwargs['force'], content_remaps=kwargs['content_remaps'] )

    ###
    # upload the merged data
    ###
    elif kwargs['action'] == 'upload':
        chunk = 100
        m = 0
        data = { 'records': [] }
        for n,i in enumerate( flat( mongo, subnets=subnets, nodenames=kwargs['nodenames'], db_name=OUTPUT_DB, pre_collate=kwargs['nogen'], fields=OUTPUT_FIELDS, null_char=kwargs['null_char'], force=kwargs['force'], content_remaps=kwargs['content_remaps'], header_remaps={
            'serial': 'u_serial',
            'PC': 'u_pc',
            'nodename': 'u_nodename',
            'port:ip_address': 'u_port_ip_address',
            'port:mac_address': 'u_port_mac_address',
            'user:id': 'u_user_id',
            'custodian:id': 'u_custodian_id',
            'PO': 'u_po',
            'warranty:start': 'u_warranty_start',
            'device_type': 'u_device_type',
            'manufacturer': 'u_manufacturer',
            'model': 'u_model',
            'location:building': 'u_location_building',
            'location:room': 'u_location_room',
        }) ):
        
            m = m + 1
            data['records'].append( i )
            
            if m >= chunk:
                for node, status, message, d in upload( kwargs['accounts']['upload']['servicenow']['url'], kwargs['accounts']['upload']['servicenow']['user'], kwargs['accounts']['upload']['servicenow']['password'], json_dumps( data ) ):
                    if not status:
                        print '%s\t%s\t%s' % ( node, status, message )
                    data = { 'records': [] }
                    m = 0
                
    elif kwargs['action'] == 'sccm-sw':
        
        def dt( s ):
            if isinstance(s,datetime):
                return '%s'%s
            return s
            
        def utf8( s ):
            return s.encode('utf-8')
        
        cur = get_odbc( **kwargs['accounts']['sccm'] )
        fields = ( 'machine name', 'publisher', 'product name', 'version', 'installed' )
        print '\t'.join(fields)
        for i in sccs_sw_data( cur ):
            # print i
            this = []
            for n in fields:
                v = dt(i[n])
                if v == None:
                    v = '-'
                this.append( v )
            t = '\t'.join( v for v in this )
            print utf8(t)


