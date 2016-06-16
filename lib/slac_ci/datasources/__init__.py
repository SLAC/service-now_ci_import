
from pymongo import MongoClient, ASCENDING, DESCENDING

from slac_ci.util import get_file_contents

from importlib import import_module

from re import sub

import logging
LOG = logging.getLogger()



def get_mongo( user=None, passfile=None, host='localhost', db='assets' ):
    client = MongoClient( host )
    return client[db]
    
def get_mongo_collection( mongo, collection='assets' ):
    return mongo[collection]



class Data(object):
    
    """ simple representation of where we can get data from """
    def __init__( self, **kwargs ):
        pass
    def users( self, klass ):
        self.users = klass
    def dhcp( self, klass ):
        self.dhcp = klass

    def query(self, query):
        if getattr(self, 'cursor'):
            self.cursor.execute( query )
            columns = [i[0].lower() for i in self.cursor.description]
            for row in self.cursor:
                try:
                    yield dict(zip(columns, row))
                except:
                    yield None

    def __iter__( self ):
        # {'is_vm': True, 'nodename': 'FERMILNX-V19', 'os': {'version': '6.4', 'name': 'rhel'}, 'updated_at': datetime.datetime(2014, 9, 13, 1, 30, 18), 'cpu': {'cores': 4, 'arch': 'x86_64', 'id': 'QEMU Virtual CPU version (cpu64-rhel6)'}, 'memory': 16079, 'model': 'KVM', 'disk': {'used': 0, 'capacity': 0}, 'ip_address': '134.79.129.60', 'port': {'interface': 'eth0', 'prefix_len': '24', 'ip_address': '134.79.129.60', 'mac_address': '52:54:00:60:57:6f'}, 'manufacturer': 'RedHat'}
        # {'status': 'Good', 'username': 'yemi', 'nodename': 'BULLET0110', 'user': {}, 'device_type': 'server', 'serial': 'FQDB7X1', 'id': 9645, 'manufacturer': 'Dell', 'full_name': 'Adesanya, Adeyemi', 'port': {'hostname': 'SP-BULLET0110', 'ip_address': '172.20.4.17', 'mac_address': 'b8:ca:3a:16:ef:a9'}, 'PC': 'PC90292', 'admin_group': 'Unix;', 'location': {'building': 'B050', 'ru': '21', 'room': '127', 'rack': '1AF25'}, 'model': 'PowerEdge M620', 'os': {'name': 'Redhat Enterprise Linux'}, 'PO': 'PO114655'}
        pass
        
        
        
    
def convert(name):
    s1 = sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()



def pull_db( mongo, datasource, accounts={}, users=None, dhcp=None, ensure_indexes=( 'nodename', 'port.mac_address', 'port.ip_address', 'serial', 'PC' ) ):
    

    cache = get_mongo_collection( mongo, datasource )
    # clear the cache
    cache.drop()

    klass = getattr( import_module('slac_ci.datasources.%s' % (datasource,) ), convert(datasource).title() )
    LOG.info("Pulling data from %s using %s, params: %s" % (datasource,klass, accounts[datasource]))
    obj = klass( **accounts[datasource] )
    if users:
        obj.users( users )
    if dhcp:
        obj.dhcp( dhcp )

    count = 0
    error = 0
    for d in obj:
        LOG.debug("> %s" % (d,))
        try:
            cache.insert( d )
            count = count + 1
        except Exception, e:
            LOG.warn("Error: %s" % (e,))
            error = error + 1
    
    logging.info("inserted %s documents, %s failed" % (count,error))

    # create indexes
    for i in ensure_indexes:
        cache.ensure_index( i, 1  )
        
        

