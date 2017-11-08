#!/usr/bin/python

# script to take csv and spit out a treemap json file for d3

import sys, os
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')


pathname = os.path.abspath( os.path.dirname(sys.argv[0]) )
LIB_PATH =  pathname + '/../lib/'
sys.path.append( LIB_PATH )

import fileinput
from slac_ci.datasources import get_mongo_collection
from slac_ci.datasources import get_mongo
import logging
import json
import pprint

logging.basicConfig( level=logging.WARN )
pp = pprint.PrettyPrinter(indent=4)

data = {}

mongo = get_mongo( host='localhost', db='assets' )
db = mongo[ 'rackwise' ]

for d in db.find():

    logging.warn( pp.pprint(d) )
            
    dept = d['owner']
    if dept == 'OCIO':
        dept = 'Office of the CIO'
    elif dept == None:
        dept = 'Unknown'

    if not dept in data:
        data[dept] = {
            'count': 0,
            'ru': 0,
            'capital_cost': 0.0,
            'service_date_oldest': None,
            'service_date_newest': None,
        }

    data[dept]['count'] += 1
    data[dept]['ru'] += d['ru'] if 'ru' in d else 0
    data[dept]['capital_cost'] += float(d['capital_cost']) if 'capital_cost' in d and d['capital_cost'] else 0
    
    if d['service_date']:
        if data[dept]['service_date_oldest'] == None or d['service_date'] < data[dept]['service_date_oldest']:
            data[dept]['service_date_oldest'] = d['service_date']
        if data[dept]['service_date_newest'] == None or d['service_date'] > data[dept]['service_date_newest']:
            data[dept]['service_date_newest'] = d['service_date']


#
#
out = {
    'name': 'b050',
    'children': [],
}

for k,d in data.iteritems():
    # logging.debug( "%s\t%s" % (k,d,) )
    this = {
        'name': k,
        'number': d['count'],
        'ru': d['ru'],
        'cost': d['capital_cost'],
        'service_date_newest': d['service_date_newest'],
        'service_date_oldest': d['service_date_oldest'],
    }
    out['children'].append( this )


pp.pprint(out)

# print json.dumps( out )