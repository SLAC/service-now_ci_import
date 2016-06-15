from slac_ci.datasources.oracle import Oracle

from slac_ci.util import parse_number

import logging
LOG = logging.getLogger(__name__)

class Bis( Oracle ):
    
    def __iter__( self):
        # A.DESCR as device_type,

        # decode(b.custodian, ' ', b.authorization_name, b.custodian) custodian_name,
        # A.PROFILE_ID,
        for d in self.query("""
        SELECT DISTINCT
            B.EMPLID as custodian_id,
            A.TAG_NUMBER as PC,
            C.LOCATION as location,
            A.ASSET_STATUS as status,
            A.TAGGABLE_SW,
            A.ACQUISITION_CD,
            A.MODEL as model,
            A.PROFILE_ID as profile_id,
            A.SERIAL_ID as serial,
            A.MANUFACTURER as manufacturer,
            A.ACQUISITION_DT as purchased,
            C.DOCUMENT_ID as PO,
            C.EFFDT  as first_seen,
            cost.total_cost as cost,
            b.deptid as department_id
        FROM PS_ASSET A, PS_ASSET_CUSTODIAN B, PS_ASSET_LOCATION C, PS_LOCATION_TBL D, (SELECT ASSET_ID, TOTAL_COST FROM PS_SL_ASSETCOST_VW WHERE BUSINESS_UNIT = 'SLAC') cost
            WHERE B.EFFDT = (SELECT MAX(EFFDT) FROM PS_ASSET_CUSTODIAN WHERE ASSET_ID = b.ASSET_ID AND EFFDT <= SYSDATE AND BUSINESS_UNIT = b.business_unit) and A.asset_id = cost.asset_id(+) and A.asset_id = b.asset_id and c.business_unit = a.business_unit and a.business_unit = b.business_unit and c.business_unit = 'SLAC' AND c.asset_id = b.asset_id and A.ASSET_STATUS IN ('E','I') AND C.EFFDT = (SELECT MAX(EFFDT) FROM PS_ASSET_LOCATION WHERE ASSET_ID = c.ASSET_ID AND EFFDT <= SYSDATE AND BUSINESS_UNIT = C.BUSINESS_UNIT) and d.location(+) = c.location and d.setid(+) = c.business_unit
        """):
        
            LOG.debug("b: %s" % (d,))
        
            if d['manufacturer'] in ( 'Seagate', ):
                continue
        
            for i in ( 'pc', 'po' ):
                if i in d and d[i]:
                    j = i.upper()
                    n = 5 if i == 'pc' else 6
                    d[j] = parse_number( d[i], prepend=j, digits=n )
                    del d[i]
                    # LOG.error("D: %s %s\t%s" % (i,j,d))
        
            # map custodian to user
            # strip leading zeros of cust id
            try:
                d['custodian_id'] = int(d['custodian_id'])
                # LOG.error("CUST ID: %s\t%s" % (d['custodian_id'], d['custodian_id'] in users))
                if d['custodian_id'] in users:
                    d['custodian'] = users[d['custodian_id']]
                    del d['custodian_id']
                    # LOG.error("found %s" % (d['custodian']))
            except:
                pass
        
            if d['serial'] in ( 'NONE', ):
                d['serial'] = None
            
            if 'profile_id' in d:
            # 72047500 Microcomputer
                if d['profile_id'] == '72047500':
                    d['device_type'] = 'computer'
            # 72047495 Microcomputer Laptop
                elif d['profile_id'] == '72047495':
                    d['device_type'] = 'laptop'
            # 74027497 Microcomputer Tablet
                elif d['profile_id'] == '74027497':
                    d['device_type'] = 'tablet'
            # 72047520 Microcomputer Printer
                elif d['profile_id'] == '72047520':
                    d['device_type'] = 'printer'
            # 72042508 Display Unit
            # 72047528 Microcomputer Hard Drive
            # 77037300 Data Storage Disc
            # 77037301 Data Storage Disc Optical
            # 77037302 Data Storage Disc Array
                elif d['profile_id'] == '77037302':
                    d['device_type'] = 'storage'
            # 77037304 Data Storage Disc Array W/Ctrl
                elif d['profile_id'] == '77037302':
                    d['device_type'] = 'storage'
            # 77037305 Data Storage Disc W/Control
                elif d['profile_id'] == '77037302':
                    d['device_type'] = 'storage'
            # 77037310 Data Storage Disc Control
                elif d['profile_id'] == '77037302':
                    d['device_type'] = 'storage'
            # 77037500 Server... ( after server we add , storage or blade etc..)
                elif d['profile_id'] == '77037500':
                    d['device_type'] = 'server'
            # 77037515 Server Enclosure
                elif d['profile_id'] == '77037515':
                    d['device_type'] = 'server'
            # 77037460 Router Controller
                elif d['profile_id'] == '77037515':
                    d['device_type'] = 'router'
            # 77032450 Switching Unit
                elif d['profile_id'] == '77032450':
                    d['device_type'] = 'switch'
                del d['profile_id']
        
        
            # sutpid overload of model with speed, remove
            for punc in ( '/', ',' ):
                if punc in d['model']:
                    d['model'] = d['model'].split(punc).pop(0)

            # stupid serials
            if d['serial'] == 'PTSAK0X0348150149727':
                d['serial'] = 'PTSAK0X034815014972700'

            # LOG.error("%s" % d['location'] )
            try:
                b,r = d['location'].strip().replace(' ','').split('_')
                d['location'] = { 'building': 'B'+b, 'room': r }
            except:
                if d['location'] in (  'OSU', 'OSU' ):
                    d['location'] = 'Off campus'
                else:
                    d['location'] = 'B'+d['location'].strip()
                d['location'] = { 'building': d['location'] }

            yield d
    