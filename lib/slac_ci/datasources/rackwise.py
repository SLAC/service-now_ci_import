from slac_ci.datasources.odbc import ODBC

from re import sub
from slac_ci.util import parse_number

import logging
LOG = logging.getLogger()


class Rackwise(ODBC):

    ###
    # rackwise sql
    ###    
    def __iter__( self ):

        for d in self.query( """SELECT

        devicedisplay as nodename,
        eqtype as device_type,

        manufacturer AS manufacturer,
        PublicViewAssetsAndSerialNumbersReport.modelnumber as model,

        PublicViewAssetsAndSerialNumbersReport.serialnumber AS serial,
        PublicViewAssetsAndSerialNumbersReport.assetnumber AS pc,
        leasestartdate,
        leasedetail AS [PO],
        leaseenddate,

        servicedate AS service_date,

        locationname AS room,
        regionname AS rack,
        PublicViewAssetsAndSerialNumbersReport.rackposition AS ru,

        warrantystartdate AS warranty_start,
        warrantyenddate AS warranty_end,
        warrantydetail,
        capitalcost AS capital_cost,

        departmentdescription,
        departmentname AS owner,
        customerdescription,
        customername

        FROM
        PublicViewAssetsAndSerialNumbersReport
            LEFT JOIN Device_Generic ON PublicViewAssetsAndSerialNumbersReport.deviceid=Device_Generic.deviceGeneric_id
            LEFT JOIN DeviceGeneric2Department ON Device_Generic.deviceGeneric_id=DeviceGeneric2Department.DeviceGenericId
              LEFT JOIN Department ON DeviceGeneric2Department.DepartmentId=Department.DepartmentId
            LEFT JOIN DeviceGeneric2Customer ON Device_Generic.deviceGeneric_id=DeviceGeneric2Customer.DeviceGenericId
              LEFT JOIN Customer ON DeviceGeneric2Customer.CustomerId=Customer.CustomerId
        
    
        WHERE SolutionName='SLAC'
        """):

            d['nodename'] = sub( '\n', '; ', d['nodename'].upper() )
            
            # pc and po
            for i in ( 'pc', 'po' ):
                n = 5 if i == 'pc' else 6
                d[i.upper()] = parse_number( d[i], prepend=i.upper(), digits=n )
                del d[i]
                if d[i.upper()] == None:
                    del d[i.upper()]
            
            # deal with multiple serial numbers? keep as array?
            if d['serial'] in ( 'UNKNOWN', ):
                d['serial'] = None
            elif 'serial' in d and d['serial']:
                d['serial'] = d['serial'].upper().replace("\"", '')

            # cost
            d['capital_cost'] = '%.2f' % round(d['capital_cost'],2) if d['capital_cost'] else None

            # device_type
            if d['device_type'] in ( 'Other - Networking', ):
                del d['device_type']

            d['location'] = {}
            if d['room'] == '2nd floor':
                d['location']['room'] = '210'
                d['location']['building'] = '050'
            elif d['room'] == '1st Floor':
                d['location']['room'] = '116'
                d['location']['building'] = '050'
            elif d['room'] == 'Bldg950':
                d['location']['building'] = '950'
                d['location']['room'] = '203'
            elif d['room'] == 'Kipac Bldg51':
                d['location']['building'] = '051'
            del d['room']
            if d['rack']:
                d['location']['rack'] = d['rack']
                del d['rack']
            if d['ru']:
                d['location']['ru'] = '%s'%d['ru']
                del d['ru']

            if 'building' in d['location']:
                d['location']['building'] = 'B'+d['location']['building']

            d['warranty'] = {
                'start': d['warranty_start'],
                'end': d['warranty_end'],
                'detail': d['warrantydetail']
            }
            d['service_date'] = d['warranty_start']
            del d['warranty_start']
            del d['warranty_end']
            del d['warrantydetail']
            
            d['customer'] = {
                'name': d['customername'],
                'description': d['customerdescription']
            }
            del d['customername']
            del d['customerdescription']
            
            d['lease'] = {
                'start': d['leasestartdate'],
                'end': d['leaseenddate'],
            }
            del d['leasestartdate']
            del d['leaseenddate']
            
            # print "%s" % d
            yield d
    
