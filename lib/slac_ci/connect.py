
import cx_Oracle
from pymongo import MongoClient, ASCENDING, DESCENDING
# import pypyodbc
import pyodbc
# from rackwise import RackWiseData

import psycopg2
import psycopg2.extras
import ipaddress
import socket
from datetime import datetime
from copy import deepcopy

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import unicodedata
import cPickle as pickle

from re import match, search, compile, sub, IGNORECASE, findall
import os
from sys import exit

import logging

from util import mac_address, parse_number



def rows_to_dict_list(cursor):
    columns = [i[0].lower() for i in cursor.description]
    for row in cursor:
        try:
            yield dict(zip(columns, row))
        except:
            yield None

def get_file_contents( passfile ):
    password = None
    with open( passfile ) as f:
        password = f.readlines().pop().strip()
    return password

def get_oracle( user=None, password=None, host='slac-tcp', port=1521, sid='SLAC', tns='SLAC' ):
    # dsn = cx_Oracle.makedsn( host, port, sid )
    this = cx_Oracle.connect( user, get_file_contents( password ), tns )
    cursor = this.cursor()
    return cursor

def get_mongo( user=None, passfile=None, host='localhost', db='assets' ):
    client = MongoClient( host )
    return client[db]
    
def get_mongo_collection( mongo, collection='assets' ):
    return mongo[collection]

def get_odbc( user=None, password=None, dsn=None, **kwargs ):
    # DSN required freetds
    s = 'DSN=%s;UID=%s;PWD=%s;' % (dsn,user,get_file_contents( password ) )
    # logging.error("S: %s" % s)
    conn = pyodbc.connect(s)
    return conn.cursor()


def get_postgres( user=None, password=None, host='net-graphite01', db='ptolemy_production', **kwargs ):
    this = psycopg2.connect(
        host = host,
        database = db,
        user = user,
        password = get_file_contents( password ),
    )
    # enable hstore
    psycopg2.extras.register_hstore(this)
    return this.cursor(cursor_factory = psycopg2.extras.RealDictCursor)





def get_users( cursor ):
    """ query sid for user info, but merge with data from res """
    cursor.execute("""
    select 
        BUT_LDT as account, 
        BUT_LID as username, 
        BUT_SID as id, 
        Useracct_admin.but.BUT_UUID as res_id, 
        EMAIL_ADDRESS as email,
        useracct_admin.mail_master.reverse_flag as flag
    from 
        Useracct_admin.but  
LEFT JOIN useracct_admin.mail_master ON useracct_admin.mail_master.but_uuid=Useracct_admin.but.but_uuid 
    where 
        Useracct_admin.but.BUT_LDT='mail'
        AND Useracct_admin.mail_master.reverse_flag IN ( 'X', 'M' )
    """)

        # Useracct_admin.but.BUT_LDT='win' OR Useracct_admin.but.BUT_LDT='unix'
    res = {}
    for d in rows_to_dict_list( cursor ):
        # validate against email
        u,_,domain = d['email'].partition('@')
        # logging.error("1) %s\t%s\t%s email %s" % (d['username'],u,d['id'],d['email']))
        if u.lower() == d['username'].lower() or '.' in d['username']:
            # logging.error("%s \t-> %s \t %s" % (u,d['username'],d['id']))
            # swap user/u if dot in username
            if '.' in d['username']:
                #logging.error("SWAP:%s %s: %s" % (u, d['username'],d) )
                t = d['username'] 
                d['username'] = u
                u = t
            i = d['id']
            if not i in res:
                res[i] = []
            # if not d['username'] in res[i]:
            #     res[i][d['username']] = []
            # res[i][d['username']].append( d['email'] )
            d['email'] = d['email'].lower().replace( '@exchange.','@' ).replace( '@mailbox.', '@')
            res[i].append( d )
        # elif '.' in u.lower():
        #     i = d['id']
            
    cursor.execute("""
    select
        BUT_LDT as account,
        BUT_LID as username,
        BUT_SID as id
    from
        Useracct_admin.but
    where
        Useracct_admin.but.BUT_LDT='win' OR Useracct_admin.but.BUT_LDT='unix'
    """)
    for d in rows_to_dict_list( cursor ):
        i = d['id']
        if i in res:
            res[i].append( d )
 
    cursor.execute("""
    select
      p.key AS id,
      p.lname as lastname,
      p.fname as firstname,
      p.ext as telephone,
      o.description department,
      d.description directorate,
      p.status as status
    from
       persons.person p,
       sid.organizations o,
       sid.organizations d
    where
        P.DEPT_ID = O.ORG_ID(+) 
        AND O.DIRECTORATE_CODE = D.DIRECTORATE_CODE 
        AND D.ORG_LEVEL = 2
    """)
    by_id = {}
    by_name = {}
    by_username = {}
    for d in rows_to_dict_list( cursor ):
        try:
            if d['id'] in res:
                # d['username'] = res[d['id']]
                # hmm.. assume first?
                #logging.error(">> %s" % (res[d['id']],))
                this = res[d['id']].pop(0)
                d['username'] = this['username']
                d['email'] = this['email']
                while '.' in d['username'] or '_' in d['username']:
                    this = res[d['id']].pop(0)
                    d['username'] = this['username']
                #logging.error("  >> %s" % (d,))

            by_id[d['id']] = d
            # print "2) %s" % (d,)
            n = "%s, %s"% ( d['lastname'], d['firstname'] )
            by_name[n.upper()] = d
            if 'username' in d:
                by_username[d['username']] = d
        except Exception, e:
            logging.debug("no username: %s in %s" % (e,d))
            # logging.error("could not determine %s" % (d,))

    return by_id, by_name, by_username

def match_user_by_name( lastname, firstname, users={} ):
    if id in users:
        return users[id]['lastname'], users[id]['firstname']
    return None, None
    
    


###
# Taylor data from afs
###
def taylor_data( path='/afs/slac/g/scs/systems/system.info/', files=('taylor.environ','nicinfo') ):
    network_re = compile( '^(?P<interface>.*)_(?P<key>(mac|ip))$' )
    dirs = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
    for i in dirs:

        d = {}
        for file in files:
            try:
                fp = '%s/%s/%s' % (path,i,file)
                # logging.debug( "FP: %s" % (fp,) )
                # d['updated_at'] = time.ctime(os.path.getmtime(fp))
                t = os.path.getmtime(fp)
                d['updated_at'] = datetime.fromtimestamp( t )
                # logging.debug(" t: %s\t%s"%(t,d['updated_at']))

                with open( fp ) as f:
                    for l in f.readlines():
                        try:
                            k,v = l.strip().split('=')
                            d[k] = v
                        except:
                            pass
            except Exception,e:
                logging.debug(" could parse %s: %s" % (file,e))

        # df
        try:
            with open( '%s/%s/%s' % (path,i,'df') ) as f:
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
                    # logging.error("  %s" % (x,))
                    for n,v in enumerate( ('capacity','used') ):
                        # logging.error("mount %s %s %s -> %s" % (n,v,x, x[n+1]) )
                        d['disk'][v] = d['disk'][v] + int(x[n+1])
                    # logging.error("    %s" % (d['disk'],))
                        
                for v in d['disk'].keys():
                    d['disk'][v] = d['disk'][v] / ( 1024*1024 )
                # logging.error("TOTAL %s %s" % (i,d['disk'],) )
        except:
            pass

        if not 'HOSTNAME' in d:
            logging.debug(" skipping...")
            continue
        # logging.debug("D: %s" % (d,))
        out = {
            'nodename': d['HOSTNAME'].upper(),
            'ip_address': d['IPADDR'] if 'IPADDR' in d else None,
            'serial': d['SERIAL'] if 'SERIAL' in d else None,
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
            # remove
            elif out['model'] in ( '..', ):
                del out['model']
            
            for i in ( 'ASUS', ):
                if 'model' in out and out['model'] and out['model'].startswith(i):
                    a = out['model'].split()
                    out['manufacturer'] = a.pop()
                    out['model'] = ' '.join(a)
            
        # mac addresses etc.
        interfaces = {}
        for k in d:
            # # logging.error("== %s" % (k,))
            if k.startswith('MAC_'):
                # logging.error("%s\t%s\t%s" % (opt,k,d[k]))
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
                # logging.error("V: %s\t%s" % (v,type(v)))
                interfaces[t['interface']][key] = v
                if key == 'mac_address':
                    interfaces[t['interface']][key] = mac_address( v )
                elif key == 'ip_address':
                    # skip certain ips
                    if interfaces[t['interface']][key].startswith('192.168.') or \
                        interfaces[t['interface']][key].startswith('198.168.') or \
                        interfaces[t['interface']][key] in ( '172.23.209.204', '172.23.209.205', '172.23.209.206', '172.23.210.204', '172.23.210.205', '172.23.210.206', '172.23.214.204', '172.23.214.205', '172.23.214.206', '172.23.20.72', '172.23.20.73', '172.23.10.14', '172.23.10.15', '134.79.129.43', '134.79.129.46', '172.23.20.72', '172.23.20.73', '134.79.129.53', '134.79.129.42' ): # lb nodes
                        interfaces[t['interface']][key] = None
                
                logging.debug(" set %s %s %s" % (t['interface'],key,interfaces[t['interface']][key]))

        # for each interface
        for k,v in out.iteritems():
            try:
                if v == '':
                    out[k] = None
            except:
                pass
                
        # setup the ip address of nic if unknown (guess)
        # logging.debug("INT: %s" % (interfaces,) )
        these_interfaces = interfaces.keys()
        if len( these_interfaces ) == 1:
            this_interface = these_interfaces.pop(0)
            if not 'ip_address' in interfaces[this_interface]:
                interfaces[this_interface]['ip_address'] = out['ip_address']
                del out['ip_address']
        else:
            for this_interface in ( 'eth0', 'em1' ):
                # logging.error("THIS: %s\t%s" % (this_interface, out))
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
            # logging.debug("ITEMS: %s %s" % (interface,items,))
            if 'mac_address' in items and items['mac_address'] in ( '00:00:00:00:00:00', ):
                continue
            items['interface'] = interface
            o['port'].append( items )
            
        yield o
        # for y in o['port']:
        #     if '_id' in o:
        #         del o['_id']
        #     o['port'] = y
        #     logging.debug(">> %s" % (o,))
        #     yield o


###
# rackwise sql
###
def rackwise_data( cursor ):

    cursor.execute("""SELECT

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
    """)

    for d in rows_to_dict_list( cursor ):

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
            d['serial'] = d['serial'].upper()

        # cost
        d['capital_cost'] = float(d['capital_cost']) if d['capital_cost'] else None

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

        d['warranty'] = {}
        if d['warranty_start']:
            d['warranty']['start'] = d['warranty_start']
            del d['warranty_start']
        if d['warranty_end']:
            d['warranty']['end'] = d['warranty_end']
            del d['warranty_end']
            
        # print "%s" % d
        yield d


def sccm_data( cursor, users_by_username={}, users_by_name={}, dhcp_table={} ):

    # cursor.execute( """SELECT
    #
    # sys.ResourceID as id,
    #
    # SYS.creation_date0 as first_seen,
    # SYS.last_logon_timestamp0 as last_update,
    # -- CS.timestamp as cs_timestamp,
    # -- bios.timestamp as bios_timestamp,
    # -- ram.timestamp as ram_timestamp,
    #
    # sys.Name0 as nodename,
    # CS.Status0 as status,
    #
    # CS.Manufacturer0 as manufacturer,
    # CS.Model0 as model,
    # BIOS.SerialNumber0 as serial,
    #
    # -- SYS.cputype0 as cpu_id,
    # count(CS.ResourceID) as cpu_sockets,
    # -- cs.SystemType0 as cpu_arch,
    # -- PROCESSOR.MaxClockSpeed0 as cpu_speed,
    # count(PROCESSOR.NumberOfCores0) as cpu_cores,
    # count(PROCESSOR.NumberOfLogicalProcessors0) as cpu_logicalcores,
    #
    #
    # sum(RAM.Capacity0) as memory,
    #
    # sum(DISK.Size0) / 1024 as disk_capacity,
    # sum(DISK.FreeSpace0) / 1024 as disk_free
    #
    # FROM
    #     v_R_System SYS
    #     INNER JOIN v_GS_COMPUTER_SYSTEM CS ON sys.Name0=cs.Name0
    #     INNER JOIN v_GS_PC_BIOS BIOS ON sys.ResourceID=bios.ResourceID
    #     INNER JOIN v_GS_PHYSICAL_MEMORY RAM ON sys.ResourceID=ram.ResourceID
    #     INNER JOIN v_GS_LOGICAL_DISK DISK ON DISK.ResourceID=sys.ResourceID
    #     INNER JOIN v_GS_PROCESSOR PROCESSOR ON PROCESSOR.ResourceID=sys.ResourceID
    #
    # GROUP BY
    #     sys.ResourceID, SYS.creation_date0, SYS.last_logon_timestamp0, sys.Name0,
    #     CS.Status0, CS.Manufacturer0, CS.Model0, BIOS.SerialNumber0
    #
    # ORDER BY sys.Name0
    # """)
    
    # os.timestamp as os_timestamp,
    # CS.UserName0 as unique_user,
    # SYS.user_name0 as username,
    # usr.Full_User_Name0 as full_name,
    # CS.PrimaryOwnerName0 as owner,
    # SYS.is_virtual_machine0 as is_vm,
    # OS.Caption0 as os_name,
    # OS.CSDVersion0 as os_servicepack,
    # OS.version0 as os_version,
    # NIC.DHCPEnabled0 as dhcp,
    # NIC.DNSHostName0 as hostname,
    #
    #
    # nic.timestamp as nic_timestamp,
    # mac.MAC_Addresses0 as mac_address,
    # ip.IP_Addresses0 as ip_address


    # INNER JOIN v_GS_OPERATING_SYSTEM OS ON sys.ResourceID=OS.ResourceID
    # INNER JOIN v_GS_NETWORK_ADAPTER_CONFIGURATION NIC ON sys.ResourceID=NIC.ResourceID
    # INNER JOIN v_RA_System_MACAddresses  mac ON mac.ResourceID=sys.ResourceID
    # INNER JOIN v_RA_System_IPAddresses ip ON ip.ResourceID=sys.ResourceID
    # INNER JOIN v_R_User usr ON sys.user_name0=usr.User_Name0

    cursor.execute( """SELECT
    
    SYS.creation_date0 as first_seen,
    SYS.last_logon_timestamp0 as last_update,
    CS.timestamp as cs_timestamp,
    bios.timestamp as bios_timestamp,
    os.timestamp as os_timestamp,
    ram.timestamp as ram_timestamp,
    nic.timestamp as nic_timestamp,
        
    sys.Name0 as nodename,
    
    CS.UserName0 as unique_user,
    SYS.user_name0 as username,
    usr.Full_User_Name0 as full_name,
    CS.PrimaryOwnerName0 as owner,
    
    CS.Status0 as status,

    SYS.is_virtual_machine0 as is_vm,
    OS.Caption0 as os_name,
    OS.CSDVersion0 as os_servicepack,
    OS.version0 as os_version,
    
    CS.Manufacturer0 as manufacturer,
    CS.Model0 as model,
    BIOS.SerialNumber0 as serial,

    SYS.cputype0 as cpu,
    CS.NumberOfProcessors0 as cpu_count,
    RAM.Capacity0 as memory,
    cs.SystemType0 as architecture,

    NIC.DHCPEnabled0 as dhcp,
    NIC.DNSHostName0 as hostname,
    
    mac.MAC_Addresses0 as mac_address,
    ip.IP_Addresses0 as ip_address

    FROM 
        v_R_System SYS,
        v_GS_COMPUTER_SYSTEM CS,
        v_GS_PC_BIOS BIOS,
        v_GS_OPERATING_SYSTEM OS, 
        v_GS_PHYSICAL_MEMORY RAM,
        v_GS_NETWORK_ADAPTER_CONFIGURATION NIC,
        v_RA_System_MACAddresses  mac,
        v_RA_System_IPAddresses ip,
        v_R_User usr
    WHERE 
        sys.Name0=cs.Name0
        AND sys.ResourceID=bios.ResourceID
        AND sys.ResourceID=OS.ResourceID
        AND sys.ResourceID=ram.ResourceID
        AND sys.ResourceID=NIC.ResourceID
        AND mac.ResourceID=sys.ResourceID
        AND ip.ResourceID=sys.ResourceID
        AND sys.user_name0=usr.User_Name0
    """)

    for r in rows_to_dict_list(cursor):

        r['os'] = {}
        for x in ( 'os_name', 'os_version', 'os_servicepack'):
            y = x.replace("os_",'')
            if x in r:
                r['os'][y] = r[x]
                del r[x]

        if 'serial' in r and r['serial'] in ( 'NONE', '', 'To be filled by O.E.M.', '1234567890', 'System Serial Number' ):
            del r['serial']
        
        r['port'] = {}
        
        # ignore bad data
        
        if 'ip_address' in r and r['ip_address'] in dhcp_table:
            del r['ip_address']
            r['port']['dhcp'] = True

        # ignore ipv6 for now
        if 'ip_address' in r:
            if ':' in r['ip_address']:
                del r['ip_address']
                del r['dhcp']
            elif r['ip_address'] == '0.0.0.0' or r['ip_address'].startswith( '169.254.' ) or r['ip_address'].startswith( '192.168.' ) or r['ip_address'].startswith('10.') or r['ip_address'].startswith('128.'):
                del r['ip_address']
                del r['dhcp']

        for x in ( 'ip_address', 'mac_address', 'dhcp' ):
            if x in r:
                if isinstance( r[x], basestring ):
                    r['port'][x] = r[x].lower()
                else:
                    if r[x] == 0:
                        r['port'][x] = False
                    elif r[x] == 1:
                        r['port'][x] = True
                    else:
                        r['port'][x] = r[x]
                del r[x]

        for x in ( 'last_update', 'cs_timestamp', 'bios_timestamp', 'os_timestamp', 'ram_timestamp', 'nic_timestamp', ):
            if x in r:
                if not 'updated_at' in r:
                    r['updated_at'] = r[x]
                if r[x] > r['updated_at']:
                    r['updated_at'] = r[x]
                del r[x]

        # r['user'] = nameify( r['full_name'] )
        
        # print "NAME %s -> " % (n,)
        n = None
        m = None
        if 'username' in r:
            n = r['username'].lower()
        if 'full_name' in r:
            m = r['full_name'].upper().replace('-A','')
        if n in users_by_username:
            # pr# int "  -> %s " % (users[n], )
            r['user'] = users_by_username[n]
            # r['user']['username'] = r['username'].lower()
            del r['full_name']
            del r['username']
        elif m in users_by_name:
            # print 'found by name %s' % (m,)
            r['user'] = users_by_name[m]
            r['user']['username'] = r['username'].lower()
            del r['full_name']
            del r['username']
            
        if r['manufacturer'] in ( 'System manufacturer', 'To be filled by O.E.M.', ):
            del r['manufacturer']
            
        if r['model'] in ( 'System Product Name', 'To be filled by O.E.M.', ):
            del r['model']

        if 'model' in r  and r['model']:
            if r['model'] == 'VMware Virtual Platform':
                r['manufacturer'] = 'VMware'
                r['is_vm'] = True
            elif r['model'] == 'Virtual Machine':
                r['manufacturer'] = 'Microsoft'
                r['is_vm'] = True
        
        r['cpu'] = {}
        for i in ( 'cpu_arch', 'cpu_sockets', 'cpu_id', 'cpu_cores', 'cpu_logicalcores' ):
            if i in r:
                x = i.replace('cpu_','')
                r['cpu'][x] = r[i]
                del r[i]
        # logging.error("%s" % (r['cpu'],))
        if 'cores' in r['cpu'] and 'sockets' in r['cpu'] and r['cpu']['cores'] > 0:
            r['cpu']['sockets'] = r['cpu']['sockets'] / r['cpu']['cores']
        
        try: 
            used = None
            used = r['disk_capacity'] - r['disk_free']
            r['disk'] = { 'capacity': r['disk_capacity'] if 'disk_capacity' in r else None, 'used': used }
            del r['disk_capacity']
            del r['disk_free']
        except:
            pass
        
        # print "%s" % r
        yield r



def bis_data( cursor, users={} ):
    # A.DESCR as device_type,
    cursor.execute("""
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
    """)

    # decode(b.custodian, ' ', b.authorization_name, b.custodian) custodian_name,
    # A.PROFILE_ID,
    for d in rows_to_dict_list(cursor):
        
        logging.debug("b: %s" % (d,))
        
        if d['manufacturer'] in ( 'Seagate', ):
            continue
        
        for i in ( 'pc', 'po' ):
            if i in d and d[i]:
                j = i.upper()
                n = 5 if i == 'pc' else 6
                d[j] = parse_number( d[i], prepend=j, digits=n )
                del d[i]
                # logging.error("D: %s %s\t%s" % (i,j,d))
        
        # map custodian to user
        # strip leading zeros of cust id
        try:
            d['custodian_id'] = int(d['custodian_id'])
            # logging.error("CUST ID: %s\t%s" % (d['custodian_id'], d['custodian_id'] in users))
            if d['custodian_id'] in users:
                d['custodian'] = users[d['custodian_id']]
                del d['custodian_id']
                # logging.error("found %s" % (d['custodian']))
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

        # logging.error("%s" % d['location'] )
        try:
            b,r = d['location'].strip().replace(' ','').split('_')
            d['location'] = { 'building': b, 'room': r }
        except:
            d['location'] = { 'building': d['location'].strip() }

        yield d



def ptolemy_arp_data( cursor, dhcp_table={} ):
    cursor.execute( """SELECT
      context->'mac_address' AS mac_address,
      context->'ip_address' AS ip_address,
      ARRAY_AGG( updated_at ) AS updated_at
    FROM
      arps__arps
    GROUP BY context->'mac_address', context->'ip_address';
    """ )

    # WHERE
    # GROUP BY mac_address, ip_address
    #   updated_at > (now() - '1 days'::interval)

    def give( d, dhcp_table={} ):
        # logging.debug( "%s" % d['updated_at'] )
        # 
        d['mac_address'] = mac_address( d['mac_address'] )
        d['updated_at'] = max(d['updated_at'])

        if d['mac_address'] in ( '00:00:00:00:00:00', ):
            del d['mac_address']
        if d['ip_address'] in ( '0.0.0.0' ):
            del d['ip_address']
           
        if 'ip_address' in d:
            if d['ip_address'] in dhcp_table or d['ip_address'].startswith( '198.129' ):
                d['dhcp_ip_address'] = d['ip_address']
                del d['ip_address']
            elif d['ip_address'].startswith( '192.168' ):
                del d['ip_address']
            else:
                d['ip_address'] = d['ip_address']

        return d

    # dedup
    table = {}
    for this in cursor:
        if this['mac_address'] and this['ip_address']:
            d = give( this, dhcp_table=dhcp_table )
            # assume those without ip are dhcp
            if not 'ip_address' in d:
                d['dhcp'] = True
            # s = stringify( d, exclude=['updated_at',] )
            if 'mac_address' in d:
                s = d['mac_address']
                if not s in table:
                    table[s] = d
                if d['updated_at'] > table[s]['updated_at']:
                    # table[s]['updated_at'] = d['updated_at']
                    table[s] = d

    for s,d in table.iteritems():
        this = {
            'updated_at': d['updated_at'],
            'port': {}
        }
        for i in ( 'ip_address', 'mac_address', 'dhcp', 'dhcp_ip_address' ):
            if i in d:
                this['port'][i] = d[i]
        # print("S: %s" % (this,))
        yield this

def ptolemy_entity_data( cursor, dhcp_table={} ):
    cursor.execute( """SELECT
      context->'device' as nodename, 
      data->'model' as model,
      data->'serial' as serial,
      updated_at
    FROM
      entity__meta where context->'type'='chassis'
    """ )
    
    # dunno how to deal with one logical node with multiple physical assets... so just ignore for now
    entities = {}
    for d in cursor:
        d['nodename'] = d['nodename'].split('.').pop(0).upper()
        if 'serial' in d and d['serial']:
            d['serial'] = d['serial'].upper()
        if 'model' in d and d['model'] in ( ' ', ):
            continue
        if not d['nodename'] in entities:
            entities[d['nodename']] = []
        entities[d['nodename']].append( d )
        
    for k,v in entities.iteritems():
        if len(v) == 1:
            yield v
            
def netdb_yaml_data( yaml=None, **kwargs ):
    docs = {}
    with open( yaml, 'r' ) as f:
        docs = load(f, Loader=Loader)

    
    for name, doc in docs.iteritems():
        # logging.error("DOC: %s" % (doc,))
        ports = []
        for i in doc['interfaces']:
            p = {
                # 'interface': ''
                'mac_address': i['ethernet'],
            }
            if 'ipnum' in i:
                for ip in i['ipnum']:
                    p['ip_address'] = ip
                    ports.append( p )
    
        bldg = doc['bldg'] if not doc['bldg'] == 'Not found' else None
        room = doc['room'] if not doc['room'] == 'UNKNOWN' else None

        try:
            fullname = None
            user = None
            fullname, user = doc['users'][0].split( ' (' )
            user = user[:-1] # strip )
        except:
            pass
     
        this = {
            'id': doc['id'],
            'nodename': doc['node'],
            'location': { 'building': bldg, 'room': room },
            "status": doc['state'],
            'model': doc['model'],
            'os': {
                'name': doc['os'][0],
            },
            'full_name': fullname,
            'username': user,
            'port': ports,
        }

        # logging.error(" >> %s" % (this,))

        yield this
    
    

def pull_db( mongo, this, accounts={}, ensure_indexes=( 'nodename', 'port.mac_address', 'port.ip_address', 'serial', 'PC' ) ):
    
    logging.info("Pulling data from %s" % (this,))

    cache = get_mongo_collection( mongo, this )

    # clear the cache
    cache.remove()

    out = None

    # need cando for lookups
    cando = get_oracle( **accounts['oracle'] )

    # lookup for if a host is dhcp or not
    dhcp_table = {}
    #for i,n,d in get_cando_ip_dns( cando ):
    #    if d:
    #        dhcp_table[i] = True

    if this == 'taylor':

        # get taylor info
        out = cache.insert( taylor_data( **accounts['taylor']) )

    elif this == 'sccm':
        
        by_id, by_name, by_username = get_users( cando )
        g = get_odbc( **accounts[this] )
        out = []
        for t in sccm_data( g, users_by_username=by_username, users_by_name=by_name, dhcp_table=dhcp_table ):
            try:                   
                out.append( cache.insert( t ) )
            except Exception, e:
                logging.error("Error: %s"%e)
        
    elif this == 'rackwise':
        
        cur = get_odbc( **accounts[this] )
        out = []
        for t in rackwise_data( cur ):
            try:
                out.append( cache.insert(t) )
            except Exception, e:
                logging.error("Error: %s" % (e,))

    elif this == 'bis':
        
        by_id, by_name, by_username = get_users( cando )
        out = []
        cando = get_oracle( **accounts[this] )
        for b in bis_data( cando, users=by_id ):
            try:
                out.append( cache.insert(b) )
            except:
                pass
        
    elif this == 'netdb':
        
        
        out = [ cache.insert(t) for t in netdb_yaml_data( **accounts[this] ) if t ]
            
    
    elif this == 'ptolemy_arp':
    
        # get arps
        p = get_postgres( *accounts[this] )
        out = []
        for t in ptolemy_arp_data( p, dhcp_table=dhcp_table ):
            try:
                out.append( cache.insert( t ) )
            except Exception, e:
                logging.error("Error: %s" % e )

    elif this == 'ptolemy_device':

        # get serial for switches
        p = get_postgres( *accounts[this] )
        out = [ cache.insert(t) for t in ptolemy_entity_data( p ) if t ]
    
    
    logging.info("inserted %s documents" % (len(out),))

    # create indexes
    for i in ensure_indexes:
        cache.ensure_index( i, 1  )



