
from slac_ci.datasources.odbc import ODBC

class Sccm(ODBC):
    def __iter__(self):

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

        for r in self.query("""SELECT
    
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
        """):

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
        
            if self.dhcp and 'ip_address' in r and r['ip_address'] in self.dhcp.dhcp:
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
                elif r['ip_address'] == '131.225.79.31':
                    del r['ip_address']

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
            if self.users and n in self.users.by_username:
                # pr# int "  -> %s " % (users[n], )
                r['user'] = self.users.by_username[n]
                # r['user']['username'] = r['username'].lower()
                del r['full_name']
                del r['username']
            elif self.users and m in self.users.by_name:
                # print 'found by name %s' % (m,)
                r['user'] = self.users.by_name[m]
                r['user']['username'] = r['username'].lower()
                del r['full_name']
                del r['username']
            
            if r['manufacturer'] in ( 'System manufacturer', 'To be filled by O.E.M.', 'To Be Filled By O.E.M.' ):
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

    
