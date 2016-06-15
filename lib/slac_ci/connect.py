
from pymongo import MongoClient, ASCENDING, DESCENDING
# import pypyodbc

# from rackwise import RackWiseData


import socket
from datetime import datetime
from copy import deepcopy



import unicodedata
import cPickle as pickle

from re import match, search, compile, sub, IGNORECASE, findall
import os
from sys import exit

import logging

from util import mac_address, parse_number












    
###
# rackwise sql
###
def sccs_sw_data( cursor ):

    cursor.execute("""SELECT 
            Name as [machine name], 
            Publisher0 as [publisher],
            ProductName0 as [product name], 
            ProductVersion0 as [version], 
            InstallDate0 as [installed]
        FROM  vWORKSTATIONSTATUS 
        INNER JOIN v_GS_INSTALLED_SOFTWARE on v_GS_INSTALLED_SOFTWARE.ResourceID = vWORKSTATIONSTATUS.ResourceID
    """)
    # vWORKSTATIONSTATUS
    #         INNER JOIN vWORKSTATIONSTATUS on vWORKSTATIONSTATUS.ResourceID = vSMS_R_System.ResourceId

    fields = ( 'machine name', 'product name', 'version', 'installed' ) #'publisher', 
    for i in rows_to_dict_list(cursor):
        yield i

    
    




