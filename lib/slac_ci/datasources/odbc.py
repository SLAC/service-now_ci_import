from slac_ci.datasources import Data
from slac_ci.util import get_file_contents

import pyodbc

class ODBC(Data):
    """ odbc import """
    def __init__( self, user=None, password=None, dsn=None, **kwargs ):
        self.user = user
        self.password = password
        self.dsn = dsn
        # DSN required freetds
        s = 'DSN=%s;UID=%s;PWD=%s;CHARSET=UTF8' % (self.dsn,self.user,get_file_contents( self.password ) )
        self.conn = pyodbc.connect(s)
        self.cursor = self.conn.cursor()