import cx_Oracle

from slac_ci.datasources import Data

from slac_ci.util import get_file_contents

class Oracle(Data):
    
    def __init__( self, user=None, password=None, host='slac-tcp', port=1521, sid='SLAC', tns='SLAC', **kwargs ):
        self.conn = cx_Oracle.connect( user, get_file_contents( password ), tns )
        self.cursor = self.conn.cursor()
