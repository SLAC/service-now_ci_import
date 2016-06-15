import psycopg2
import psycopg2.extras

from slac_ci.datasources import Data

from slac_ci.util import get_file_contents

class Postgres(Data):
    
    def __init__( self, user=None, password=None, host='net-graphite01', db='ptolemy_production', **kwargs ):
        self.conn = psycopg2.connect(
            host = host,
            database = db,
            user = user,
            password = get_file_contents( password ),
        )
        # enable hstore
        psycopg2.extras.register_hstore(self.conn)
        self.cursor = self.conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
