
from slac_ci.datasources.postgres import Postgres

class Ptolemy(Postgres):

    def __iter__( self ):
        self.cursor.execute( """SELECT
          context->'device' as nodename, 
          data->'model' as model,
          data->'serial' as serial,
          updated_at
        FROM
          entity__meta where context->'type'='chassis'
        """ )
    
        # dunno how to deal with one logical node with multiple physical assets... so just ignore for now
        entities = {}
        for d in self.cursor:
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
    