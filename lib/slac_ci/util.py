
from re import match, search, compile, sub, IGNORECASE


def mac_address( m, format='host' ):
    m = m.replace('.','').replace(':','')
    if format == 'host':
        a = [ m[i:i+2] for i in xrange(0,len(m),2) ]
        return ':'.join(a)
    return m
    
    
def parse_number( s, prepend='', digits=5 ):
    if isinstance( s, basestring ):
        m = search( '(?P<number>\d{%s})'%digits, s )
        if m:
            return '%s%s' % (prepend,m.groupdict()['number'])
        elif s.upper() == 'N/A':
            return None
    return None

def nameify( s ):
    try:
        n = str("%s"%s).upper().split(', ')
        return {
            'lastname': n[0],
            'firstname': n[1]
        }
    except:
        return {}

def get_file_contents( passfile ):
    password = None
    with open( passfile ) as f:
        password = f.readlines().pop().strip()
    return password